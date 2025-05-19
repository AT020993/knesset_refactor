from __future__ import annotations

# Standard Library Imports
import io
import logging
import re  # For safe filename generation and SQL injection
import sys
from pathlib import Path
from textwrap import dedent

# Third-Party Imports
import pandas as pd
import streamlit as st

# Add the 'src' directory to sys.path
_CURRENT_FILE_DIR = Path(__file__).resolve().parent
_SRC_DIR = _CURRENT_FILE_DIR.parent
_PROJECT_ROOT = _SRC_DIR.parent

if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Local Application Imports
from utils.logger_setup import setup_logging  # type: ignore
from backend.fetch_table import TABLES  # type: ignore # Import TABLES list
from backend import config as backend_config # Import config
import ui.plot_generators as pg
import ui.sidebar_components as sc
import ui.ui_utils as ui_utils
import ui.chart_builder_ui as cb_ui

# Initialize logger for the UI module
ui_logger = setup_logging("knesset.ui.data_refresh", console_output=True)
ui_logger.info("--- data_refresh.py script started ---")


# --- Constants and Global-like Configurations ---
# DB_PATH = Path("data/warehouse.duckdb") # Removed
# PARQUET_DIR = Path("data/parquet") # Removed
MAX_ROWS_FOR_CHART_BUILDER = 50000
MAX_UNIQUE_VALUES_FOR_FACET = 50

EXPORTS = {
    "Queries + Full Details": {
        "sql": """
            SELECT 
                Q.QueryID, 
                Q.Number, 
                Q.KnessetNum, 
                Q.Name AS QueryName, 
                Q.TypeID AS QueryTypeID, 
                Q.TypeDesc AS QueryTypeDesc,
                S.Desc AS QueryStatusDesc, 
                P.FirstName AS MKFirstName, 
                P.LastName AS MKLastName,
                P.GenderDesc AS MKGender, -- Added MK Gender
                P2P.FactionName AS MKFactionName, 
                ufs.CoalitionStatus AS MKFactionCoalitionStatus, 
                M.Name AS MinistryName,
                strftime(CAST(Q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDateFormatted
            FROM KNS_Query Q
            LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
            LEFT JOIN KNS_PersonToPosition P2P ON Q.PersonID = P2P.PersonID 
                AND Q.KnessetNum = P2P.KnessetNum 
                AND CAST(Q.SubmitDate AS TIMESTAMP) BETWEEN CAST(P2P.StartDate AS TIMESTAMP) AND CAST(COALESCE(P2P.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN KNS_GovMinistry M ON Q.GovMinistryID = M.GovMinistryID
            LEFT JOIN KNS_Status S ON Q.StatusID = S.StatusID
            LEFT JOIN UserFactionCoalitionStatus ufs ON P2P.FactionID = ufs.FactionID AND P2P.KnessetNum = ufs.KnessetNum
            ORDER BY Q.KnessetNum DESC, Q.QueryID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "Q.KnessetNum",
        "faction_filter_column": "P2P.FactionID",
    },
    "Agenda Items + Full Details": {
        "sql": """
            SELECT 
                A.AgendaID, 
                A.Number AS AgendaNumber, 
                A.KnessetNum, 
                A.Name AS AgendaName, 
                A.ClassificationDesc AS AgendaClassification,
                S.Desc AS AgendaStatus, 
                INIT_P.FirstName AS InitiatorFirstName, 
                INIT_P.LastName AS InitiatorLastName,
                INIT_P.GenderDesc AS InitiatorGender, -- Added Initiator Gender
                INIT_P2P.FactionName AS InitiatorFactionName, 
                INIT_UFS.CoalitionStatus AS InitiatorFactionCoalitionStatus,
                HC.Name AS HandlingCommitteeName, 
                strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted
            FROM KNS_Agenda A
            LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
            LEFT JOIN KNS_Person INIT_P ON A.InitiatorPersonID = INIT_P.PersonID
            LEFT JOIN KNS_PersonToPosition INIT_P2P ON A.InitiatorPersonID = INIT_P2P.PersonID 
                AND A.KnessetNum = INIT_P2P.KnessetNum 
                AND CAST(COALESCE(A.PresidentDecisionDate, A.LastUpdatedDate) AS TIMESTAMP) BETWEEN CAST(INIT_P2P.StartDate AS TIMESTAMP) AND CAST(COALESCE(INIT_P2P.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN UserFactionCoalitionStatus INIT_UFS ON INIT_P2P.FactionID = INIT_UFS.FactionID AND INIT_P2P.KnessetNum = INIT_UFS.KnessetNum
            LEFT JOIN KNS_Committee HC ON A.CommitteeID = HC.CommitteeID
            ORDER BY A.KnessetNum DESC, A.AgendaID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column": "INIT_P2P.FactionID",
    },
}

AVAILABLE_PLOTS_BY_TOPIC = {
    "Queries": {
        "Number of Queries per Year": pg.plot_queries_by_year,
        "Distribution of Query Types": pg.plot_query_types_distribution,
        "Queries by Faction (Coalition/Opposition Status)": pg.plot_queries_by_faction_status, 
        "Queries per Faction (Single Knesset)": pg.plot_queries_per_faction_in_knesset, 
        "Queries by Coalition & Answer Status (Single Knesset)": pg.plot_queries_by_coalition_and_answer_status,
        "Query Performance by Ministry (Single Knesset)": pg.plot_queries_by_ministry_and_status,
    },
    "Agendas": {
        "Number of Agenda Items per Year": pg.plot_agendas_by_year,
        "Distribution of Agenda Classifications": pg.plot_agenda_classifications_pie,
        "Agenda Item Status Distribution": pg.plot_agenda_status_distribution, 
        "Agendas per Faction (Single Knesset)": pg.plot_agendas_per_faction_in_knesset,
        "Agendas by Coalition & Status (Single Knesset)": pg.plot_agendas_by_coalition_and_status,
    }
}

st.set_page_config(page_title="Knesset OData – Refresh & Export", layout="wide")

# --- Session State Initialization ---
ui_logger.info("--- Initializing session state ---")
# For Predefined Queries
if "selected_query_name" not in st.session_state: st.session_state.selected_query_name = None
if "executed_query_name" not in st.session_state: st.session_state.executed_query_name = None
if "executed_sql_string" not in st.session_state: st.session_state.executed_sql_string = ""
if "query_results_df" not in st.session_state: st.session_state.query_results_df = pd.DataFrame()
if "show_query_results" not in st.session_state: 
    st.session_state.show_query_results = False
    ui_logger.debug("Initialized st.session_state.show_query_results to False")
if "applied_knesset_filter_to_query" not in st.session_state: st.session_state.applied_knesset_filter_to_query = []
if "last_executed_sql" not in st.session_state: st.session_state.last_executed_sql = ""


# For Interactive Table Explorer
if "selected_table_for_explorer" not in st.session_state: st.session_state.selected_table_for_explorer = None
if "executed_table_explorer_name" not in st.session_state: st.session_state.executed_table_explorer_name = None
if "table_explorer_df" not in st.session_state: st.session_state.table_explorer_df = pd.DataFrame()
if "show_table_explorer_results" not in st.session_state: 
    st.session_state.show_table_explorer_results = False
    ui_logger.debug("Initialized st.session_state.show_table_explorer_results to False")

# For Sidebar Filters 
if "ms_knesset_filter" not in st.session_state: st.session_state.ms_knesset_filter = []
if "ms_faction_filter" not in st.session_state: st.session_state.ms_faction_filter = []

# For Predefined Data Visualizations
if "selected_plot_topic" not in st.session_state: st.session_state.selected_plot_topic = "" 
if "selected_plot_name_from_topic" not in st.session_state: st.session_state.selected_plot_name_from_topic = "" 
if "generated_plot_figure" not in st.session_state: st.session_state.generated_plot_figure = None
if "plot_specific_knesset_selection" not in st.session_state: st.session_state.plot_specific_knesset_selection = ""


# For Interactive Chart Builder
if "builder_selected_table" not in st.session_state: st.session_state.builder_selected_table = None 
if "builder_selected_table_previous_run" not in st.session_state: st.session_state.builder_selected_table_previous_run = None 
if "builder_chart_type" not in st.session_state: st.session_state.builder_chart_type = "bar"
if "previous_builder_chart_type" not in st.session_state: st.session_state.previous_builder_chart_type = "bar" 
if "builder_columns" not in st.session_state: st.session_state.builder_columns = []
if "builder_numeric_columns" not in st.session_state: st.session_state.builder_numeric_columns = []
if "builder_categorical_columns" not in st.session_state: st.session_state.builder_categorical_columns = []
if "builder_x_axis" not in st.session_state: st.session_state.builder_x_axis = None
if "builder_y_axis" not in st.session_state: st.session_state.builder_y_axis = None
if "builder_color" not in st.session_state: st.session_state.builder_color = None
if "builder_size" not in st.session_state: st.session_state.builder_size = None
if "builder_facet_row" not in st.session_state: st.session_state.builder_facet_row = None
if "builder_facet_col" not in st.session_state: st.session_state.builder_facet_col = None
if "builder_hover_name" not in st.session_state: st.session_state.builder_hover_name = None
if "builder_names" not in st.session_state: st.session_state.builder_names = None
if "builder_values" not in st.session_state: st.session_state.builder_values = None
if "builder_log_x" not in st.session_state: st.session_state.builder_log_x = False
if "builder_log_y" not in st.session_state: st.session_state.builder_log_y = False
if "builder_barmode" not in st.session_state: st.session_state.builder_barmode = "relative"
if "builder_generated_chart" not in st.session_state: st.session_state.builder_generated_chart = None
if "builder_knesset_filter_cs" not in st.session_state: st.session_state.builder_knesset_filter_cs = []
if "builder_faction_filter_cs" not in st.session_state: st.session_state.builder_faction_filter_cs = []
if "builder_data_for_cs_filters" not in st.session_state: st.session_state.builder_data_for_cs_filters = pd.DataFrame()

ui_logger.info(f"Session state after initialization: show_query_results is {st.session_state.get('show_query_results', 'NOT FOUND')}")
ui_logger.info("--- Finished initializing session state ---")


# Fetch global filter options once
knesset_nums_options_global, factions_options_df_global = ui_utils.get_filter_options_from_db(backend_config.DB_PATH, ui_logger)
faction_display_map_global = {
    f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"]
    for _, row in factions_options_df_global.iterrows()
}

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar UI (Delegated)
# ──────────────────────────────────────────────────────────────────────────────
sc.display_sidebar(
    db_path_arg=backend_config.DB_PATH,
    exports_arg=EXPORTS,
    connect_func_arg=lambda read_only=True: ui_utils.connect_db(backend_config.DB_PATH, read_only, _logger_obj=ui_logger),
    get_db_table_list_func_arg=lambda: ui_utils.get_db_table_list(backend_config.DB_PATH, _logger_obj=ui_logger),
    get_table_columns_func_arg=lambda table_name: ui_utils.get_table_columns(backend_config.DB_PATH, table_name, _logger_obj=ui_logger),
    get_filter_options_func_arg=lambda: ui_utils.get_filter_options_from_db(backend_config.DB_PATH, _logger_obj=ui_logger),
    faction_display_map_arg=faction_display_map_global,
    ui_logger_arg=ui_logger,
    format_exc_func_arg=ui_utils.format_exception_for_ui
)

# ──────────────────────────────────────────────────────────────────────────────
# Main Area UI
# ──────────────────────────────────────────────────────────────────────────────
st.title("🇮🇱 Knesset Data Warehouse Console")

with st.expander("ℹ️ How This Works", expanded=False):
    st.markdown(dedent(f"""
        * **Data Refresh:** Use sidebar controls to fetch OData tables or update faction statuses.
        * **Predefined Queries:** Select a query, apply filters, click "Run". Results appear below.
        * **Interactive Table Explorer:** Select a table, apply filters, click "Explore". Results appear below.
        * **Predefined Visualizations:** Select a plot topic, then a specific plot. If your sidebar Knesset filter isn't for a single Knesset, a dropdown will appear to let you focus the plot on one Knesset.
        * **Interactive Chart Builder:** Dynamically create your own charts. Data for charts is filtered by sidebar selections and then by chart-specific filters. Data limited to {MAX_ROWS_FOR_CHART_BUILDER} rows. Faceting is limited to columns with fewer than {MAX_UNIQUE_VALUES_FOR_FACET} unique values.
        * **Ad-hoc SQL:** Use the sandbox at the bottom to run custom SQL.
    """))

# --- Predefined Query Results Area ---
st.divider()
st.header("📄 Predefined Query Results")
if st.session_state.get("show_query_results", False) and st.session_state.get("executed_query_name"):
    subheader_text = f"Results for: **{st.session_state.executed_query_name}**"
    if st.session_state.get("applied_filters_info_query"):
        subheader_text += f" (Active Filters: *{'; '.join(st.session_state.applied_filters_info_query)}*)"
    st.markdown(subheader_text)
    if not st.session_state.query_results_df.empty:
        st.dataframe(st.session_state.query_results_df, use_container_width=True, height=400)
        safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", st.session_state.executed_query_name)
        col_csv, col_excel = st.columns(2)
        with col_csv:
            st.download_button("⬇️ CSV", st.session_state.query_results_df.to_csv(index=False).encode("utf-8-sig"), f"{safe_name}_results.csv", "text/csv")
        with col_excel:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                st.session_state.query_results_df.to_excel(writer, index=False, sheet_name="Results")
            st.download_button("⬇️ Excel", excel_buffer.getvalue(), f"{safe_name}_results.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("The query returned no results with the current filters.")
    with st.expander("Show Executed SQL", expanded=False):
        st.code(st.session_state.get("last_executed_sql", "No SQL executed yet."), language="sql")
else:
    st.info("Run a predefined query from the sidebar to see results here.")

# --- Table Explorer Results Area ---
st.divider()
st.header("📖 Interactive Table Explorer Results")
if st.session_state.get("show_table_explorer_results", False) and st.session_state.get("executed_table_explorer_name"):
    st.subheader(f"Exploring: **{st.session_state.executed_table_explorer_name}**")
    k_filters = st.session_state.get("ms_knesset_filter", [])
    f_filters = st.session_state.get("ms_faction_filter", [])
    st.markdown(f"Active Filters: Knesset(s): `{k_filters or 'All'}` Faction(s): `{f_filters or 'All'}`")
    if not st.session_state.table_explorer_df.empty:
        st.dataframe(st.session_state.table_explorer_df, use_container_width=True, height=400)
        safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", st.session_state.executed_table_explorer_name)
        col_csv, col_excel = st.columns(2)
        with col_csv:
            st.download_button("⬇️ CSV", st.session_state.table_explorer_df.to_csv(index=False).encode("utf-8-sig"), f"{safe_name}_data.csv", "text/csv")
        with col_excel:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                st.session_state.table_explorer_df.to_excel(writer, index=False, sheet_name="TableData")
            st.download_button("⬇️ Excel", excel_buffer.getvalue(), f"{safe_name}_data.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("The table exploration returned no results with the current filters.")
else:
    st.info("Explore a table from the sidebar to see its data here.")

# --- Predefined Data Visualizations Section ---
st.divider()
st.header("📈 Predefined Visualizations")
if not backend_config.DB_PATH.exists():
    st.warning("Database not found. Visualizations cannot be generated. Please run a data refresh.")
else:
    plot_topic_options = [""] + list(AVAILABLE_PLOTS_BY_TOPIC.keys())
    current_selected_topic = st.session_state.get("selected_plot_topic", "")
    topic_select_default_index = 0
    if current_selected_topic in plot_topic_options:
        try: topic_select_default_index = plot_topic_options.index(current_selected_topic)
        except ValueError: topic_select_default_index = 0
    
    selected_topic_widget = st.selectbox(
        "1. Choose Plot Topic:",
        options=plot_topic_options,
        index=topic_select_default_index,
        key="sb_selected_plot_topic_widget"
    )

    if selected_topic_widget != st.session_state.get("selected_plot_topic"):
        st.session_state.selected_plot_topic = selected_topic_widget
        st.session_state.selected_plot_name_from_topic = "" 
        st.session_state.plot_specific_knesset_selection = "" 
        st.rerun()

    selected_plot_name_for_display = "" 
    if st.session_state.get("selected_plot_topic"):
        charts_in_topic = AVAILABLE_PLOTS_BY_TOPIC[st.session_state.selected_plot_topic]
        chart_options_for_topic = [""] + list(charts_in_topic.keys())
        
        current_selected_chart_from_topic = st.session_state.get("selected_plot_name_from_topic", "")
        chart_select_default_index = 0
        if current_selected_chart_from_topic in chart_options_for_topic:
            try: chart_select_default_index = chart_options_for_topic.index(current_selected_chart_from_topic)
            except ValueError: chart_select_default_index = 0

        selected_chart_widget = st.selectbox(
            f"2. Choose Visualization for '{st.session_state.selected_plot_topic}':",
            options=chart_options_for_topic,
            index=chart_select_default_index,
            key=f"sb_selected_chart_for_topic_{st.session_state.selected_plot_topic.replace(' ', '_')}"
        )

        if selected_chart_widget != st.session_state.get("selected_plot_name_from_topic"):
            st.session_state.selected_plot_name_from_topic = selected_chart_widget
            st.session_state.plot_specific_knesset_selection = "" 
            st.rerun()
        
        selected_plot_name_for_display = st.session_state.selected_plot_name_from_topic

    final_knesset_filter_for_plot = None 
    
    if selected_plot_name_for_display and selected_plot_name_for_display != "": 
        knesset_filter_from_sidebar = st.session_state.get("ms_knesset_filter", [])

        if len(knesset_filter_from_sidebar) == 1:
            final_knesset_filter_for_plot = knesset_filter_from_sidebar
            ui_logger.info(f"Predefined plot '{selected_plot_name_for_display}': Using single Knesset from sidebar: {final_knesset_filter_for_plot}")
        else:
            plot_specific_knesset_options = [""] + knesset_nums_options_global 
            current_plot_specific_knesset = st.session_state.get("plot_specific_knesset_selection", "")
            
            selected_knesset_widget_val = st.selectbox(
                f"Focus '{selected_plot_name_for_display}' on a single Knesset:",
                options=plot_specific_knesset_options,
                index=plot_specific_knesset_options.index(current_plot_specific_knesset) if current_plot_specific_knesset in plot_specific_knesset_options else 0,
                key=f"plot_specific_knesset_selector_for_{selected_plot_name_for_display.replace(' ', '_')}" 
            )

            if selected_knesset_widget_val != current_plot_specific_knesset:
                 st.session_state.plot_specific_knesset_selection = selected_knesset_widget_val
                 st.rerun() 

            if st.session_state.plot_specific_knesset_selection and st.session_state.plot_specific_knesset_selection != "":
                try:
                    final_knesset_filter_for_plot = [int(st.session_state.plot_specific_knesset_selection)]
                    ui_logger.info(f"Predefined plot '{selected_plot_name_for_display}': Using plot-specific Knesset: {final_knesset_filter_for_plot}")
                except ValueError:
                    ui_logger.error(f"Could not convert plot-specific Knesset selection '{st.session_state.plot_specific_knesset_selection}' to int.")
                    st.error("Invalid Knesset number selected for plot focus.")
                    final_knesset_filter_for_plot = None
            else:
                st.info(f"To view the '{selected_plot_name_for_display}' plot, please select a specific Knesset above or choose a single Knesset in the sidebar filters.")
                final_knesset_filter_for_plot = None 

        if final_knesset_filter_for_plot: 
            plot_function = AVAILABLE_PLOTS_BY_TOPIC[st.session_state.selected_plot_topic][selected_plot_name_for_display]
            with st.spinner(f"Generating '{selected_plot_name_for_display}' for Knesset(s) {final_knesset_filter_for_plot}..."):
                try:
                    figure = plot_function(
                        backend_config.DB_PATH,
                        lambda read_only=True: ui_utils.connect_db(backend_config.DB_PATH, read_only, _logger_obj=ui_logger),
                        ui_logger,
                        knesset_filter=final_knesset_filter_for_plot,
                        faction_filter=[faction_display_map_global[name] for name in st.session_state.ms_faction_filter if name in faction_display_map_global]
                    )
                    if figure:
                        st.plotly_chart(figure, use_container_width=True)
                        st.session_state.generated_plot_figure = figure
                except Exception as e:
                    ui_logger.error(f"Error displaying plot '{selected_plot_name_for_display}': {e}", exc_info=True)
                    st.error(f"An error occurred while generating the plot '{selected_plot_name_for_display}': {ui_utils.format_exception_for_ui(sys.exc_info())}")
                    st.code(str(e) + "\n\n" + ui_utils.format_exception_for_ui(sys.exc_info()))
    elif st.session_state.get("selected_plot_topic"):
        st.info("Please choose a specific visualization from the dropdown above.")
    else: 
        st.info("Select a plot topic to see available visualizations.")

# --- Interactive Chart Builder Section (Delegated) ---
st.divider()
cb_ui.display_chart_builder(
    db_path=backend_config.DB_PATH,
    max_rows_for_chart_builder=MAX_ROWS_FOR_CHART_BUILDER,
    max_unique_values_for_facet=MAX_UNIQUE_VALUES_FOR_FACET,
    faction_display_map_global=faction_display_map_global,
    logger_obj=ui_logger
)

# --- Ad-hoc SQL Query Section ---
st.divider()
with st.expander("🧑‍🔬 Run an Ad-hoc SQL Query (Advanced)", expanded=False):
    if not backend_config.DB_PATH.exists(): st.warning("Database not found. Cannot run SQL queries.")
    else:
        st.markdown("Construct your SQL query. Sidebar filters are **not** automatically applied here. Include them in your `WHERE` clause if needed.")
        default_sql_query = "SELECT t.table_name, t.row_count FROM duckdb_tables() t WHERE t.schema_name = 'main' ORDER BY t.table_name;"
        sql_query_input = st.text_area("Enter your SQL query:", default_sql_query, height=150, key="adhoc_sql_query")
        if st.button("▶︎ Run Ad-hoc SQL", key="run_adhoc_sql"):
            if sql_query_input.strip():
                try:
                    con = ui_utils.connect_db(backend_config.DB_PATH, read_only=True, _logger_obj=ui_logger)
                    adhoc_result_df = ui_utils.safe_execute_query(con, sql_query_input, _logger_obj=ui_logger)
                    con.close()
                    st.dataframe(adhoc_result_df, use_container_width=True)
                    if not adhoc_result_df.empty:
                        st.download_button("⬇️ CSV", adhoc_result_df.to_csv(index=False).encode("utf-8-sig"), "adhoc_results.csv", "text/csv")
                except Exception as e:
                    ui_logger.error(f"❌ Ad-hoc SQL Query Error: {e}", exc_info=True)
                    st.error(f"❌ SQL Query Error: {e}")
                    st.code(str(e) + "\n\n" + ui_utils.format_exception_for_ui(sys.exc_info()))
            else: st.warning("SQL query cannot be empty.")

# --- Table Update Status ---
st.divider()
with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
    if backend_config.DB_PATH.exists():
        tables_to_check_status_main = sorted(list(set(TABLES)))
        status_data_main = [{"Table": t_name, "Last Updated (Parquet Mod Time)": ui_utils.get_last_updated_for_table(backend_config.PARQUET_DIR, t_name, ui_logger)} for t_name in tables_to_check_status_main]
        if status_data_main: st.dataframe(pd.DataFrame(status_data_main), hide_index=True, use_container_width=True)
        else: st.info("No tables found to display status, or TABLES list is empty.")
    else: st.info("Database not found. Table status cannot be displayed.")

