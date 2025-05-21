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
import ui.plot_generators as pg
import ui.sidebar_components as sc
import ui.ui_utils as ui_utils
import ui.chart_builder_ui as cb_ui

# Initialize logger for the UI module
ui_logger = setup_logging("knesset.ui.data_refresh", console_output=True)
ui_logger.info("--- data_refresh.py script started ---")


# --- Constants and Global-like Configurations ---
DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/parquet")
MAX_ROWS_FOR_CHART_BUILDER = 50000
MAX_UNIQUE_VALUES_FOR_FACET = 50

EXPORTS = {
    "Queries + Full Details": {
        "sql": """
WITH MKLatestFactionDetailsInKnesset AS (
    -- This CTE finds the most recent (or primary) faction and coalition status for each MK in each Knesset they served.
    -- It ranks positions by StartDate descending, so rn=1 is the latest.
    SELECT
        p2p.PersonID,
        p2p.KnessetNum,
        p2p.FactionID,
        p2p.FactionName,
        ufs.CoalitionStatus,
        ROW_NUMBER() OVER (PARTITION BY p2p.PersonID, p2p.KnessetNum ORDER BY p2p.StartDate DESC, p2p.FinishDate DESC NULLS LAST) as rn
    FROM KNS_PersonToPosition p2p
    LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum
    WHERE p2p.FactionID IS NOT NULL -- Only consider records where there is a faction
)
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
    P.GenderDesc AS MKGender,
    
    -- Use FactionName from active position if available, otherwise from latest known position in that Knesset
    COALESCE(P2P_active.FactionName, FallbackFaction.FactionName) AS MKFactionName,
    -- Use CoalitionStatus from active position if available, otherwise from latest known position in that Knesset
    COALESCE(UFS_active.CoalitionStatus, FallbackFaction.CoalitionStatus) AS MKFactionCoalitionStatus,
    
    M.Name AS MinistryName,
    strftime(CAST(Q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDateFormatted
FROM KNS_Query Q
LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
LEFT JOIN KNS_GovMinistry M ON Q.GovMinistryID = M.GovMinistryID
LEFT JOIN KNS_Status S ON Q.StatusID = S.StatusID

-- Primary attempt: Join KNS_PersonToPosition active at query submission time
LEFT JOIN KNS_PersonToPosition P2P_active ON Q.PersonID = P2P_active.PersonID
    AND Q.KnessetNum = P2P_active.KnessetNum
    AND CAST(Q.SubmitDate AS TIMESTAMP) BETWEEN CAST(P2P_active.StartDate AS TIMESTAMP) AND CAST(COALESCE(P2P_active.FinishDate, '9999-12-31') AS TIMESTAMP)
-- Join UserFactionCoalitionStatus based on this active position
LEFT JOIN UserFactionCoalitionStatus UFS_active ON P2P_active.FactionID = UFS_active.FactionID AND P2P_active.KnessetNum = UFS_active.KnessetNum

-- Fallback: Join with the latest faction details for that MK in that Knesset
LEFT JOIN MKLatestFactionDetailsInKnesset FallbackFaction ON Q.PersonID = FallbackFaction.PersonID
    AND Q.KnessetNum = FallbackFaction.KnessetNum AND FallbackFaction.rn = 1
    
ORDER BY Q.KnessetNum DESC, Q.QueryID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "Q.KnessetNum", # This remains the same for filtering the overall query
        "faction_filter_column": "COALESCE(P2P_active.FactionID, FallbackFaction.FactionID)", # Filter on the effective FactionID
    },
    "Agenda Items + Full Details": {
        "sql": """
WITH MKLatestFactionDetailsInKnesset AS (
    SELECT
        p2p.PersonID,
        p2p.KnessetNum,
        p2p.FactionID,
        p2p.FactionName,
        ufs.CoalitionStatus,
        ROW_NUMBER() OVER (PARTITION BY p2p.PersonID, p2p.KnessetNum ORDER BY p2p.StartDate DESC, p2p.FinishDate DESC NULLS LAST) as rn
    FROM KNS_PersonToPosition p2p
    LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum
    WHERE p2p.FactionID IS NOT NULL
)
SELECT
    A.AgendaID,
    A.Number AS AgendaNumber,
    A.KnessetNum,
    A.Name AS AgendaName,
    A.ClassificationDesc AS AgendaClassification,
    S.Desc AS AgendaStatus,
    INIT_P.FirstName AS InitiatorFirstName,
    INIT_P.LastName AS InitiatorLastName,
    INIT_P.GenderDesc AS InitiatorGender,

    COALESCE(P2P_active_init.FactionName, FallbackFaction_init.FactionName) AS InitiatorFactionName,
    COALESCE(UFS_active_init.CoalitionStatus, FallbackFaction_init.CoalitionStatus) AS InitiatorFactionCoalitionStatus,

    HC.Name AS HandlingCommitteeName,
    strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted
FROM KNS_Agenda A
LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
LEFT JOIN KNS_Person INIT_P ON A.InitiatorPersonID = INIT_P.PersonID
LEFT JOIN KNS_Committee HC ON A.CommitteeID = HC.CommitteeID

-- Primary attempt for Initiator
LEFT JOIN KNS_PersonToPosition P2P_active_init ON A.InitiatorPersonID = P2P_active_init.PersonID
    AND A.KnessetNum = P2P_active_init.KnessetNum
    AND CAST(COALESCE(A.PresidentDecisionDate, A.LastUpdatedDate) AS TIMESTAMP) BETWEEN CAST(P2P_active_init.StartDate AS TIMESTAMP) AND CAST(COALESCE(P2P_active_init.FinishDate, '9999-12-31') AS TIMESTAMP)
LEFT JOIN UserFactionCoalitionStatus UFS_active_init ON P2P_active_init.FactionID = UFS_active_init.FactionID AND P2P_active_init.KnessetNum = UFS_active_init.KnessetNum

-- Fallback for Initiator
LEFT JOIN MKLatestFactionDetailsInKnesset FallbackFaction_init ON A.InitiatorPersonID = FallbackFaction_init.PersonID
    AND A.KnessetNum = FallbackFaction_init.KnessetNum AND FallbackFaction_init.rn = 1

ORDER BY A.KnessetNum DESC, A.AgendaID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column": "COALESCE(P2P_active_init.FactionID, FallbackFaction_init.FactionID)",
    },
}

# Plot names updated to reflect single Knesset focus where applicable
AVAILABLE_PLOTS_BY_TOPIC = {
    "Queries": {
        "Queries by Time Period": pg.plot_queries_by_time_period,
        "Distribution of Query Types (Single Knesset)": pg.plot_query_types_distribution,
        "Queries by Faction Status (Single Knesset)": pg.plot_queries_by_faction_status,
        "Queries per Faction (Single Knesset)": pg.plot_queries_per_faction_in_knesset,
        "Queries by Coalition & Answer Status (Single Knesset)": pg.plot_queries_by_coalition_and_answer_status,
        "Query Performance by Ministry (Single Knesset)": pg.plot_queries_by_ministry_and_status,
    },
    "Agendas": {
        "Agenda Items by Time Period": pg.plot_agendas_by_time_period,
        "Distribution of Agenda Classifications (Single Knesset)": pg.plot_agenda_classifications_pie,
        "Agenda Item Status Distribution (Single Knesset)": pg.plot_agenda_status_distribution,
        "Agendas per Faction (Single Knesset)": pg.plot_agendas_per_faction_in_knesset,
        "Agendas by Coalition & Status (Single Knesset)": pg.plot_agendas_by_coalition_and_status,
    }
}

st.set_page_config(page_title="Knesset OData – Refresh & Export", layout="wide")

ui_logger.info("--- Initializing session state ---")
if "selected_query_name" not in st.session_state: st.session_state.selected_query_name = None
if "executed_query_name" not in st.session_state: st.session_state.executed_query_name = None
if "executed_sql_string" not in st.session_state: st.session_state.executed_sql_string = ""
if "query_results_df" not in st.session_state: st.session_state.query_results_df = pd.DataFrame()
if "show_query_results" not in st.session_state: st.session_state.show_query_results = False
if "applied_knesset_filter_to_query" not in st.session_state: st.session_state.applied_knesset_filter_to_query = []
if "last_executed_sql" not in st.session_state: st.session_state.last_executed_sql = ""

if "selected_table_for_explorer" not in st.session_state: st.session_state.selected_table_for_explorer = None
if "executed_table_explorer_name" not in st.session_state: st.session_state.executed_table_explorer_name = None
if "table_explorer_df" not in st.session_state: st.session_state.table_explorer_df = pd.DataFrame()
if "show_table_explorer_results" not in st.session_state: st.session_state.show_table_explorer_results = False

if "ms_knesset_filter" not in st.session_state: st.session_state.ms_knesset_filter = []
if "ms_faction_filter" not in st.session_state: st.session_state.ms_faction_filter = []

if "selected_plot_topic" not in st.session_state: st.session_state.selected_plot_topic = ""
if "selected_plot_name_from_topic" not in st.session_state: st.session_state.selected_plot_name_from_topic = ""
if "generated_plot_figure" not in st.session_state: st.session_state.generated_plot_figure = None
if "plot_main_knesset_selection" not in st.session_state: st.session_state.plot_main_knesset_selection = ""
if "plot_aggregation_level" not in st.session_state: st.session_state.plot_aggregation_level = "Yearly"
if "plot_show_average_line" not in st.session_state: st.session_state.plot_show_average_line = False

if "builder_selected_table" not in st.session_state: st.session_state.builder_selected_table = None
if "builder_selected_table_previous_run" not in st.session_state: st.session_state.builder_selected_table_previous_run = None
# ... (rest of chart builder session state)

ui_logger.info("--- Finished initializing session state ---")

knesset_nums_options_global, factions_options_df_global = ui_utils.get_filter_options_from_db(DB_PATH, ui_logger)
faction_display_map_global = {
    f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"]
    for _, row in factions_options_df_global.iterrows()
}

sc.display_sidebar(
    db_path_arg=DB_PATH,
    exports_arg=EXPORTS,
    connect_func_arg=lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger),
    get_db_table_list_func_arg=lambda: ui_utils.get_db_table_list(DB_PATH, _logger_obj=ui_logger),
    get_table_columns_func_arg=lambda table_name: ui_utils.get_table_columns(DB_PATH, table_name, _logger_obj=ui_logger),
    get_filter_options_func_arg=lambda: (knesset_nums_options_global, factions_options_df_global),
    faction_display_map_arg=faction_display_map_global,
    ui_logger_arg=ui_logger,
    format_exc_func_arg=ui_utils.format_exception_for_ui
)

st.title("🇮🇱 Knesset Data Warehouse Console")

with st.expander("ℹ️ How This Works", expanded=False):
    st.markdown(dedent(f"""
        * **Data Refresh:** Use sidebar controls to fetch OData tables or update faction statuses.
        * **Predefined Queries & Table Explorer:** These sections use the **sidebar filters** for Knesset and Faction.
        * **Predefined Visualizations:**
            * Select a plot topic, then a specific plot.
            * **A Knesset selector will appear below these dropdowns.** This is the primary Knesset filter for the plots.
            * For plots like "Queries/Agendas by Time Period", you can select "All Knessets (Color Coded)" to see multiple Knessets, or pick a specific one. Other plots will typically focus on the single selected Knesset.
            * Time-based plots also offer aggregation level and average line options.
        * **Interactive Chart Builder:** Data for charts is filtered by sidebar selections and then by chart-specific filters.
        * **Ad-hoc SQL:** Use the sandbox at the bottom to run custom SQL.
    """))

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


st.divider()
st.header("📖 Interactive Table Explorer Results")
if st.session_state.get("show_table_explorer_results", False) and st.session_state.get("executed_table_explorer_name"):
    st.subheader(f"Exploring: **{st.session_state.executed_table_explorer_name}**")
    k_filters_sidebar = st.session_state.get("ms_knesset_filter", [])
    f_filters_sidebar = st.session_state.get("ms_faction_filter", [])
    st.markdown(f"Active Sidebar Filters: Knesset(s): `{k_filters_sidebar or 'All'}` Faction(s): `{f_filters_sidebar or 'All'}`")
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


st.divider()
st.header("📈 Predefined Visualizations")
if not DB_PATH.exists():
    st.warning("Database not found. Visualizations cannot be generated. Please run a data refresh.")
else:
    plot_topic_options = [""] + list(AVAILABLE_PLOTS_BY_TOPIC.keys())
    current_selected_topic = st.session_state.get("selected_plot_topic", "")
    topic_select_default_index = plot_topic_options.index(current_selected_topic) if current_selected_topic in plot_topic_options else 0

    selected_topic_widget = st.selectbox(
        "1. Choose Plot Topic:",
        options=plot_topic_options,
        index=topic_select_default_index,
        key="sb_selected_plot_topic_widget"
    )

    if selected_topic_widget != st.session_state.get("selected_plot_topic"):
        st.session_state.selected_plot_topic = selected_topic_widget
        st.session_state.selected_plot_name_from_topic = ""
        st.session_state.plot_main_knesset_selection = ""
        st.session_state.plot_aggregation_level = "Yearly"
        st.session_state.plot_show_average_line = False
        st.rerun()

    selected_plot_name_for_display = ""
    if st.session_state.get("selected_plot_topic"):
        charts_in_topic = AVAILABLE_PLOTS_BY_TOPIC[st.session_state.selected_plot_topic]
        chart_options_for_topic = [""] + list(charts_in_topic.keys())
        current_selected_chart_from_topic = st.session_state.get("selected_plot_name_from_topic", "")
        chart_select_default_index = chart_options_for_topic.index(current_selected_chart_from_topic) if current_selected_chart_from_topic in chart_options_for_topic else 0

        selected_chart_widget = st.selectbox(
            f"2. Choose Visualization for '{st.session_state.selected_plot_topic}':",
            options=chart_options_for_topic,
            index=chart_select_default_index,
            key=f"sb_selected_chart_for_topic_{st.session_state.selected_plot_topic.replace(' ', '_')}"
        )

        if selected_chart_widget != st.session_state.get("selected_plot_name_from_topic"):
            st.session_state.selected_plot_name_from_topic = selected_chart_widget
            st.session_state.plot_aggregation_level = "Yearly"
            st.session_state.plot_show_average_line = False
            st.rerun()
        selected_plot_name_for_display = st.session_state.selected_plot_name_from_topic

    final_knesset_filter_for_plot = None
    plot_knesset_options = [""]
    if knesset_nums_options_global:
        plot_knesset_options.extend(sorted(knesset_nums_options_global, key=int, reverse=True))

    can_show_all_knessets = selected_plot_name_for_display in ["Queries by Time Period", "Agenda Items by Time Period"]
    if can_show_all_knessets:
        if "All Knessets (Color Coded)" not in plot_knesset_options:
             plot_knesset_options.insert(1, "All Knessets (Color Coded)")


    if selected_plot_name_for_display:
        current_main_knesset_selection_in_state = st.session_state.get("plot_main_knesset_selection", "")
        
        if current_main_knesset_selection_in_state not in plot_knesset_options:
            current_main_knesset_selection_in_state = "" 
            st.session_state.plot_main_knesset_selection = ""


        knesset_select_default_index = plot_knesset_options.index(current_main_knesset_selection_in_state) \
            if current_main_knesset_selection_in_state in plot_knesset_options else 0

        aggregation_level_for_plot = st.session_state.get("plot_aggregation_level", "Yearly")
        show_average_line_for_plot = st.session_state.get("plot_show_average_line", False)
        
        selected_knesset_main_area_val = "" 

        if selected_plot_name_for_display in ["Queries by Time Period", "Agenda Items by Time Period"]:
            col_knesset_select, col_agg_select, col_avg_line = st.columns([2, 1, 1])
            with col_knesset_select:
                selected_knesset_main_area_val = st.selectbox(
                    "3. Select Knesset for Plot:",
                    options=plot_knesset_options,
                    index=knesset_select_default_index,
                    key="plot_main_knesset_selector_tp" 
                )
            with col_agg_select:
                st.session_state.plot_aggregation_level = st.selectbox(
                    "Aggregate:", options=["Yearly", "Monthly", "Quarterly"],
                    index=["Yearly", "Monthly", "Quarterly"].index(aggregation_level_for_plot),
                    key=f"agg_level_{selected_plot_name_for_display.replace(' ', '_')}"
                )
            with col_avg_line:
                st.session_state.plot_show_average_line = st.checkbox(
                    "Avg Line", value=show_average_line_for_plot,
                    key=f"avg_line_{selected_plot_name_for_display.replace(' ', '_')}"
                )
            aggregation_level_for_plot = st.session_state.plot_aggregation_level
            show_average_line_for_plot = st.session_state.plot_show_average_line
        else: 
            options_for_single_knesset_plot = [opt for opt in plot_knesset_options if opt != "All Knessets (Color Coded)"]
            if current_main_knesset_selection_in_state not in options_for_single_knesset_plot:
                 current_main_knesset_selection_in_state = "" 
                 st.session_state.plot_main_knesset_selection = ""

            single_knesset_default_idx = options_for_single_knesset_plot.index(current_main_knesset_selection_in_state) \
                if current_main_knesset_selection_in_state in options_for_single_knesset_plot else 0

            selected_knesset_main_area_val = st.selectbox(
                "3. Select Knesset for Plot:",
                options=options_for_single_knesset_plot,
                index=single_knesset_default_idx,
                key="plot_main_knesset_selector_single" 
            )

        if selected_knesset_main_area_val != st.session_state.get("plot_main_knesset_selection", ""):
            st.session_state.plot_main_knesset_selection = selected_knesset_main_area_val
            st.rerun()
        
        current_selection_for_filter = st.session_state.get("plot_main_knesset_selection")
        if current_selection_for_filter == "All Knessets (Color Coded)" and can_show_all_knessets:
            final_knesset_filter_for_plot = None
            ui_logger.info(f"Plot '{selected_plot_name_for_display}': Showing all Knessets (color coded).")
        elif current_selection_for_filter and current_selection_for_filter != "":
            try:
                final_knesset_filter_for_plot = [int(current_selection_for_filter)]
                ui_logger.info(f"Plot '{selected_plot_name_for_display}': Using main area Knesset selection: {final_knesset_filter_for_plot}")
            except ValueError:
                st.error(f"Invalid Knesset number selected: {current_selection_for_filter}")
                final_knesset_filter_for_plot = False
        else:
            requires_single = "(Single Knesset)" in selected_plot_name_for_display or not can_show_all_knessets
            if requires_single:
                 st.info(f"Please select a Knesset for the '{selected_plot_name_for_display}' plot.")
            final_knesset_filter_for_plot = False


        can_generate_plot = selected_plot_name_for_display and final_knesset_filter_for_plot is not False

        if can_generate_plot:
            plot_function = AVAILABLE_PLOTS_BY_TOPIC[st.session_state.selected_plot_topic][selected_plot_name_for_display]
            plot_args = {
                "db_path": DB_PATH,
                "connect_func": lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger),
                "logger_obj": ui_logger,
                "knesset_filter": final_knesset_filter_for_plot,
                "faction_filter": [faction_display_map_global[name] for name in st.session_state.ms_faction_filter if name in faction_display_map_global]
            }
            if selected_plot_name_for_display in ["Queries by Time Period", "Agenda Items by Time Period"]:
                plot_args["aggregation_level"] = aggregation_level_for_plot
                plot_args["show_average_line"] = show_average_line_for_plot

            with st.spinner(f"Generating '{selected_plot_name_for_display}'..."):
                try:
                    figure = plot_function(**plot_args)
                    if figure:
                        st.plotly_chart(figure, use_container_width=True)
                        st.session_state.generated_plot_figure = figure
                except Exception as e:
                    ui_logger.error(f"Error displaying plot '{selected_plot_name_for_display}': {e}", exc_info=True)
                    st.error(f"An error occurred while generating the plot: {e}")
                    st.code(str(e) + "\n\n" + ui_utils.format_exception_for_ui(sys.exc_info()))

    elif st.session_state.get("selected_plot_topic"):
        st.info("Please choose a specific visualization from the dropdown above.")
    else:
        st.info("Select a plot topic to see available visualizations.")


st.divider()
cb_ui.display_chart_builder(
    db_path=DB_PATH,
    max_rows_for_chart_builder=MAX_ROWS_FOR_CHART_BUILDER,
    max_unique_values_for_facet=MAX_UNIQUE_VALUES_FOR_FACET,
    faction_display_map_global=faction_display_map_global,
    logger_obj=ui_logger
)

st.divider()
with st.expander("🧑‍🔬 Run an Ad-hoc SQL Query (Advanced)", expanded=False):
    if not DB_PATH.exists(): st.warning("Database not found. Cannot run SQL queries.")
    else:
        st.markdown("Construct your SQL query. Sidebar filters are **not** automatically applied here. Include them in your `WHERE` clause if needed.")
        default_sql_query = "SELECT t.table_name, t.row_count FROM duckdb_tables() t WHERE t.schema_name = 'main' ORDER BY t.table_name;"
        sql_query_input = st.text_area("Enter your SQL query:", default_sql_query, height=150, key="adhoc_sql_query")
        if st.button("▶︎ Run Ad-hoc SQL", key="run_adhoc_sql"):
            if sql_query_input.strip():
                try:
                    con = ui_utils.connect_db(DB_PATH, read_only=True, _logger_obj=ui_logger)
                    adhoc_result_df = ui_utils.safe_execute_query(con, sql_query_input, _logger_obj=ui_logger)
                    if con: con.close()
                    st.dataframe(adhoc_result_df, use_container_width=True)
                    if not adhoc_result_df.empty:
                        st.download_button("⬇️ CSV", adhoc_result_df.to_csv(index=False).encode("utf-8-sig"), "adhoc_results.csv", "text/csv")
                except Exception as e:
                    ui_logger.error(f"❌ Ad-hoc SQL Query Error: {e}", exc_info=True)
                    st.error(f"❌ SQL Query Error: {e}")
                    st.code(str(e) + "\n\n" + ui_utils.format_exception_for_ui(sys.exc_info()))
                    if 'con' in locals() and con: con.close()
            else: st.warning("SQL query cannot be empty.")


st.divider()
with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
    if DB_PATH.exists():
        tables_to_check_status_main = sorted(list(set(TABLES)))
        status_data_main = [{"Table": t_name, "Last Updated (Parquet Mod Time)": ui_utils.get_last_updated_for_table(PARQUET_DIR, t_name, ui_logger)} for t_name in tables_to_check_status_main]
        if status_data_main: st.dataframe(pd.DataFrame(status_data_main), hide_index=True, use_container_width=True)
        else: st.info("No tables found to display status, or TABLES list is empty.")
    else: st.info("Database not found. Table status cannot be displayed.")

