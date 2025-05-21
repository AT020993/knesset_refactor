from __future__ import annotations

# Standard Library Imports
import io
import logging # Keep this, logger_setup will configure it
import re  # For safe filename generation and SQL injection
import sys
from pathlib import Path
from textwrap import dedent

# Third-Party Imports
import pandas as pd
import streamlit as st
import duckdb # Explicitly import duckdb if used directly, though ui_utils might handle connections

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
# setup_logging will get the logger named 'knesset.ui.data_refresh'
ui_logger = logging.getLogger("knesset.ui.data_refresh") # Use logging.getLogger
# Call setup_logging if it's not called at a higher level app entry point
# For this module, assuming it might be called elsewhere or we call it here if it's the main entry for this part.
# If setup_logging is idempotent or handles multiple calls, this is fine.
# If this script can be run standalone for its UI, setup_logging here is appropriate.
if not ui_logger.handlers: # Basic check if logger is already configured
    setup_logging("knesset.ui.data_refresh", console_output=True)

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
    -- It ranks positions by StartDate descending, then by PersonToPositionID for tie-breaking.
    SELECT
        p2p.PersonID,
        p2p.KnessetNum,
        p2p.FactionID,
        p2p.FactionName,
        ufs.CoalitionStatus,
        p2p.PersonToPositionID, -- Added for tie-breaking in rn
        ROW_NUMBER() OVER (PARTITION BY p2p.PersonID, p2p.KnessetNum ORDER BY p2p.StartDate DESC, p2p.FinishDate DESC NULLS LAST, p2p.PersonToPositionID DESC) as rn
    FROM KNS_PersonToPosition p2p
    LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum
    WHERE p2p.FactionID IS NOT NULL -- Only consider records where there is a faction
),
ActiveMKFactionDetailsForQuery AS (
    -- This CTE finds the single most relevant active faction for an MK at the time of a specific query's submission.
    SELECT
        q_inner.QueryID, -- Link back to the specific query
        p2p_inner.FactionID AS ActiveFactionID,
        p2p_inner.FactionName AS ActiveFactionName,
        ufs_inner.CoalitionStatus AS ActiveCoalitionStatus,
        ROW_NUMBER() OVER (
            PARTITION BY q_inner.QueryID -- Ensure one faction detail per query
            -- Prioritize by StartDate, then by PersonToPositionID to break ties if multiple positions are active on SubmitDate
            ORDER BY p2p_inner.StartDate DESC, p2p_inner.PersonToPositionID DESC
        ) as rn_active
    FROM KNS_Query q_inner -- Start from KNS_Query to get specific QueryID and SubmitDate
    JOIN KNS_PersonToPosition p2p_inner ON q_inner.PersonID = p2p_inner.PersonID -- PersonID is the MK who submitted
        AND q_inner.KnessetNum = p2p_inner.KnessetNum
        -- Check if the position was active on the query's submission date
        AND CAST(q_inner.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p_inner.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p_inner.FinishDate, '9999-12-31') AS TIMESTAMP)
    LEFT JOIN UserFactionCoalitionStatus ufs_inner ON p2p_inner.FactionID = ufs_inner.FactionID AND p2p_inner.KnessetNum = ufs_inner.KnessetNum
    WHERE p2p_inner.FactionID IS NOT NULL -- Ensure the position has an associated faction
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
    
    -- Use FactionName and CoalitionStatus from the uniquely identified active position, otherwise from the latest known fallback position
    COALESCE(AMFD.ActiveFactionName, FallbackFaction.FactionName) AS MKFactionName,
    COALESCE(AMFD.ActiveCoalitionStatus, FallbackFaction.CoalitionStatus) AS MKFactionCoalitionStatus,
    
    M.Name AS MinistryName,
    strftime(CAST(Q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDateFormatted
FROM KNS_Query Q
LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
LEFT JOIN KNS_GovMinistry M ON Q.GovMinistryID = M.GovMinistryID
LEFT JOIN KNS_Status S ON Q.StatusID = S.StatusID

-- Join for the uniquely identified active faction details for this query
LEFT JOIN ActiveMKFactionDetailsForQuery AMFD ON Q.QueryID = AMFD.QueryID AND AMFD.rn_active = 1

-- Fallback: Join with the latest faction details for that MK in that Knesset if no specific active one was found by AMFD
LEFT JOIN MKLatestFactionDetailsInKnesset FallbackFaction ON Q.PersonID = FallbackFaction.PersonID
    AND Q.KnessetNum = FallbackFaction.KnessetNum AND FallbackFaction.rn = 1
    
ORDER BY Q.KnessetNum DESC, Q.QueryID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "Q.KnessetNum",
        "faction_filter_column": "COALESCE(AMFD.ActiveFactionID, FallbackFaction.FactionID)", # Filter on the effective FactionID
    },
    "Agenda Items + Full Details": {
        "sql": """
WITH MKLatestFactionDetailsInKnesset AS (
    -- This CTE finds the most recent (or primary) faction and coalition status for each MK in each Knesset they served.
    SELECT
        p2p.PersonID,
        p2p.KnessetNum,
        p2p.FactionID,
        p2p.FactionName,
        ufs.CoalitionStatus,
        p2p.PersonToPositionID,
        ROW_NUMBER() OVER (PARTITION BY p2p.PersonID, p2p.KnessetNum ORDER BY p2p.StartDate DESC, p2p.FinishDate DESC NULLS LAST, p2p.PersonToPositionID DESC) as rn
    FROM KNS_PersonToPosition p2p
    LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum
    WHERE p2p.FactionID IS NOT NULL
),
ActiveInitiatorFactionDetailsForAgenda AS (
    -- This CTE finds the single most relevant active faction for an agenda item's initiator at the time of decision/update.
    SELECT
        a_inner.AgendaID, -- Link back to the specific agenda item
        p2p_inner.FactionID AS ActiveFactionID,
        p2p_inner.FactionName AS ActiveFactionName,
        ufs_inner.CoalitionStatus AS ActiveCoalitionStatus,
        ROW_NUMBER() OVER (
            PARTITION BY a_inner.AgendaID -- Ensure one faction detail per agenda item's initiator
            ORDER BY p2p_inner.StartDate DESC, p2p_inner.PersonToPositionID DESC
        ) as rn_active
    FROM KNS_Agenda a_inner
    JOIN KNS_PersonToPosition p2p_inner ON a_inner.InitiatorPersonID = p2p_inner.PersonID
        AND a_inner.KnessetNum = p2p_inner.KnessetNum
        AND CAST(COALESCE(a_inner.PresidentDecisionDate, a_inner.LastUpdatedDate) AS TIMESTAMP) -- Relevant date for agenda item
            BETWEEN CAST(p2p_inner.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p_inner.FinishDate, '9999-12-31') AS TIMESTAMP)
    LEFT JOIN UserFactionCoalitionStatus ufs_inner ON p2p_inner.FactionID = ufs_inner.FactionID AND p2p_inner.KnessetNum = ufs_inner.KnessetNum
    WHERE p2p_inner.FactionID IS NOT NULL AND a_inner.InitiatorPersonID IS NOT NULL
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

    COALESCE(AIFD.ActiveFactionName, FallbackFaction_init.FactionName) AS InitiatorFactionName,
    COALESCE(AIFD.ActiveCoalitionStatus, FallbackFaction_init.CoalitionStatus) AS InitiatorFactionCoalitionStatus,

    HC.Name AS HandlingCommitteeName,
    strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted
FROM KNS_Agenda A
LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
LEFT JOIN KNS_Person INIT_P ON A.InitiatorPersonID = INIT_P.PersonID
LEFT JOIN KNS_Committee HC ON A.CommitteeID = HC.CommitteeID

-- Join for the uniquely identified active faction details for this agenda's initiator
LEFT JOIN ActiveInitiatorFactionDetailsForAgenda AIFD ON A.AgendaID = AIFD.AgendaID AND AIFD.rn_active = 1

-- Fallback for Initiator
LEFT JOIN MKLatestFactionDetailsInKnesset FallbackFaction_init ON A.InitiatorPersonID = FallbackFaction_init.PersonID
    AND A.KnessetNum = FallbackFaction_init.KnessetNum AND FallbackFaction_init.rn = 1

ORDER BY A.KnessetNum DESC, A.AgendaID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column": "COALESCE(AIFD.ActiveFactionID, FallbackFaction_init.FactionID)",
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
# For applied filters display text
if "applied_filters_info_query" not in st.session_state: st.session_state.applied_filters_info_query = []


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
# ... (rest of chart builder session state from original file should be here if any)
# Add any missing chart builder session state initializations from your original file if they were there.
# Example:
# if "builder_x_axis" not in st.session_state: st.session_state.builder_x_axis = None
# if "builder_y_axis" not in st.session_state: st.session_state.builder_y_axis = None
# etc.

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
    if st.session_state.get("applied_filters_info_query"): # Check if this key exists and has content
        filters_applied_text = '; '.join(st.session_state.applied_filters_info_query)
        if filters_applied_text and filters_applied_text != "Knesset(s): All; Faction(s): All": # Avoid showing if only default "All"
             subheader_text += f" (Active Filters: *{filters_applied_text}*)"
    st.markdown(subheader_text)

    if not st.session_state.query_results_df.empty:
        st.dataframe(st.session_state.query_results_df, use_container_width=True, height=400)
        safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", st.session_state.executed_query_name)
        col_csv, col_excel = st.columns(2)
        with col_csv:
            st.download_button("⬇️ CSV", st.session_state.query_results_df.to_csv(index=False).encode("utf-8-sig"), f"{safe_name}_results.csv", "text/csv", key=f"csv_dl_{safe_name}")
        with col_excel:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                st.session_state.query_results_df.to_excel(writer, index=False, sheet_name="Results")
            st.download_button("⬇️ Excel", excel_buffer.getvalue(), f"{safe_name}_results.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"excel_dl_{safe_name}")
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
    f_filters_sidebar_names = st.session_state.get("ms_faction_filter", []) # These are display names
    
    # Construct filter display string
    filter_display_parts = []
    if k_filters_sidebar:
        filter_display_parts.append(f"Knesset(s): `{', '.join(map(str, k_filters_sidebar))}`")
    else:
        filter_display_parts.append("Knesset(s): `All`")
    
    if f_filters_sidebar_names:
        filter_display_parts.append(f"Faction(s): `{', '.join(f_filters_sidebar_names)}`")
    else:
        filter_display_parts.append("Faction(s): `All`")
        
    st.markdown(f"Active Sidebar Filters: {'; '.join(filter_display_parts)}")

    if not st.session_state.table_explorer_df.empty:
        st.dataframe(st.session_state.table_explorer_df, use_container_width=True, height=400)
        safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", st.session_state.executed_table_explorer_name)
        col_csv_ex, col_excel_ex = st.columns(2)
        with col_csv_ex:
            st.download_button("⬇️ CSV", st.session_state.table_explorer_df.to_csv(index=False).encode("utf-8-sig"), f"{safe_name}_data.csv", "text/csv", key=f"csv_dl_ex_{safe_name}")
        with col_excel_ex:
            excel_buffer_ex = io.BytesIO()
            with pd.ExcelWriter(excel_buffer_ex, engine="openpyxl") as writer:
                st.session_state.table_explorer_df.to_excel(writer, index=False, sheet_name="TableData")
            st.download_button("⬇️ Excel", excel_buffer_ex.getvalue(), f"{safe_name}_data.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"excel_dl_ex_{safe_name}")
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
        key="sb_selected_plot_topic_widget" # Unique key
    )

    if selected_topic_widget != st.session_state.get("selected_plot_topic"):
        st.session_state.selected_plot_topic = selected_topic_widget
        st.session_state.selected_plot_name_from_topic = "" # Reset sub-selection
        st.session_state.plot_main_knesset_selection = "" # Reset Knesset selection for plot
        st.session_state.plot_aggregation_level = "Yearly" # Reset plot specific options
        st.session_state.plot_show_average_line = False
        st.rerun()

    selected_plot_name_for_display = ""
    if st.session_state.get("selected_plot_topic"):
        charts_in_topic = AVAILABLE_PLOTS_BY_TOPIC[st.session_state.selected_plot_topic]
        chart_options_for_topic = [""] + list(charts_in_topic.keys())
        current_selected_chart_from_topic = st.session_state.get("selected_plot_name_from_topic", "")
        chart_select_default_index = chart_options_for_topic.index(current_selected_chart_from_topic) if current_selected_chart_from_topic in chart_options_for_topic else 0

        # Ensure key is unique if topic changes, by including topic in key
        selected_chart_widget = st.selectbox(
            f"2. Choose Visualization for '{st.session_state.selected_plot_topic}':",
            options=chart_options_for_topic,
            index=chart_select_default_index,
            key=f"sb_selected_chart_for_topic_{st.session_state.selected_plot_topic.replace(' ', '_')}"
        )

        if selected_chart_widget != st.session_state.get("selected_plot_name_from_topic"):
            st.session_state.selected_plot_name_from_topic = selected_chart_widget
            st.session_state.plot_aggregation_level = "Yearly" # Reset plot specific options on new chart selection
            st.session_state.plot_show_average_line = False
            st.rerun()
        selected_plot_name_for_display = st.session_state.selected_plot_name_from_topic

    final_knesset_filter_for_plot = None # Default to no filter / error state
    plot_knesset_options = [""] # Start with a blank option
    if knesset_nums_options_global: # knesset_nums_options_global should be list of strings
        plot_knesset_options.extend(sorted([str(k) for k in knesset_nums_options_global], key=int, reverse=True))


    can_show_all_knessets = selected_plot_name_for_display in ["Queries by Time Period", "Agenda Items by Time Period"]
    if can_show_all_knessets:
        if "All Knessets (Color Coded)" not in plot_knesset_options:
             plot_knesset_options.insert(1, "All Knessets (Color Coded)") # Insert after the blank


    if selected_plot_name_for_display: # Only show Knesset selector if a plot is chosen
        current_main_knesset_selection_in_state = str(st.session_state.get("plot_main_knesset_selection", ""))
        
        # Validate current selection against available options
        if current_main_knesset_selection_in_state not in plot_knesset_options:
            current_main_knesset_selection_in_state = "" # Reset if invalid
            st.session_state.plot_main_knesset_selection = ""


        knesset_select_default_index = plot_knesset_options.index(current_main_knesset_selection_in_state) \
            if current_main_knesset_selection_in_state in plot_knesset_options else 0

        aggregation_level_for_plot = st.session_state.get("plot_aggregation_level", "Yearly")
        show_average_line_for_plot = st.session_state.get("plot_show_average_line", False)
        
        selected_knesset_main_area_val = "" # To store the widget's current value

        # Time-period plots have more options
        if selected_plot_name_for_display in ["Queries by Time Period", "Agenda Items by Time Period"]:
            col_knesset_select, col_agg_select, col_avg_line = st.columns([2, 1, 1])
            with col_knesset_select:
                selected_knesset_main_area_val = st.selectbox(
                    "3. Select Knesset for Plot:",
                    options=plot_knesset_options, # These already include "All Knessets..." if applicable
                    index=knesset_select_default_index,
                    key=f"plot_main_knesset_selector_tp_{selected_plot_name_for_display.replace(' ', '_')}" # Unique key
                )
            with col_agg_select:
                # Ensure key is unique per plot for these options too
                st.session_state.plot_aggregation_level = st.selectbox(
                    "Aggregate:", options=["Yearly", "Monthly", "Quarterly"],
                    index=["Yearly", "Monthly", "Quarterly"].index(aggregation_level_for_plot), # find current index
                    key=f"agg_level_{selected_plot_name_for_display.replace(' ', '_')}"
                )
            with col_avg_line:
                st.session_state.plot_show_average_line = st.checkbox(
                    "Avg Line", value=show_average_line_for_plot,
                    key=f"avg_line_{selected_plot_name_for_display.replace(' ', '_')}"
                )
            # Update local vars from session state after widgets, as they might have changed it
            aggregation_level_for_plot = st.session_state.plot_aggregation_level
            show_average_line_for_plot = st.session_state.plot_show_average_line
        else: # For plots that require a single Knesset
            options_for_single_knesset_plot = [opt for opt in plot_knesset_options if opt != "All Knessets (Color Coded)" and opt != ""]
            # Ensure current selection is valid for single-Knesset plots
            if current_main_knesset_selection_in_state not in options_for_single_knesset_plot and current_main_knesset_selection_in_state != "":
                 current_main_knesset_selection_in_state = "" # Reset if "All Knessets" was selected for a single-Knesset plot
                 st.session_state.plot_main_knesset_selection = ""
            
            single_knesset_default_idx = options_for_single_knesset_plot.index(current_main_knesset_selection_in_state) \
                if current_main_knesset_selection_in_state in options_for_single_knesset_plot else 0

            # Add a blank option at the beginning for single Knesset plots if not already there via plot_knesset_options
            effective_options_single = [""] + options_for_single_knesset_plot
            if current_main_knesset_selection_in_state not in effective_options_single:
                current_main_knesset_selection_in_state = ""
            
            single_knesset_default_idx = effective_options_single.index(current_main_knesset_selection_in_state)


            selected_knesset_main_area_val = st.selectbox(
                "3. Select Knesset for Plot:",
                options=effective_options_single, # Use filtered options
                index=single_knesset_default_idx,
                key=f"plot_main_knesset_selector_single_{selected_plot_name_for_display.replace(' ', '_')}" # Unique key
            )

        # Handle change in Knesset selection
        if selected_knesset_main_area_val != st.session_state.get("plot_main_knesset_selection", ""):
            st.session_state.plot_main_knesset_selection = selected_knesset_main_area_val
            st.rerun() # Rerun to update plot or messages
        
        # Determine final_knesset_filter_for_plot based on validated selection
        current_selection_for_filter = st.session_state.get("plot_main_knesset_selection")
        if current_selection_for_filter == "All Knessets (Color Coded)" and can_show_all_knessets:
            final_knesset_filter_for_plot = None # None means all for these plots
            ui_logger.info(f"Plot '{selected_plot_name_for_display}': Showing all Knessets (color coded).")
        elif current_selection_for_filter and current_selection_for_filter != "": # A specific Knesset number string
            try:
                final_knesset_filter_for_plot = [int(current_selection_for_filter)] # Convert to list of int
                ui_logger.info(f"Plot '{selected_plot_name_for_display}': Using main area Knesset selection: {final_knesset_filter_for_plot}")
            except ValueError:
                st.error(f"Invalid Knesset number selected: {current_selection_for_filter}")
                final_knesset_filter_for_plot = False # Error state
        else: # Blank selection
            # Check if the plot *requires* a single Knesset selection
            requires_single_knesset = "(Single Knesset)" in selected_plot_name_for_display or not can_show_all_knessets
            if requires_single_knesset:
                 st.info(f"Please select a Knesset for the '{selected_plot_name_for_display}' plot.")
            # For plots that can show "All", a blank might mean "don't plot yet" or default to all if designed that way.
            # Here, False means don't attempt to plot.
            final_knesset_filter_for_plot = False


        # Attempt to generate plot if all conditions met
        can_generate_plot = selected_plot_name_for_display and (final_knesset_filter_for_plot is not False)

        if can_generate_plot:
            plot_function = AVAILABLE_PLOTS_BY_TOPIC[st.session_state.selected_plot_topic][selected_plot_name_for_display]
            # Prepare arguments for the plot function
            plot_args = {
                "db_path": DB_PATH,
                "connect_func": lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger),
                "logger_obj": ui_logger,
                "knesset_filter": final_knesset_filter_for_plot, # This is now a list of ints or None
                "faction_filter": [faction_display_map_global[name] for name in st.session_state.get("ms_faction_filter", []) if name in faction_display_map_global]
            }
            # Add specific args for time period plots
            if selected_plot_name_for_display in ["Queries by Time Period", "Agenda Items by Time Period"]:
                plot_args["aggregation_level"] = aggregation_level_for_plot
                plot_args["show_average_line"] = show_average_line_for_plot

            with st.spinner(f"Generating '{selected_plot_name_for_display}'..."):
                try:
                    figure = plot_function(**plot_args)
                    if figure:
                        st.plotly_chart(figure, use_container_width=True)
                        st.session_state.generated_plot_figure = figure # Save for potential later use
                except Exception as e:
                    ui_logger.error(f"Error displaying plot '{selected_plot_name_for_display}': {e}", exc_info=True)
                    st.error(f"An error occurred while generating the plot: {ui_utils.format_exception_for_ui(sys.exc_info())}")
                    # st.code(str(e) + "\n\n" + ui_utils.format_exception_for_ui(sys.exc_info())) # Redundant with above

    elif st.session_state.get("selected_plot_topic"): # Topic selected, but no specific chart
        st.info("Please choose a specific visualization from the dropdown above.")
    else: # No topic selected
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
                con = None # Initialize con to None
                try:
                    con = ui_utils.connect_db(DB_PATH, read_only=True, _logger_obj=ui_logger)
                    adhoc_result_df = ui_utils.safe_execute_query(con, sql_query_input, _logger_obj=ui_logger)
                    if con: con.close() # Close connection after use
                    st.dataframe(adhoc_result_df, use_container_width=True)
                    if not adhoc_result_df.empty:
                        st.download_button("⬇️ CSV", adhoc_result_df.to_csv(index=False).encode("utf-8-sig"), "adhoc_results.csv", "text/csv", key="adhoc_csv_dl")
                except Exception as e:
                    ui_logger.error(f"❌ Ad-hoc SQL Query Error: {e}", exc_info=True)
                    st.error(f"❌ SQL Query Error: {ui_utils.format_exception_for_ui(sys.exc_info())}")
                    # st.code(str(e) + "\n\n" + ui_utils.format_exception_for_ui(sys.exc_info())) # Redundant
                    if con: con.close() # Ensure connection is closed on error too
            else: st.warning("SQL query cannot be empty.")


st.divider()
with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
    if DB_PATH.exists():
        tables_to_check_status_main = sorted(list(set(TABLES))) # Ensure unique table names
        status_data_main = [{"Table": t_name, "Last Updated (Parquet Mod Time)": ui_utils.get_last_updated_for_table(PARQUET_DIR, t_name, ui_logger)} for t_name in tables_to_check_status_main]
        if status_data_main: st.dataframe(pd.DataFrame(status_data_main), hide_index=True, use_container_width=True)
        else: st.info("No tables found to display status, or TABLES list is empty.")
    else: st.info("Database not found. Table status cannot be displayed.")

ui_logger.info("--- data_refresh.py script finished loading UI components ---")

