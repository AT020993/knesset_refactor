from __future__ import annotations

# Standard Library Imports
import asyncio # Keep for main app if other async ops are used, though sidebar handles its own
import io
import logging
import re  # For safe filename generation and SQL injection
import sys
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from zoneinfo import ZoneInfo

# Third-Party Imports
import duckdb
import openpyxl  # Required by pandas for Excel writing to .xlsx, even if not directly used by code
import pandas as pd
import streamlit as st
import plotly.express as px  # Import Plotly Express

# Add the 'src' directory to sys.path to allow absolute imports
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
# import backend.fetch_table as ft # ft is used within sidebar_components now
import ui.plot_generators as pg  # Import the plot generators module
import ui.sidebar_components as sc # Import the new sidebar components module

# Initialize logger for the UI module
ui_logger = setup_logging("knesset.ui.data_refresh", console_output=True)

# Helper to format exceptions for UI display
_DEF_LOG_FORMATTER = logging.Formatter()

def _format_exc():
    return _DEF_LOG_FORMATTER.formatException(sys.exc_info())

DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/parquet")
MAX_ROWS_FOR_CHART_BUILDER = 50000 # Max rows to fetch for interactive chart builder
MAX_UNIQUE_VALUES_FOR_FACET = 50 # Max unique values for a column to be used for faceting

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="Knesset OData – Refresh & Export", layout="wide")

# --- Session State Initialization ---
# For Predefined Queries
if "selected_query_name" not in st.session_state: st.session_state.selected_query_name = None
if "executed_query_name" not in st.session_state: st.session_state.executed_query_name = None
if "executed_sql_string" not in st.session_state: st.session_state.executed_sql_string = ""
if "query_results_df" not in st.session_state: st.session_state.query_results_df = pd.DataFrame()
if "show_query_results" not in st.session_state: st.session_state.show_query_results = False
if "applied_knesset_filter_to_query" not in st.session_state: st.session_state.applied_knesset_filter_to_query = []

# For Interactive Table Explorer
if "selected_table_for_explorer" not in st.session_state: st.session_state.selected_table_for_explorer = None
if "executed_table_explorer_name" not in st.session_state: st.session_state.executed_table_explorer_name = None
if "table_explorer_df" not in st.session_state: st.session_state.table_explorer_df = pd.DataFrame()
if "show_table_explorer_results" not in st.session_state: st.session_state.show_table_explorer_results = False

# For Sidebar Filters
if "ms_knesset_filter" not in st.session_state: st.session_state.ms_knesset_filter = []
if "ms_faction_filter" not in st.session_state: st.session_state.ms_faction_filter = []

# For Predefined Data Visualizations
if "selected_plot_name" not in st.session_state: st.session_state.selected_plot_name = None
if "generated_plot_figure" not in st.session_state: st.session_state.generated_plot_figure = None
if "previous_predefined_plot_selection" not in st.session_state: st.session_state.previous_predefined_plot_selection = ""
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


# --- Database Connection and Utility Functions ---
@st.cache_resource(ttl=300)
def _connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Establishes a new connection to the DuckDB database."""
    if not DB_PATH.exists() and not read_only:
        st.info(f"Database {DB_PATH} does not exist. It will be created by DuckDB during write operation.")
    elif not DB_PATH.exists() and read_only:
        st.warning(f"Database {DB_PATH} does not exist. Please run a data refresh first. Query execution will fail.")
        return duckdb.connect(database=":memory:", read_only=True)
    try:
        con = duckdb.connect(database=DB_PATH.as_posix(), read_only=read_only)
        con.execute("SELECT 1")
        return con
    except Exception as e:
        ui_logger.error(f"Error connecting to database: {e}", exc_info=True)
        st.error(f"Database connection error: {e}")
        return duckdb.connect(database=":memory:", read_only=True)

def _safe_execute_query(con: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    """Safely execute a query with proper error handling."""
    try:
        return con.execute(query).df()
    except Exception as e:
        ui_logger.error(f"Query execution error: {e}", exc_info=True)
        st.error(f"Query execution error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_db_table_list():
    """Fetches the list of all tables from the database."""
    ui_logger.info("Fetching database table list...")
    if not DB_PATH.exists():
        ui_logger.warning("Database file not found. Returning empty table list.")
        return []
    try:
        con = _connect(read_only=True)
        tables_df = _safe_execute_query(con, "SHOW TABLES;")
        table_list = sorted(tables_df["name"].tolist()) if not tables_df.empty else []
        ui_logger.info(f"Database table list fetched: {len(table_list)} tables.")
        return table_list
    except Exception as e:
        ui_logger.error(f"Error in get_db_table_list: {e}", exc_info=True)
        st.sidebar.error(f"DB error listing tables: {e}", icon="🔥")
        return []

@st.cache_data(ttl=3600)
def get_table_columns(table_name: str) -> tuple[list[str], list[str], list[str]]:
    """Fetches all column names, numeric column names, and categorical column names for a table."""
    if not table_name or not DB_PATH.exists(): return [], [], []
    try:
        con = _connect(read_only=True)
        columns_df = _safe_execute_query(con, f"PRAGMA table_info('{table_name}');")
        if columns_df.empty: return [], [], []
        all_cols = columns_df["name"].tolist()
        numeric_cols = columns_df[columns_df["type"].str.contains("INTEGER|FLOAT|DOUBLE|DECIMAL|NUMERIC|BIGINT|SMALLINT|TINYINT|REAL|NUMBER", case=False, na=False)]["name"].tolist()
        categorical_cols = [col for col in all_cols if col not in numeric_cols]
        return all_cols, numeric_cols, categorical_cols
    except Exception as e:
        ui_logger.error(f"Error getting columns for table {table_name}: {e}", exc_info=True)
        return [], [], []

@st.cache_data(ttl=3600)
def get_filter_options_from_db():
    """Fetches distinct Knesset numbers and faction data for filter dropdowns."""
    ui_logger.info("Fetching filter options from database...")
    if not DB_PATH.exists():
        ui_logger.warning("Database file not found. Returning empty filter options.")
        return [], pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])
    try:
        con = _connect(read_only=True)
        knesset_nums_df = _safe_execute_query(con, "SELECT DISTINCT KnessetNum FROM KNS_KnessetDates ORDER BY KnessetNum DESC;")
        knesset_nums_options = sorted(knesset_nums_df["KnessetNum"].unique().tolist(), reverse=True) if not knesset_nums_df.empty else []

        db_tables_df = _safe_execute_query(con, "SELECT table_name FROM duckdb_tables() WHERE schema_name='main';")
        db_tables_list = db_tables_df["table_name"].str.lower().tolist()

        factions_query = ""
        if "userfactioncoalitionstatus" in db_tables_list and "kns_faction" in db_tables_list:
            factions_query = """
                SELECT DISTINCT COALESCE(ufcs.FactionName, kf.Name) AS FactionName, kf.FactionID, kf.KnessetNum
                FROM KNS_Faction AS kf
                LEFT JOIN UserFactionCoalitionStatus AS ufcs ON kf.FactionID = ufcs.FactionID AND kf.KnessetNum = ufcs.KnessetNum
                ORDER BY FactionName;
            """
        elif "kns_faction" in db_tables_list:
            ui_logger.info("UserFactionCoalitionStatus table not found, fetching faction names from KNS_Faction.")
            factions_query = "SELECT DISTINCT Name AS FactionName, FactionID, KnessetNum FROM KNS_Faction ORDER BY FactionName;"
        else:
            ui_logger.warning("KNS_Faction table not found. Cannot fetch faction filter options.")
            return knesset_nums_options, pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])

        factions_df = _safe_execute_query(con, factions_query)
        ui_logger.info(f"Filter options fetched: {len(knesset_nums_options)} Knesset Nums, {len(factions_df)} Factions.")
        return knesset_nums_options, factions_df
    except Exception as e:
        ui_logger.error(f"Error in get_filter_options_from_db: {e}", exc_info=True)
        return [], pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])

knesset_nums_options_global, factions_options_df_global = get_filter_options_from_db()
faction_display_map_global = {
    f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"]
    for _, row in factions_options_df_global.iterrows()
}


def _human_ts(ts_value):
    if ts_value is None or pd.isna(ts_value): return "N/A"
    try:
        if isinstance(ts_value, (int, float)): dt_obj = datetime.fromtimestamp(ts_value, ZoneInfo("UTC"))
        elif isinstance(ts_value, str):
            try: dt_obj = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
            except ValueError: dt_obj = pd.to_datetime(ts_value).to_pydatetime()
        elif isinstance(ts_value, datetime): dt_obj = ts_value
        elif isinstance(ts_value, pd.Timestamp): dt_obj = ts_value.to_pydatetime()
        else: return "Invalid date format"
        if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=ZoneInfo("UTC"))
        else: dt_obj = dt_obj.astimezone(ZoneInfo("UTC"))
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception as e:
        ui_logger.warning(f"Could not parse timestamp '{ts_value}': {e}")
        return str(ts_value)

def _get_last_updated_for_table(table_name: str) -> str:
    parquet_file = PARQUET_DIR / f"{table_name}.parquet"
    if parquet_file.exists():
        try: return _human_ts(parquet_file.stat().st_mtime)
        except Exception as e:
            ui_logger.warning(f"Could not get mod_time for {parquet_file}: {e}")
            return "Error reading timestamp"
    return "Never (or N/A)"

EXPORTS = {
    "Queries + Full Details": {
        "sql": """
            SELECT Q.QueryID, Q.Number, Q.KnessetNum, Q.Name AS QueryName, Q.TypeID AS QueryTypeID, Q.TypeDesc AS QueryTypeDesc,
                   S.Desc AS QueryStatusDesc, P.FirstName AS MKFirstName, P.LastName AS MKLastName, 
                   P2P.FactionName AS MKFactionName, ufs.CoalitionStatus AS MKFactionCoalitionStatus, M.Name AS MinistryName,
                   strftime(CAST(Q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDateFormatted
            FROM KNS_Query Q
            LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
            LEFT JOIN KNS_PersonToPosition P2P ON Q.PersonID = P2P.PersonID AND Q.KnessetNum = P2P.KnessetNum AND CAST(Q.SubmitDate AS TIMESTAMP) BETWEEN CAST(P2P.StartDate AS TIMESTAMP) AND CAST(COALESCE(P2P.FinishDate, '9999-12-31') AS TIMESTAMP)
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
            SELECT A.AgendaID, A.Number AS AgendaNumber, A.KnessetNum, A.Name AS AgendaName, A.ClassificationDesc AS AgendaClassification,
                   S.Desc AS AgendaStatus, INIT_P.FirstName AS InitiatorFirstName, INIT_P.LastName AS InitiatorLastName,
                   INIT_P2P.FactionName AS InitiatorFactionName, INIT_UFS.CoalitionStatus AS InitiatorFactionCoalitionStatus,
                   HC.Name AS HandlingCommitteeName, strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted
            FROM KNS_Agenda A
            LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
            LEFT JOIN KNS_Person INIT_P ON A.InitiatorPersonID = INIT_P.PersonID
            LEFT JOIN KNS_PersonToPosition INIT_P2P ON A.InitiatorPersonID = INIT_P2P.PersonID AND A.KnessetNum = INIT_P2P.KnessetNum AND CAST(COALESCE(A.PresidentDecisionDate, A.LastUpdatedDate) AS TIMESTAMP) BETWEEN CAST(INIT_P2P.StartDate AS TIMESTAMP) AND CAST(COALESCE(INIT_P2P.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN UserFactionCoalitionStatus INIT_UFS ON INIT_P2P.FactionID = INIT_UFS.FactionID AND INIT_P2P.KnessetNum = INIT_UFS.KnessetNum
            LEFT JOIN KNS_Committee HC ON A.CommitteeID = HC.CommitteeID
            ORDER BY A.KnessetNum DESC, A.AgendaID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column": "INIT_P2P.FactionID",
    },
}

AVAILABLE_PLOTS = {
    "Number of Queries per Year": pg.plot_queries_by_year,
    "Distribution of Query Types": pg.plot_query_types_distribution,
    "Queries by Faction (Coalition/Opposition)": pg.plot_queries_by_faction_status, 
    "Number of Agenda Items per Year": pg.plot_agendas_by_year,
    "Distribution of Agenda Classifications": pg.plot_agenda_classifications_pie,
    "Agenda Item Status Distribution": pg.plot_agenda_status_distribution, 
    # "Number of Factions per Knesset": pg.plot_factions_per_knesset, # This line is removed
}

sc.display_sidebar(
    db_path_arg=DB_PATH,
    exports_arg=EXPORTS,
    connect_func_arg=_connect,
    get_db_table_list_func_arg=get_db_table_list,
    get_table_columns_func_arg=get_table_columns,
    get_filter_options_func_arg=get_filter_options_from_db,
    faction_display_map_arg=faction_display_map_global,
    ui_logger_arg=ui_logger,
    format_exc_func_arg=_format_exc
)

st.title("🇮🇱 Knesset Data Warehouse Console")

with st.expander("ℹ️ How This Works", expanded=False):
    st.markdown(dedent(f"""
        * **Data Refresh:** Use sidebar controls to fetch OData tables or update faction statuses.
        * **Predefined Queries:** Select a query, apply filters, click "Run". Results appear below.
        * **Interactive Table Explorer:** Select a table, apply filters, click "Explore". Results appear below.
        * **Predefined Visualizations:** Select a plot. If your sidebar Knesset filter isn't for a single Knesset, a dropdown will appear to let you focus the plot on one Knesset.
        * **Interactive Chart Builder:** Dynamically create your own charts. Data for charts is filtered by sidebar selections and limited to {MAX_ROWS_FOR_CHART_BUILDER} rows. Faceting is limited to columns with fewer than {MAX_UNIQUE_VALUES_FOR_FACET} unique values.
        * **Ad-hoc SQL:** Use the sandbox at the bottom to run custom SQL.
    """))

st.divider()
st.header("📄 Predefined Query Results")
if st.session_state.show_query_results and st.session_state.executed_query_name:
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
if st.session_state.show_table_explorer_results and st.session_state.executed_table_explorer_name:
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

st.divider()
st.header("📈 Predefined Visualizations")
if not DB_PATH.exists():
    st.warning("Database not found. Visualizations cannot be generated. Please run a data refresh.")
else:
    plot_options = [""] + list(AVAILABLE_PLOTS.keys())
    current_selected_plot_val = st.session_state.get("selected_plot_name", "")
    plot_select_default_index = 0
    if current_selected_plot_val in plot_options:
        try:
            plot_select_default_index = plot_options.index(current_selected_plot_val)
        except ValueError:
            plot_select_default_index = 0

    selected_plot_name_widget = st.selectbox( 
        "Choose a predefined visualization:",
        options=plot_options,
        index=plot_select_default_index,
        key="sb_selected_plot_main_widget", 
    )

    if selected_plot_name_widget != st.session_state.get("selected_plot_name"):
        st.session_state.selected_plot_name = selected_plot_name_widget
        st.session_state.plot_specific_knesset_selection = "" 
        st.rerun() 

    final_knesset_filter_for_plot = None 
    
    if st.session_state.selected_plot_name and st.session_state.selected_plot_name != "": 
        knesset_filter_from_sidebar = st.session_state.get("ms_knesset_filter", [])

        if len(knesset_filter_from_sidebar) == 1:
            final_knesset_filter_for_plot = knesset_filter_from_sidebar
            ui_logger.info(f"Predefined plot '{st.session_state.selected_plot_name}': Using single Knesset from sidebar: {final_knesset_filter_for_plot}")
        else:
            plot_specific_knesset_options = [""] + knesset_nums_options_global 
            current_plot_specific_knesset = st.session_state.get("plot_specific_knesset_selection", "")
            
            selected_knesset_widget_val = st.selectbox(
                f"Focus '{st.session_state.selected_plot_name}' on a single Knesset:",
                options=plot_specific_knesset_options,
                index=plot_specific_knesset_options.index(current_plot_specific_knesset) if current_plot_specific_knesset in plot_specific_knesset_options else 0,
                key=f"plot_specific_knesset_selector_for_{st.session_state.selected_plot_name.replace(' ', '_')}" 
            )

            if selected_knesset_widget_val != current_plot_specific_knesset:
                 st.session_state.plot_specific_knesset_selection = selected_knesset_widget_val
                 st.rerun() 

            if st.session_state.plot_specific_knesset_selection and st.session_state.plot_specific_knesset_selection != "":
                try:
                    final_knesset_filter_for_plot = [int(st.session_state.plot_specific_knesset_selection)]
                    ui_logger.info(f"Predefined plot '{st.session_state.selected_plot_name}': Using plot-specific Knesset: {final_knesset_filter_for_plot}")
                except ValueError:
                    ui_logger.error(f"Could not convert plot-specific Knesset selection '{st.session_state.plot_specific_knesset_selection}' to int.")
                    st.error("Invalid Knesset number selected for plot focus.")
                    final_knesset_filter_for_plot = None
            else:
                st.info(f"To view the '{st.session_state.selected_plot_name}' plot, please select a specific Knesset above or choose a single Knesset in the sidebar filters.")
                final_knesset_filter_for_plot = None 

        if final_knesset_filter_for_plot: 
            plot_function = AVAILABLE_PLOTS[st.session_state.selected_plot_name]
            with st.spinner(f"Generating '{st.session_state.selected_plot_name}' for Knesset(s) {final_knesset_filter_for_plot}..."):
                try:
                    figure = plot_function(
                        DB_PATH, 
                        _connect, 
                        ui_logger,
                        knesset_filter=final_knesset_filter_for_plot, 
                        faction_filter=[faction_display_map_global[name] for name in st.session_state.ms_faction_filter if name in faction_display_map_global]
                    )
                    if figure:
                        st.plotly_chart(figure, use_container_width=True)
                        st.session_state.generated_plot_figure = figure
                except Exception as e:
                    ui_logger.error(f"Error displaying plot '{st.session_state.selected_plot_name}': {e}", exc_info=True)
                    st.error(f"An error occurred while generating the plot: {e}")
                    st.code(str(e) + "\n\n" + _format_exc())
    else: 
        st.info("Select a predefined visualization from the dropdown to display it.")


st.divider()
st.header("🛠️ Interactive Chart Builder")
if not DB_PATH.exists():
    st.warning("Database not found. Chart Builder requires data. Please run a data refresh.")
else:
    db_tables_for_builder = [""] + get_db_table_list()
    
    current_builder_selected_table_value = st.session_state.get('builder_selected_table') 
    table_select_default_index = 0
    if current_builder_selected_table_value and current_builder_selected_table_value in db_tables_for_builder:
        try:
            table_select_default_index = db_tables_for_builder.index(current_builder_selected_table_value)
        except ValueError:
            table_select_default_index = 0 
    
    selectbox_output_table = st.selectbox(
        "1. Select Table to Visualize:",
        options=db_tables_for_builder,
        index=table_select_default_index, 
        key="builder_table_select_widget", 
    )

    if selectbox_output_table != st.session_state.get("builder_selected_table_previous_run"):
        ui_logger.info(f"Chart Builder: User selection for table changed from '{st.session_state.get('builder_selected_table_previous_run')}' to '{selectbox_output_table}'.")
        
        if selectbox_output_table and selectbox_output_table != "": 
            st.session_state.builder_selected_table = selectbox_output_table 
            ui_logger.info(f"Chart Builder: Valid table '{selectbox_output_table}' set. Updating columns and resetting axes.")
            all_cols, numeric_cols, categorical_cols = get_table_columns(selectbox_output_table)
            st.session_state.builder_columns = [""] + all_cols
            st.session_state.builder_numeric_columns = [""] + numeric_cols
            st.session_state.builder_categorical_columns = [""] + categorical_cols
        else: 
            ui_logger.info("Chart Builder: Placeholder selected for table. Resetting active table and dependent state.")
            st.session_state.builder_selected_table = None 
            st.session_state.builder_columns = [""]
            st.session_state.builder_numeric_columns = [""]
            st.session_state.builder_categorical_columns = [""]

        st.session_state.builder_x_axis = None
        st.session_state.builder_y_axis = None
        st.session_state.builder_color = None
        st.session_state.builder_size = None
        st.session_state.builder_facet_row = None
        st.session_state.builder_facet_col = None
        st.session_state.builder_hover_name = None
        st.session_state.builder_names = None
        st.session_state.builder_values = None
        st.session_state.builder_generated_chart = None
        
        st.session_state.builder_selected_table_previous_run = selectbox_output_table 
        st.rerun()

    if st.session_state.get("builder_selected_table"): 
        ui_logger.info(f"Chart Builder: Rendering options for table: {st.session_state.builder_selected_table}")
        st.write(f"Selected Table: **{st.session_state.builder_selected_table}**")
        
        chart_types = ["bar", "line", "scatter", "pie", "histogram", "box"]
        current_chart_type = st.session_state.get("builder_chart_type", "bar")
        st.session_state.builder_chart_type = st.selectbox(
            "2. Select Chart Type:",
            options=chart_types,
            index=chart_types.index(current_chart_type) if current_chart_type in chart_types else 0,
            key="builder_chart_type_selector"
        )

        if st.session_state.previous_builder_chart_type != st.session_state.builder_chart_type:
            ui_logger.info(f"Chart type changed from {st.session_state.previous_builder_chart_type} to {st.session_state.builder_chart_type}. Validating axes.")
            new_chart_type = st.session_state.builder_chart_type
            rerun_needed_for_chart_type_change = False 
            y_axis_current = st.session_state.get("builder_y_axis")
            y_options_numeric = st.session_state.get("builder_numeric_columns", [])
            
            if new_chart_type in ["line", "scatter"]: 
                if y_axis_current and y_axis_current not in y_options_numeric:
                    ui_logger.info(f"Resetting Y-axis ('{y_axis_current}') as it's not numeric and new chart type '{new_chart_type}' requires numeric Y.")
                    st.session_state.builder_y_axis = None
                    rerun_needed_for_chart_type_change = True
            
            if new_chart_type != "pie" and st.session_state.previous_builder_chart_type == "pie":
                st.session_state.builder_names = None
                st.session_state.builder_values = None
                rerun_needed_for_chart_type_change = True 

            st.session_state.previous_builder_chart_type = new_chart_type
            st.session_state.builder_generated_chart = None 

            if rerun_needed_for_chart_type_change:
                st.rerun()

        cols_c1, cols_c2 = st.columns(2)
        with cols_c1:
            def get_safe_index(options_list, current_value_key):
                val = st.session_state.get(current_value_key)
                try:
                    return options_list.index(val) if val and val in options_list else 0
                except ValueError: return 0

            x_axis_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_x_axis = st.selectbox("X-axis:", options=x_axis_options, index=get_safe_index(x_axis_options, "builder_x_axis"), key="cb_x_axis")
            
            if st.session_state.builder_chart_type not in ["pie", "histogram",]:
                y_axis_options_all = st.session_state.get("builder_columns", [""])
                y_axis_options_numeric = st.session_state.get("builder_numeric_columns", [""])
                current_y_options = y_axis_options_numeric if st.session_state.builder_chart_type not in ["bar", "box"] else y_axis_options_all
                st.session_state.builder_y_axis = st.selectbox("Y-axis:", options=current_y_options, index=get_safe_index(current_y_options, "builder_y_axis"), help="Select a numeric column for Y-axis (Bar and Box plots can also use categorical).", key="cb_y_axis")
            
            if st.session_state.builder_chart_type == "pie":
                pie_names_options = st.session_state.get("builder_categorical_columns", [""])
                st.session_state.builder_names = st.selectbox("Names (for Pie chart slices):", options=pie_names_options, index=get_safe_index(pie_names_options, "builder_names"), key="cb_pie_names")
                
                pie_values_options = st.session_state.get("builder_numeric_columns", [""])
                st.session_state.builder_values = st.selectbox("Values (for Pie chart sizes):", options=pie_values_options, index=get_safe_index(pie_values_options, "builder_values"), key="cb_pie_values")
            
            color_by_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_color = st.selectbox("Color by:", options=color_by_options, index=get_safe_index(color_by_options, "builder_color"), key="cb_color")
            
            if st.session_state.builder_chart_type in ["scatter"]:
                size_by_options = st.session_state.get("builder_numeric_columns", [""])
                st.session_state.builder_size = st.selectbox("Size by (for scatter):", options=size_by_options, index=get_safe_index(size_by_options, "builder_size"), key="cb_size")
        
        with cols_c2:
            facet_row_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_facet_row = st.selectbox("Facet Row by:", options=facet_row_options, index=get_safe_index(facet_row_options, "builder_facet_row"), key="cb_facet_row")

            facet_col_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_facet_col = st.selectbox("Facet Column by:", options=facet_col_options, index=get_safe_index(facet_col_options, "builder_facet_col"), key="cb_facet_col")
            
            hover_name_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_hover_name = st.selectbox("Hover Name:", options=hover_name_options, index=get_safe_index(hover_name_options, "builder_hover_name"), key="cb_hover_name")
            
            if st.session_state.builder_chart_type not in ["pie"]:
                st.session_state.builder_log_x = st.checkbox("Logarithmic X-axis", value=st.session_state.get("builder_log_x", False), key="cb_log_x")
                if st.session_state.builder_chart_type not in ["histogram"]:
                    st.session_state.builder_log_y = st.checkbox("Logarithmic Y-axis", value=st.session_state.get("builder_log_y", False), key="cb_log_y")
            
            if st.session_state.builder_chart_type == "bar":
                barmode_options = ["relative", "group", "overlay", "stack"]
                current_barmode = st.session_state.get("builder_barmode", "relative")
                st.session_state.builder_barmode = st.selectbox("Bar Mode:", options=barmode_options, index=barmode_options.index(current_barmode) if current_barmode in barmode_options else 0, key="cb_barmode")

        if st.button("📊 Generate Chart", key="btn_generate_custom_chart"):
            ui_logger.info("--- 'Generate Chart' BUTTON CLICKED ---")
            
            selected_x = st.session_state.get('builder_x_axis') if st.session_state.get('builder_x_axis', "") != "" else None
            selected_y = st.session_state.get('builder_y_axis') if st.session_state.get('builder_y_axis', "") != "" else None
            selected_names = st.session_state.get('builder_names') if st.session_state.get('builder_names', "") != "" else None
            selected_values = st.session_state.get('builder_values') if st.session_state.get('builder_values', "") != "" else None
            selected_color = st.session_state.get('builder_color') if st.session_state.get('builder_color', "") != "" else None
            selected_size = st.session_state.get('builder_size') if st.session_state.get('builder_size', "") != "" else None
            selected_facet_row = st.session_state.get('builder_facet_row') if st.session_state.get('builder_facet_row', "") != "" else None
            selected_facet_col = st.session_state.get('builder_facet_col') if st.session_state.get('builder_facet_col', "") != "" else None
            selected_hover_name = st.session_state.get('builder_hover_name') if st.session_state.get('builder_hover_name', "") != "" else None
            
            ui_logger.debug(f"Chart Builder Selections: X='{selected_x}', Y='{selected_y}', Names='{selected_names}', Values='{selected_values}', Color='{selected_color}', ChartType='{st.session_state.builder_chart_type}'")
            ui_logger.debug(f"Facet Row: '{selected_facet_row}', Facet Col: '{selected_facet_col}'")

            valid_input = True
            active_table_for_chart = st.session_state.get("builder_selected_table") 
            if not active_table_for_chart: 
                st.error("Error: No table selected for chart generation."); valid_input = False
            elif st.session_state.builder_chart_type not in ["pie", "histogram", "box"] and (not selected_x or not selected_y):
                st.error("Please select valid X-axis and Y-axis columns for this chart type."); valid_input = False
            elif st.session_state.builder_chart_type in ["histogram", "box"] and not selected_x : 
                st.error(f"Please select a valid X-axis for the {st.session_state.builder_chart_type} chart."); valid_input = False
            elif st.session_state.builder_chart_type == "pie" and (not selected_names or not selected_values): 
                st.error("Please select valid 'Names' and 'Values' columns for the Pie chart."); valid_input = False

            if valid_input:
                ui_logger.info(f"Input validated for table '{active_table_for_chart}'. Proceeding to fetch data and generate chart.")
                try:
                    con = _connect(read_only=True)
                    base_query = f'SELECT * FROM "{active_table_for_chart}"' 
                    where_clauses = []
                    table_all_columns_for_filter_check = st.session_state.get("builder_columns", []) 
                    actual_knesset_col = next((col for col in table_all_columns_for_filter_check if col.lower() == "knessetnum"), None)
                    actual_faction_col = next((col for col in table_all_columns_for_filter_check if col.lower() == "factionid"), None)

                    if actual_knesset_col and st.session_state.ms_knesset_filter:
                        where_clauses.append(f'"{actual_knesset_col}" IN ({", ".join(map(str, st.session_state.ms_knesset_filter))})')
                    if actual_faction_col and st.session_state.ms_faction_filter:
                        selected_faction_ids_builder = [faction_display_map_global[name] for name in st.session_state.ms_faction_filter if name in faction_display_map_global]
                        if selected_faction_ids_builder: where_clauses.append(f'"{actual_faction_col}" IN ({", ".join(map(str, selected_faction_ids_builder))})')
                    
                    final_query = base_query
                    if where_clauses: final_query += " WHERE " + " AND ".join(where_clauses)
                    final_query += f" LIMIT {MAX_ROWS_FOR_CHART_BUILDER}"
                    
                    ui_logger.info(f"Executing chart builder query: {final_query}")
                    df_full = con.sql(final_query).df()
                    ui_logger.info(f"Data fetched for chart. df_full is empty: {df_full.empty}. Rows: {len(df_full)}")

                    if df_full.empty:
                        st.warning("No data in the selected table after applying filters and selection. Cannot generate chart.")
                        st.session_state.builder_generated_chart = None
                    else:
                        if len(df_full) >= MAX_ROWS_FOR_CHART_BUILDER:
                            st.warning(f"Chart data is limited to {MAX_ROWS_FOR_CHART_BUILDER} rows. Apply more specific filters for a complete dataset if needed.")
                        
                        chart_params = {"data_frame": df_full, 
                                        "title": f"{st.session_state.builder_chart_type.capitalize()} of {active_table_for_chart}"}
                        
                        if selected_x: chart_params["x"] = selected_x
                        if selected_y and st.session_state.builder_chart_type not in ["histogram"]: chart_params["y"] = selected_y
                        if st.session_state.builder_chart_type == "pie":
                            if selected_names: chart_params["names"] = selected_names
                            if selected_values: chart_params["values"] = selected_values
                        if selected_color: chart_params["color"] = selected_color
                        if selected_size and st.session_state.builder_chart_type == "scatter": chart_params["size"] = selected_size
                        
                        facet_issue = False
                        if selected_facet_row:
                            if selected_facet_row in df_full.columns:
                                unique_facet_rows = df_full[selected_facet_row].nunique()
                                if unique_facet_rows > MAX_UNIQUE_VALUES_FOR_FACET:
                                    st.error(f"Cannot use '{selected_facet_row}' for Facet Row: Too many unique values ({unique_facet_rows}). Max allowed: {MAX_UNIQUE_VALUES_FOR_FACET}.")
                                    facet_issue = True
                                else:
                                    chart_params["facet_row"] = selected_facet_row
                            else:
                                ui_logger.warning(f"Selected facet_row column '{selected_facet_row}' not found in fetched DataFrame for chart.")
                        
                        if selected_facet_col and not facet_issue: 
                            if selected_facet_col in df_full.columns:
                                unique_facet_cols = df_full[selected_facet_col].nunique()
                                if unique_facet_cols > MAX_UNIQUE_VALUES_FOR_FACET:
                                    st.error(f"Cannot use '{selected_facet_col}' for Facet Column: Too many unique values ({unique_facet_cols}). Max allowed: {MAX_UNIQUE_VALUES_FOR_FACET}.")
                                    facet_issue = True
                                else:
                                    chart_params["facet_col"] = selected_facet_col
                            else:
                                ui_logger.warning(f"Selected facet_col column '{selected_facet_col}' not found in fetched DataFrame for chart.")

                        if facet_issue:
                            st.session_state.builder_generated_chart = None 
                            ui_logger.warning("Chart generation halted due to facet cardinality issue.")
                        else: 
                            if selected_hover_name: chart_params["hover_name"] = selected_hover_name
                            if st.session_state.builder_chart_type not in ["pie"]: chart_params["log_x"] = st.session_state.get("builder_log_x", False)
                            if st.session_state.builder_chart_type not in ["pie", "histogram"]: chart_params["log_y"] = st.session_state.get("builder_log_y", False)
                            if st.session_state.builder_chart_type == "bar" and st.session_state.get("builder_barmode"): chart_params["barmode"] = st.session_state.builder_barmode
                            
                            ui_logger.info(f"Attempting to generate {st.session_state.builder_chart_type} chart with params: {chart_params}")
                            
                            essential_missing = False
                            current_chart_type = st.session_state.builder_chart_type
                            if current_chart_type == "pie" and (not chart_params.get("names") or not chart_params.get("values")):
                                st.error("For Pie chart, 'Names' and 'Values' must be selected with valid columns."); essential_missing = True
                            elif current_chart_type in ["histogram", "box"] and not chart_params.get("x"):
                                st.error(f"For {current_chart_type} chart, 'X-axis' must be selected with a valid column."); essential_missing = True
                            elif current_chart_type not in ["pie", "histogram", "box"] and (not chart_params.get("x") or not chart_params.get("y")):
                                st.error(f"For {current_chart_type} chart, 'X-axis' and 'Y-axis' must be selected with valid columns."); essential_missing = True

                            if essential_missing:
                                ui_logger.warning("Essential parameters missing for chart generation right before Plotly call.")
                                st.session_state.builder_generated_chart = None
                            else:
                                fig_builder = getattr(px, current_chart_type)(**chart_params)
                                st.session_state.builder_generated_chart = fig_builder
                                st.toast(f"Chart '{chart_params['title']}' generated!", icon="🎉")
                except Exception as e:
                    ui_logger.error(f"Error generating custom chart: {e}", exc_info=True)
                    st.error(f"Could not generate chart: {e}")
                    st.code(f"Query attempt: {final_query if 'final_query' in locals() else 'N/A'}\n\nError: {str(e)}\n\nTraceback:\n{_format_exc()}")
                    st.session_state.builder_generated_chart = None
            else:
                 ui_logger.warning("Input validation failed for chart generation (before data fetch).")
        
        if st.session_state.builder_generated_chart:
            st.plotly_chart(st.session_state.builder_generated_chart, use_container_width=True)
    else: 
        ui_logger.debug("Chart Builder: No valid table selected in st.session_state.builder_selected_table, so chart options are not rendered.")


# --- Ad-hoc SQL Query Section ---
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
                    con = _connect(read_only=True)
                    adhoc_result_df = con.sql(sql_query_input).df()
                    st.dataframe(adhoc_result_df, use_container_width=True)
                    if not adhoc_result_df.empty:
                        st.download_button("⬇️ CSV", adhoc_result_df.to_csv(index=False).encode("utf-8-sig"), "adhoc_results.csv", "text/csv")
                except Exception as e:
                    ui_logger.error(f"❌ Ad-hoc SQL Query Error: {e}", exc_info=True)
                    st.error(f"❌ SQL Query Error: {e}")
                    st.code(str(e) + "\n\n" + _format_exc())
            else: st.warning("SQL query cannot be empty.")

# --- Table Update Status ---
st.divider()
with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
    if DB_PATH.exists():
        tables_to_check_status_main = sorted(list(set(TABLES))) 
        status_data_main = [{"Table": t_name, "Last Updated (Parquet Mod Time)": _get_last_updated_for_table(t_name)} for t_name in tables_to_check_status_main]
        if status_data_main: st.dataframe(pd.DataFrame(status_data_main), hide_index=True, use_container_width=True)
        else: st.info("No tables found to display status, or TABLES list is empty.")
    else: st.info("Database not found. Table status cannot be displayed.")

