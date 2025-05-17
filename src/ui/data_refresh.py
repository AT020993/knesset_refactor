from __future__ import annotations

# Standard Library Imports
import asyncio
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
import backend.fetch_table as ft  # type: ignore
import ui.plot_generators as pg  # Import the plot generators module

# Initialize logger for the UI module
ui_logger = setup_logging("knesset.ui.data_refresh", console_output=True)

_ALL_TABLE_NAMES_FROM_METADATA = TABLES
_SELECT_ALL_TABLES_OPTION = "🔄 Select/Deselect All Tables"

# Helper to format exceptions for UI display
_DEF_LOG_FORMATTER = logging.Formatter()


def _format_exc():
    return _DEF_LOG_FORMATTER.formatException(sys.exc_info())


DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/parquet")

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


st.set_page_config(page_title="Knesset OData – Refresh & Export", layout="wide")

# --- Session State Initialization ---
# For Predefined Queries
if "selected_query_name" not in st.session_state:
    st.session_state.selected_query_name = None
if "executed_query_name" not in st.session_state:
    st.session_state.executed_query_name = None
if "executed_sql_string" not in st.session_state:
    st.session_state.executed_sql_string = ""
if "query_results_df" not in st.session_state:
    st.session_state.query_results_df = pd.DataFrame()
if "show_query_results" not in st.session_state:
    st.session_state.show_query_results = False
if "applied_knesset_filter_to_query" not in st.session_state:
    st.session_state.applied_knesset_filter_to_query = []

# For Interactive Table Explorer
if "selected_table_for_explorer" not in st.session_state:
    st.session_state.selected_table_for_explorer = None
if "executed_table_explorer_name" not in st.session_state:
    st.session_state.executed_table_explorer_name = None
if "table_explorer_df" not in st.session_state:
    st.session_state.table_explorer_df = pd.DataFrame()
if "show_table_explorer_results" not in st.session_state:
    st.session_state.show_table_explorer_results = False

# For Sidebar Filters
if "ms_knesset_filter" not in st.session_state:
    st.session_state.ms_knesset_filter = []
if "ms_faction_filter" not in st.session_state:
    st.session_state.ms_faction_filter = []

# For Predefined Data Visualizations
if "selected_plot_name" not in st.session_state:
    st.session_state.selected_plot_name = None
if "generated_plot_figure" not in st.session_state:
    st.session_state.generated_plot_figure = None

# For Interactive Chart Builder
if "builder_selected_table" not in st.session_state:
    st.session_state.builder_selected_table = None
if "builder_chart_type" not in st.session_state:
    st.session_state.builder_chart_type = "bar"  # Default chart type
if "builder_columns" not in st.session_state:  # To store columns of selected table
    st.session_state.builder_columns = []
if "builder_numeric_columns" not in st.session_state:
    st.session_state.builder_numeric_columns = []
if "builder_categorical_columns" not in st.session_state:
    st.session_state.builder_categorical_columns = []
# Chart specific parameters
if "builder_x_axis" not in st.session_state:
    st.session_state.builder_x_axis = None
if "builder_y_axis" not in st.session_state:
    st.session_state.builder_y_axis = None
if "builder_color" not in st.session_state:
    st.session_state.builder_color = None
if "builder_size" not in st.session_state:
    st.session_state.builder_size = None
if "builder_facet_row" not in st.session_state:
    st.session_state.builder_facet_row = None
if "builder_facet_col" not in st.session_state:
    st.session_state.builder_facet_col = None
if "builder_hover_name" not in st.session_state:
    st.session_state.builder_hover_name = None
if "builder_names" not in st.session_state:
    st.session_state.builder_names = None  # For pie charts
if "builder_values" not in st.session_state:
    st.session_state.builder_values = None  # For pie charts
if "builder_log_x" not in st.session_state:
    st.session_state.builder_log_x = False
if "builder_log_y" not in st.session_state:
    st.session_state.builder_log_y = False
if "builder_barmode" not in st.session_state:
    st.session_state.builder_barmode = "relative"  # For bar charts
if "builder_generated_chart" not in st.session_state:
    st.session_state.builder_generated_chart = None


# --- Database Connection and Utility Functions ---
@st.cache_resource(ttl=300)  # Cache for 5 minutes
def _connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Establishes a new connection to the DuckDB database."""
    if not DB_PATH.exists() and not read_only:
        st.info(
            f"Database {DB_PATH} does not exist. It will be created by DuckDB during write operation."
        )
    elif not DB_PATH.exists() and read_only:
        st.warning(
            f"Database {DB_PATH} does not exist. Please run a data refresh first. Query execution will fail."
        )
        return duckdb.connect(database=":memory:", read_only=True)

    try:
        con = duckdb.connect(database=DB_PATH.as_posix(), read_only=read_only)
        # Test the connection
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
    if not table_name or not DB_PATH.exists():
        return [], [], []
    try:
        con = _connect(read_only=True)
        columns_df = _safe_execute_query(con, f"PRAGMA table_info('{table_name}');")
        if columns_df.empty:
            return [], [], []

        all_cols = columns_df["name"].tolist()
        numeric_cols = columns_df[
            columns_df["type"].str.contains(
                "INTEGER|FLOAT|DOUBLE|DECIMAL|NUMERIC|BIGINT|SMALLINT|TINYINT",
                case=False,
                na=False,
            )
        ]["name"].tolist()
        categorical_cols = [col for col in all_cols if col not in numeric_cols]

        return all_cols, numeric_cols, categorical_cols
    except Exception as e:
        ui_logger.error(
            f"Error getting columns for table {table_name}: {e}", exc_info=True
        )
        return [], [], []


def get_filter_options_from_db():
    """Fetches distinct Knesset numbers and faction data for filter dropdowns."""
    ui_logger.info("Fetching filter options from database...")
    if not DB_PATH.exists():
        ui_logger.warning("Database file not found. Returning empty filter options.")
        st.sidebar.warning("DB not found. Filters unavailable.", icon="⚠️")
        return [], pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])

    try:
        con = _connect(read_only=True)
        knesset_nums_df = _safe_execute_query(
            con,
            "SELECT DISTINCT KnessetNum FROM KNS_KnessetDates ORDER BY KnessetNum DESC;",
        )
        knesset_nums_options = (
            sorted(knesset_nums_df["KnessetNum"].unique().tolist(), reverse=True)
            if not knesset_nums_df.empty
            else []
        )

        db_tables_df = _safe_execute_query(
            con, "SELECT table_name FROM duckdb_tables() WHERE schema_name='main';"
        )
        db_tables_list = db_tables_df["table_name"].str.lower().tolist()

        if (
            "userfactioncoalitionstatus" in db_tables_list
            and "kns_faction" in db_tables_list
        ):
            factions_query = """
                SELECT DISTINCT COALESCE(ufcs.FactionName, kf.Name) AS FactionName, kf.FactionID, kf.KnessetNum
                FROM KNS_Faction AS kf
                LEFT JOIN UserFactionCoalitionStatus AS ufcs ON kf.FactionID = ufcs.FactionID AND kf.KnessetNum = ufcs.KnessetNum
                ORDER BY FactionName;
            """
        elif "kns_faction" in db_tables_list:
            ui_logger.info(
                "UserFactionCoalitionStatus table not found, fetching faction names from KNS_Faction."
            )
            factions_query = "SELECT DISTINCT Name AS FactionName, FactionID, KnessetNum FROM KNS_Faction ORDER BY FactionName;"
        else:
            ui_logger.warning(
                "KNS_Faction table not found. Cannot fetch faction filter options."
            )
            return knesset_nums_options, pd.DataFrame(
                columns=["FactionName", "FactionID", "KnessetNum"]
            )

        factions_df = _safe_execute_query(con, factions_query)
        ui_logger.info(
            f"Filter options fetched: {len(knesset_nums_options)} Knesset Nums, {len(factions_df)} Factions."
        )
        return knesset_nums_options, factions_df
    except Exception as e:
        ui_logger.error(f"Error in get_filter_options_from_db: {e}", exc_info=True)
        st.sidebar.error(f"DB error fetching filters: {e}", icon="🔥")
        return [], pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])


knesset_nums_options, factions_options_df = get_filter_options_from_db()
faction_display_map = {
    f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"]
    for _, row in factions_options_df.iterrows()
}


def _human_ts(ts_value):
    """Converts a timestamp or datetime object to a human-readable UTC string."""
    if ts_value is None or pd.isna(ts_value):
        return "N/A"
    try:
        if isinstance(ts_value, (int, float)):
            dt_obj = datetime.fromtimestamp(ts_value, ZoneInfo("UTC"))
        elif isinstance(ts_value, str):
            try:
                dt_obj = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
            except ValueError:
                dt_obj = pd.to_datetime(ts_value).to_pydatetime()
        elif isinstance(ts_value, datetime):
            dt_obj = ts_value
        elif isinstance(ts_value, pd.Timestamp):
            dt_obj = ts_value.to_pydatetime()
        else:
            return "Invalid date format"

        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=ZoneInfo("UTC"))
        else:
            dt_obj = dt_obj.astimezone(ZoneInfo("UTC"))
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception as e:
        ui_logger.warning(f"Could not parse timestamp '{ts_value}': {e}")
        return str(ts_value)


def _get_last_updated_for_table(table_name: str) -> str:
    """Gets the last updated timestamp for a table (Parquet file modification time)."""
    parquet_file = PARQUET_DIR / f"{table_name}.parquet"
    if parquet_file.exists():
        try:
            return _human_ts(parquet_file.stat().st_mtime)
        except Exception as e:
            ui_logger.warning(f"Could not get mod_time for {parquet_file}: {e}")
            return "Error reading timestamp"
    return "Never (or N/A)"


# --- EXPORTS Dictionary for Predefined Queries ---
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

# --- Plot Generators Mapping ---
AVAILABLE_PLOTS = {
    "Number of Queries per Year": pg.plot_queries_by_year,
    "Distribution of Query Types": pg.plot_query_types_distribution,
    "Number of Agenda Items per Year": pg.plot_agendas_by_year,
    "Distribution of Agenda Classifications": pg.plot_agenda_classifications_pie,
    "Number of Factions per Knesset": pg.plot_factions_per_knesset,
}


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar UI
# ──────────────────────────────────────────────────────────────────────────────
def _handle_data_refresh_button_click():
    if st.session_state.get("data_refresh_process_running", False):
        st.sidebar.warning("Refresh process is already running.")
        return

    all_tables_selected = st.session_state.get("ms_tables_to_refresh", [])
    if not all_tables_selected:
        st.sidebar.warning("No tables selected for refresh.")
        return

    tables_to_run = [t for t in all_tables_selected if t != _SELECT_ALL_TABLES_OPTION]
    if _SELECT_ALL_TABLES_OPTION in all_tables_selected or not tables_to_run:
        tables_to_run = TABLES
    if not tables_to_run:
        st.sidebar.info("No tables are defined or selected for refresh.")
        return

    st.session_state.data_refresh_process_running = True
    ui_logger.info(f"Starting data refresh for tables: {tables_to_run}")
    progress_bar_sidebar = st.sidebar.progress(0, text="Preparing refresh...")
    status_text_sidebar = st.sidebar.empty()
    status_text_sidebar.text("Initializing refresh...")

    def _sidebar_progress_cb(table_name_done: str, num_rows_fetched: int):
        total_tables = len(tables_to_run)
        if "completed_tables_count" not in st.session_state:
            st.session_state.completed_tables_count = 0
        st.session_state.completed_tables_count += 1
        percentage = (st.session_state.completed_tables_count / total_tables) * 100
        message = f"Fetched {num_rows_fetched} rows for {table_name_done}. ({st.session_state.completed_tables_count}/{total_tables} tables done)"
        progress_bar_sidebar.progress(int(percentage), text=message)
        status_text_sidebar.text(message)
        ui_logger.info(message)

    async def _refresh_async_wrapper(tables_list_async):
        st.session_state.completed_tables_count = 0
        await ft.refresh_tables(
            tables=tables_list_async, progress_cb=_sidebar_progress_cb, db_path=DB_PATH
        )

    try:
        asyncio.run(_refresh_async_wrapper(tables_to_run))
        st.sidebar.success("Data refresh process complete!")
        status_text_sidebar.success("All selected tables refreshed successfully.")
        progress_bar_sidebar.progress(100, text="Refresh complete!")
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    except Exception as e:
        ui_logger.error(f"❌ Data Refresh Error: {e}", exc_info=True)
        st.sidebar.error(f"❌ Data Refresh Error: {e}")
        st.sidebar.code(f"Error: {str(e)}\n\nTraceback:\n{_format_exc()}")
        status_text_sidebar.error(f"Error during refresh: {e}")
        progress_bar_sidebar.progress(0, text=f"Error: {e}")
    finally:
        st.session_state.data_refresh_process_running = False
        if "completed_tables_count" in st.session_state:
            del st.session_state.completed_tables_count


st.sidebar.header("🔄 Data Refresh Controls")
options_for_multiselect = [_SELECT_ALL_TABLES_OPTION] + TABLES
default_selection_refresh = st.session_state.get("ms_tables_to_refresh", [])
st.session_state.ms_tables_to_refresh = st.sidebar.multiselect(
    label="Select tables to refresh/fetch:",
    options=options_for_multiselect,
    default=default_selection_refresh,
    key="ms_tables_to_refresh_widget",
)

if _SELECT_ALL_TABLES_OPTION in st.session_state.ms_tables_to_refresh:
    if not st.session_state.get("all_tables_selected_for_refresh_flag", False):
        st.session_state.ms_tables_to_refresh = [_SELECT_ALL_TABLES_OPTION] + TABLES
        st.session_state.all_tables_selected_for_refresh_flag = True
        st.rerun()
elif (
    st.session_state.get("all_tables_selected_for_refresh_flag", False)
    and _SELECT_ALL_TABLES_OPTION not in st.session_state.ms_tables_to_refresh
):
    st.session_state.ms_tables_to_refresh = []
    st.session_state.all_tables_selected_for_refresh_flag = False
    st.rerun()

if st.sidebar.button("🔄 Refresh Selected Data", key="btn_refresh_data"):
    _handle_data_refresh_button_click()

st.sidebar.divider()
st.sidebar.header("🔎 Predefined Queries")
query_names_options = [""] + list(EXPORTS.keys())
st.session_state.selected_query_name = st.sidebar.selectbox(
    "Select a predefined query:",
    options=query_names_options,
    index=query_names_options.index(st.session_state.selected_query_name)
    if st.session_state.selected_query_name in query_names_options
    else 0,
    key="sb_selected_query_name",
)

if st.sidebar.button(
    "▶️ Run Selected Query", disabled=(not st.session_state.selected_query_name)
):
    if st.session_state.selected_query_name and DB_PATH.exists():
        try:
            query_info = EXPORTS[st.session_state.selected_query_name]
            base_sql = query_info["sql"]
            knesset_filter_col = query_info.get("knesset_filter_column")
            faction_filter_col = query_info.get("faction_filter_column")
            modified_sql = base_sql.strip().rstrip(";")
            applied_filters_info = []
            where_conditions = []

            if knesset_filter_col and st.session_state.ms_knesset_filter:
                selected_knesset_nums = st.session_state.ms_knesset_filter
                where_conditions.append(
                    f"{knesset_filter_col} IN ({', '.join(map(str, selected_knesset_nums))})"
                )
                applied_filters_info.append(
                    f"KnessetNum IN ({', '.join(map(str, selected_knesset_nums))})"
                )
            if faction_filter_col and st.session_state.ms_faction_filter:
                selected_faction_ids = [
                    faction_display_map[name]
                    for name in st.session_state.ms_faction_filter
                    if name in faction_display_map
                ]
                if selected_faction_ids:
                    where_conditions.append(
                        f"{faction_filter_col} IN ({', '.join(map(str, selected_faction_ids))})"
                    )
                    applied_filters_info.append(
                        f"FactionID IN ({', '.join(map(str, selected_faction_ids))})"
                    )

            if where_conditions:
                combined_where_clause = " AND ".join(where_conditions)
                keyword_to_use = (
                    "AND"
                    if re.search(r"\sWHERE\s", modified_sql, re.IGNORECASE)
                    else "WHERE"
                )
                filter_string_to_add = f" {keyword_to_use} {combined_where_clause}"
                clauses_keywords_to_find = [
                    r"GROUP\s+BY",
                    r"HAVING",
                    r"WINDOW",
                    r"ORDER\s+BY",
                    r"LIMIT",
                    r"OFFSET",
                    r"FETCH",
                ]
                insertion_point = len(modified_sql)
                for pattern_str in clauses_keywords_to_find:
                    match = re.search(pattern_str, modified_sql, re.IGNORECASE)
                    if match and match.start() < insertion_point:
                        insertion_point = match.start()
                prefix = modified_sql[:insertion_point].strip()
                suffix = modified_sql[insertion_point:].strip()
                modified_sql = f"{prefix}{filter_string_to_add} {suffix}".strip()

            ui_logger.info(
                f"Executing predefined query: {st.session_state.selected_query_name} with SQL:\n{modified_sql}"
            )
            con = _connect(read_only=True)
            st.session_state.query_results_df = con.sql(modified_sql).df()
            st.session_state.executed_query_name = st.session_state.selected_query_name
            st.session_state.show_query_results = True
            st.session_state.show_table_explorer_results = False
            st.session_state.applied_filters_info_query = applied_filters_info
            st.session_state.last_executed_sql = modified_sql
            st.toast(
                f"✅ Query '{st.session_state.executed_query_name}' executed.",
                icon="📊",
            )
        except Exception as e:
            ui_logger.error(
                f"Error executing query '{st.session_state.selected_query_name}': {e}",
                exc_info=True,
            )
            ui_logger.error(
                f"Failed SQL for '{st.session_state.selected_query_name}':\n{modified_sql if 'modified_sql' in locals() else base_sql}"
            )
            st.error(
                f"Error executing query '{st.session_state.selected_query_name}': {e}"
            )
            st.code(str(e) + "\n\n" + _format_exc())
            st.session_state.show_query_results = False
            st.session_state.query_results_df = pd.DataFrame()
    elif not DB_PATH.exists():
        st.error(
            "Database not found. Please ensure 'data/warehouse.duckdb' exists or run data refresh."
        )
        st.session_state.show_query_results = False

st.sidebar.divider()
st.sidebar.header("🔬 Interactive Table Explorer")
db_tables_list_for_explorer = [""] + get_db_table_list()
st.session_state.selected_table_for_explorer = st.sidebar.selectbox(
    "Select a table to explore:",
    options=db_tables_list_for_explorer,
    index=db_tables_list_for_explorer.index(
        st.session_state.selected_table_for_explorer
    )
    if st.session_state.selected_table_for_explorer in db_tables_list_for_explorer
    else 0,
    key="sb_selected_table_explorer",
)

if st.sidebar.button(
    "🔍 Explore Selected Table",
    disabled=(not st.session_state.selected_table_for_explorer),
):
    if st.session_state.selected_table_for_explorer and DB_PATH.exists():
        table_to_explore = st.session_state.selected_table_for_explorer
        try:
            con = _connect(read_only=True)
            base_query = f'SELECT * FROM "{table_to_explore}"'
            where_clauses = []
            table_columns_df = con.execute(
                f"PRAGMA table_info('{table_to_explore}')"
            ).df()
            table_columns = table_columns_df["name"].str.lower().tolist()
            db_tables_list_lower = [t.lower() for t in get_db_table_list()]
            join_clause = ""
            select_prefix = f'"{table_to_explore}".*'

            if (
                table_to_explore.lower() == "kns_faction"
                and "userfactioncoalitionstatus" in db_tables_list_lower
            ):
                select_prefix = "f.*, ufs.CoalitionStatus AS UserCoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition"
                base_query = f"SELECT {select_prefix} FROM KNS_Faction f"
                join_clause = "LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID AND f.KnessetNum = ufs.KnessetNum"
            elif (
                table_to_explore.lower() == "kns_persontoposition"
                and "userfactioncoalitionstatus" in db_tables_list_lower
            ):
                select_prefix = "p2p.*, ufs.CoalitionStatus AS UserCoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition"
                base_query = f"SELECT {select_prefix} FROM KNS_PersonToPosition p2p"
                join_clause = "LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum"

            knesset_col_name_explorer = "KnessetNum"
            if table_to_explore.lower() == "kns_faction" and join_clause:
                knesset_col_name_explorer = "f.KnessetNum"
            if table_to_explore.lower() == "kns_persontoposition" and join_clause:
                knesset_col_name_explorer = "p2p.KnessetNum"
            if "knessetnum" in table_columns and st.session_state.ms_knesset_filter:
                where_clauses.append(
                    f"{knesset_col_name_explorer} IN ({', '.join(map(str, st.session_state.ms_knesset_filter))})"
                )

            faction_col_name_explorer = "FactionID"
            if table_to_explore.lower() == "kns_faction" and join_clause:
                faction_col_name_explorer = "f.FactionID"
            if table_to_explore.lower() == "kns_persontoposition" and join_clause:
                faction_col_name_explorer = "p2p.FactionID"
            if "factionid" in table_columns and st.session_state.ms_faction_filter:
                selected_faction_ids_explorer = [
                    faction_display_map[name]
                    for name in st.session_state.ms_faction_filter
                    if name in faction_display_map
                ]
                if selected_faction_ids_explorer:
                    where_clauses.append(
                        f"{faction_col_name_explorer} IN ({', '.join(map(str, selected_faction_ids_explorer))})"
                    )

            final_query = base_query
            if join_clause:
                final_query += f" {join_clause}"
            if where_clauses:
                final_query += " WHERE " + " AND ".join(where_clauses)

            order_by_col_explorer = None
            if "lastupdateddate" in table_columns:
                order_by_col_explorer = "LastUpdatedDate"
            elif "startdate" in table_columns:
                order_by_col_explorer = "StartDate"
            elif table_columns:
                order_by_col_explorer = f'"{table_columns[0]}"'
            if order_by_col_explorer:
                prefix_for_order = (
                    "f."
                    if table_to_explore.lower() == "kns_faction" and join_clause
                    else "p2p."
                    if table_to_explore.lower() == "kns_persontoposition"
                    and join_clause
                    else ""
                )
                final_query += (
                    f" ORDER BY {prefix_for_order}{order_by_col_explorer.replace('"', '')} DESC"
                    if prefix_for_order
                    and order_by_col_explorer.lower().replace('"', "") in table_columns
                    else f" ORDER BY {order_by_col_explorer} DESC"
                )
            final_query += " LIMIT 1000"

            ui_logger.info(
                f"Exploring table '{table_to_explore}' with SQL: {final_query}"
            )
            st.session_state.table_explorer_df = con.sql(final_query).df()
            st.session_state.executed_table_explorer_name = table_to_explore
            st.session_state.show_table_explorer_results = True
            st.session_state.show_query_results = False
            st.toast(f"🔍 Explored table: {table_to_explore}", icon="📖")
        except Exception as e:
            ui_logger.error(
                f"Error exploring table '{table_to_explore}': {e}", exc_info=True
            )
            st.error(f"Error exploring table '{table_to_explore}': {e}")
            st.code(str(e) + "\n\n" + _format_exc())
            st.session_state.show_table_explorer_results = False
            st.session_state.table_explorer_df = pd.DataFrame()
    elif not st.session_state.selected_table_for_explorer:
        st.warning("Please select a table to explore.")
    elif not DB_PATH.exists():
        st.error("Database not found. Cannot explore tables.")

st.sidebar.divider()
st.sidebar.header("📊 Filters (Apply to Queries, Explorer & Plots)")
st.session_state.ms_knesset_filter = st.sidebar.multiselect(
    "Knesset Number(s):",
    options=knesset_nums_options,
    default=st.session_state.get("ms_knesset_filter", []),
    key="ms_knesset_filter_widget",
)
st.session_state.ms_faction_filter = st.sidebar.multiselect(
    "Faction(s) (by Knesset):",
    options=list(faction_display_map.keys()),
    default=st.session_state.get("ms_faction_filter", []),
    help="Select factions. The Knesset number in parentheses provides context.",
    key="ms_faction_filter_widget",
)

# ──────────────────────────────────────────────────────────────────────────────
# Main Area UI
# ──────────────────────────────────────────────────────────────────────────────
st.title("🇮🇱 Knesset Data Warehouse Console")

with st.expander("ℹ️ How This Works", expanded=False):
    st.markdown(
        dedent(f"""
        * **Data Refresh:** Use sidebar controls to fetch OData tables or update faction statuses.
        * **Predefined Queries:** Select a query, apply filters, click "Run". Results appear below.
        * **Interactive Table Explorer:** Select a table, apply filters, click "Explore". Results appear below.
        * **Predefined Visualizations:** Select a plot from the "Predefined Visualizations" section.
        * **Interactive Chart Builder:** Dynamically create your own charts in the "Interactive Chart Builder" section.
        * **Ad-hoc SQL:** Use the sandbox at the bottom to run custom SQL.
    """)
    )

# --- Predefined Query Results Area ---
st.divider()
st.header("📄 Predefined Query Results")
if st.session_state.show_query_results and st.session_state.executed_query_name:
    subheader_text = f"Results for: **{st.session_state.executed_query_name}**"
    if st.session_state.get("applied_filters_info_query"):
        subheader_text += f" (Active Filters: *{'; '.join(st.session_state.applied_filters_info_query)}*)"
    st.markdown(subheader_text)
    if not st.session_state.query_results_df.empty:
        st.dataframe(
            st.session_state.query_results_df, use_container_width=True, height=400
        )
        safe_name = re.sub(
            r"[^a-zA-Z0-9_\-]+", "_", st.session_state.executed_query_name
        )
        col_csv, col_excel = st.columns(2)
        with col_csv:
            st.download_button(
                "⬇️ CSV",
                st.session_state.query_results_df.to_csv(index=False).encode(
                    "utf-8-sig"
                ),
                f"{safe_name}_results.csv",
                "text/csv",
            )
        with col_excel:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                st.session_state.query_results_df.to_excel(
                    writer, index=False, sheet_name="Results"
                )
            st.download_button(
                "⬇️ Excel",
                excel_buffer.getvalue(),
                f"{safe_name}_results.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    else:
        st.info("The query returned no results with the current filters.")
    with st.expander("Show Executed SQL", expanded=False):
        st.code(
            st.session_state.get("last_executed_sql", "No SQL executed yet."),
            language="sql",
        )
else:
    st.info("Run a predefined query from the sidebar to see results here.")

# --- Table Explorer Results Area ---
st.divider()
st.header("📖 Interactive Table Explorer Results")
if (
    st.session_state.show_table_explorer_results
    and st.session_state.executed_table_explorer_name
):
    st.subheader(f"Exploring: **{st.session_state.executed_table_explorer_name}**")
    k_filters = st.session_state.get("ms_knesset_filter", [])
    f_filters = st.session_state.get("ms_faction_filter", [])
    st.markdown(
        f"Active Filters: Knesset(s): `{k_filters or 'All'}` Faction(s): `{f_filters or 'All'}`"
    )
    if not st.session_state.table_explorer_df.empty:
        st.dataframe(
            st.session_state.table_explorer_df, use_container_width=True, height=400
        )
        safe_name = re.sub(
            r"[^a-zA-Z0-9_\-]+", "_", st.session_state.executed_table_explorer_name
        )
        col_csv, col_excel = st.columns(2)
        with col_csv:
            st.download_button(
                "⬇️ CSV",
                st.session_state.table_explorer_df.to_csv(index=False).encode(
                    "utf-8-sig"
                ),
                f"{safe_name}_data.csv",
                "text/csv",
            )
        with col_excel:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                st.session_state.table_explorer_df.to_excel(
                    writer, index=False, sheet_name="TableData"
                )
            st.download_button(
                "⬇️ Excel",
                excel_buffer.getvalue(),
                f"{safe_name}_data.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    else:
        st.info("The table exploration returned no results with the current filters.")
else:
    st.info("Explore a table from the sidebar to see its data here.")

# --- Predefined Data Visualizations Section ---
st.divider()
st.header("📈 Predefined Visualizations")
if not DB_PATH.exists():
    st.warning(
        "Database not found. Visualizations cannot be generated. Please run a data refresh."
    )
else:
    plot_options = [""] + list(AVAILABLE_PLOTS.keys())
    current_selected_plot = st.session_state.get("selected_plot_name", "")
    if current_selected_plot not in plot_options:
        current_selected_plot = ""
        st.session_state.selected_plot_name = ""
    st.session_state.selected_plot_name = st.selectbox(
        "Choose a predefined visualization:",
        options=plot_options,
        index=plot_options.index(current_selected_plot),
        key="sb_selected_plot",
    )
    if st.session_state.selected_plot_name:
        plot_function = AVAILABLE_PLOTS[st.session_state.selected_plot_name]
        with st.spinner(f"Generating '{st.session_state.selected_plot_name}'..."):
            try:
                figure = plot_function(DB_PATH, _connect, ui_logger)
                if figure:
                    st.plotly_chart(figure, use_container_width=True)
                    st.session_state.generated_plot_figure = figure
            except Exception as e:
                ui_logger.error(
                    f"Error displaying plot '{st.session_state.selected_plot_name}': {e}",
                    exc_info=True,
                )
                st.error(f"An error occurred while generating the plot: {e}")
                st.code(str(e) + "\n\n" + _format_exc())
    else:
        st.info("Select a predefined visualization from the dropdown to display it.")

# --- Interactive Chart Builder Section ---
st.divider()
st.header("🛠️ Interactive Chart Builder")

if not DB_PATH.exists():
    st.warning(
        "Database not found. Chart Builder requires data. Please run a data refresh."
    )
else:
    # 1. Select Table
    db_tables_for_builder = [""] + get_db_table_list()
    selected_table = st.selectbox(
        "1. Select Table to Visualize:",
        options=db_tables_for_builder,
        index=db_tables_for_builder.index(st.session_state.builder_selected_table)
        if st.session_state.builder_selected_table in db_tables_for_builder
        else 0,
        key="builder_table_select",
    )

    if selected_table and selected_table != st.session_state.get(
        "builder_selected_table_previous_run"
    ):
        # Table has changed, update columns
        st.session_state.builder_selected_table = selected_table
        all_cols, numeric_cols, categorical_cols = get_table_columns(selected_table)
        st.session_state.builder_columns = [""] + all_cols  # Add empty option
        st.session_state.builder_numeric_columns = [""] + numeric_cols
        st.session_state.builder_categorical_columns = [""] + categorical_cols
        # Reset dependent selections
        st.session_state.builder_x_axis = None
        st.session_state.builder_y_axis = None
        st.session_state.builder_color = None
        st.session_state.builder_size = None
        st.session_state.builder_facet_row = None
        st.session_state.builder_facet_col = None
        st.session_state.builder_hover_name = None
        st.session_state.builder_names = None
        st.session_state.builder_values = None
        st.session_state.builder_generated_chart = None  # Clear previous chart
        st.rerun()  # Rerun to update UI with new column options
    st.session_state.builder_selected_table_previous_run = selected_table

    if st.session_state.builder_selected_table:
        st.write(f"Selected Table: **{st.session_state.builder_selected_table}**")

        # 2. Select Chart Type
        chart_types = ["bar", "line", "scatter", "pie", "histogram", "box"]
        st.session_state.builder_chart_type = st.selectbox(
            "2. Select Chart Type:",
            options=chart_types,
            index=chart_types.index(st.session_state.builder_chart_type)
            if st.session_state.builder_chart_type in chart_types
            else 0,
        )

        cols_c1, cols_c2 = st.columns(2)

        with cols_c1:
            # 3. Select Axes and other aesthetics
            if st.session_state.builder_chart_type not in ["pie"]:
                st.session_state.builder_x_axis = st.selectbox(
                    "X-axis:",
                    options=st.session_state.builder_columns,
                    index=st.session_state.builder_columns.index(
                        st.session_state.builder_x_axis
                    )
                    if st.session_state.builder_x_axis
                    in st.session_state.builder_columns
                    else 0,
                )

            if st.session_state.builder_chart_type not in [
                "pie",
                "histogram",
            ]:  # Histogram Y is count
                st.session_state.builder_y_axis = st.selectbox(
                    "Y-axis:",
                    options=st.session_state.builder_numeric_columns
                    if st.session_state.builder_chart_type != "bar"
                    else st.session_state.builder_columns,  # Bar Y can be cat or num
                    index=(
                        st.session_state.builder_numeric_columns
                        if st.session_state.builder_chart_type != "bar"
                        else st.session_state.builder_columns
                    ).index(st.session_state.builder_y_axis)
                    if st.session_state.builder_y_axis
                    in (
                        st.session_state.builder_numeric_columns
                        if st.session_state.builder_chart_type != "bar"
                        else st.session_state.builder_columns
                    )
                    else 0,
                    help="Select a numeric column for Y-axis (except for Bar charts which can also use categorical)."
                    if st.session_state.builder_chart_type != "bar"
                    else "Select column for Y-axis.",
                )

            if st.session_state.builder_chart_type == "pie":
                st.session_state.builder_names = st.selectbox(
                    "Names (for Pie chart slices):",
                    options=st.session_state.builder_categorical_columns,
                    index=st.session_state.builder_categorical_columns.index(
                        st.session_state.builder_names
                    )
                    if st.session_state.builder_names
                    in st.session_state.builder_categorical_columns
                    else 0,
                )
                st.session_state.builder_values = st.selectbox(
                    "Values (for Pie chart sizes):",
                    options=st.session_state.builder_numeric_columns,
                    index=st.session_state.builder_numeric_columns.index(
                        st.session_state.builder_values
                    )
                    if st.session_state.builder_values
                    in st.session_state.builder_numeric_columns
                    else 0,
                )

            st.session_state.builder_color = st.selectbox(
                "Color by:",
                options=st.session_state.builder_columns,
                index=st.session_state.builder_columns.index(
                    st.session_state.builder_color
                )
                if st.session_state.builder_color in st.session_state.builder_columns
                else 0,
            )

            if st.session_state.builder_chart_type in ["scatter"]:
                st.session_state.builder_size = st.selectbox(
                    "Size by (for scatter):",
                    options=st.session_state.builder_numeric_columns,
                    index=st.session_state.builder_numeric_columns.index(
                        st.session_state.builder_size
                    )
                    if st.session_state.builder_size
                    in st.session_state.builder_numeric_columns
                    else 0,
                )

        with cols_c2:
            st.session_state.builder_facet_row = st.selectbox(
                "Facet Row by:",
                options=st.session_state.builder_columns,
                index=st.session_state.builder_columns.index(
                    st.session_state.builder_facet_row
                )
                if st.session_state.builder_facet_row
                in st.session_state.builder_columns
                else 0,
            )
            st.session_state.builder_facet_col = st.selectbox(
                "Facet Column by:",
                options=st.session_state.builder_columns,
                index=st.session_state.builder_columns.index(
                    st.session_state.builder_facet_col
                )
                if st.session_state.builder_facet_col
                in st.session_state.builder_columns
                else 0,
            )
            st.session_state.builder_hover_name = st.selectbox(
                "Hover Name:",
                options=st.session_state.builder_columns,
                index=st.session_state.builder_columns.index(
                    st.session_state.builder_hover_name
                )
                if st.session_state.builder_hover_name
                in st.session_state.builder_columns
                else 0,
            )

            if st.session_state.builder_chart_type not in ["pie"]:
                st.session_state.builder_log_x = st.checkbox(
                    "Logarithmic X-axis", value=st.session_state.builder_log_x
                )
                if st.session_state.builder_chart_type not in ["histogram"]:
                    st.session_state.builder_log_y = st.checkbox(
                        "Logarithmic Y-axis", value=st.session_state.builder_log_y
                    )

            if st.session_state.builder_chart_type == "bar":
                st.session_state.builder_barmode = st.selectbox(
                    "Bar Mode:",
                    options=["relative", "group", "overlay", "stack"],
                    index=["relative", "group", "overlay", "stack"].index(
                        st.session_state.builder_barmode
                    )
                    if st.session_state.builder_barmode
                    in ["relative", "group", "overlay", "stack"]
                    else 0,
                )

        if st.button("📊 Generate Chart", key="btn_generate_custom_chart"):
            if not st.session_state.builder_selected_table:
                st.error("Please select a table first.")
            elif st.session_state.builder_chart_type not in ["pie", "histogram"] and (
                not st.session_state.builder_x_axis
                or not st.session_state.builder_y_axis
            ):
                st.error("Please select X-axis and Y-axis columns.")
            elif (
                st.session_state.builder_chart_type == "histogram"
                and not st.session_state.builder_x_axis
            ):
                st.error("Please select X-axis for the histogram.")
            elif st.session_state.builder_chart_type == "pie" and (
                not st.session_state.builder_names
                or not st.session_state.builder_values
            ):
                st.error(
                    "Please select 'Names' and 'Values' columns for the Pie chart."
                )
            else:
                try:
                    con = _connect(read_only=True)
                    # Apply sidebar filters (KnessetNum, FactionID) to the selected table for the chart builder
                    # This makes the chart builder context-aware of the global filters

                    df_full = con.table(
                        f'"{st.session_state.builder_selected_table}"'
                    ).df()  # Load full table first

                    # Apply Knesset Number Filter if applicable
                    if (
                        "KnessetNum" in df_full.columns
                        and st.session_state.ms_knesset_filter
                    ):
                        df_full = df_full[
                            df_full["KnessetNum"].isin(
                                st.session_state.ms_knesset_filter
                            )
                        ]

                    # Apply Faction Filter if applicable
                    if (
                        "FactionID" in df_full.columns
                        and st.session_state.ms_faction_filter
                    ):
                        selected_faction_ids_builder = [
                            faction_display_map[name]
                            for name in st.session_state.ms_faction_filter
                            if name in faction_display_map
                        ]
                        if selected_faction_ids_builder:
                            df_full = df_full[
                                df_full["FactionID"].isin(selected_faction_ids_builder)
                            ]

                    if df_full.empty:
                        st.warning(
                            "No data in the selected table after applying sidebar filters. Cannot generate chart."
                        )
                        st.session_state.builder_generated_chart = None
                    else:
                        chart_params = {
                            "data_frame": df_full,
                            "title": f"{st.session_state.builder_chart_type.capitalize()} of {st.session_state.builder_selected_table}",
                        }
                        if st.session_state.builder_x_axis:
                            chart_params["x"] = st.session_state.builder_x_axis
                        if (
                            st.session_state.builder_y_axis
                            and st.session_state.builder_chart_type not in ["histogram"]
                        ):
                            chart_params["y"] = st.session_state.builder_y_axis
                        if st.session_state.builder_color:
                            chart_params["color"] = st.session_state.builder_color
                        if (
                            st.session_state.builder_size
                            and st.session_state.builder_chart_type == "scatter"
                        ):
                            chart_params["size"] = st.session_state.builder_size
                        if st.session_state.builder_facet_row:
                            chart_params["facet_row"] = (
                                st.session_state.builder_facet_row
                            )
                        if st.session_state.builder_facet_col:
                            chart_params["facet_col"] = (
                                st.session_state.builder_facet_col
                            )
                        if st.session_state.builder_hover_name:
                            chart_params["hover_name"] = (
                                st.session_state.builder_hover_name
                            )
                        if (
                            st.session_state.builder_log_x
                            and st.session_state.builder_chart_type not in ["pie"]
                        ):
                            chart_params["log_x"] = st.session_state.builder_log_x
                        if (
                            st.session_state.builder_log_y
                            and st.session_state.builder_chart_type
                            not in ["pie", "histogram"]
                        ):
                            chart_params["log_y"] = st.session_state.builder_log_y
                        if (
                            st.session_state.builder_chart_type == "bar"
                            and st.session_state.builder_barmode
                        ):
                            chart_params["barmode"] = st.session_state.builder_barmode
                        if st.session_state.builder_chart_type == "pie":
                            chart_params["names"] = st.session_state.builder_names
                            chart_params["values"] = st.session_state.builder_values

                        # Select the correct Plotly Express function
                        if st.session_state.builder_chart_type == "bar":
                            fig_builder = px.bar(**chart_params)
                        elif st.session_state.builder_chart_type == "line":
                            fig_builder = px.line(**chart_params)
                        elif st.session_state.builder_chart_type == "scatter":
                            fig_builder = px.scatter(**chart_params)
                        elif st.session_state.builder_chart_type == "pie":
                            fig_builder = px.pie(**chart_params)
                        elif st.session_state.builder_chart_type == "histogram":
                            fig_builder = px.histogram(**chart_params)
                        elif st.session_state.builder_chart_type == "box":
                            fig_builder = px.box(**chart_params)
                        else:
                            st.error(
                                f"Unsupported chart type: {st.session_state.builder_chart_type}"
                            )
                            fig_builder = None

                        if fig_builder:
                            st.session_state.builder_generated_chart = fig_builder
                            st.toast(
                                f"Chart '{chart_params['title']}' generated!", icon="🎉"
                            )
                        else:
                            st.session_state.builder_generated_chart = None

                except Exception as e:
                    ui_logger.error(
                        f"Error generating custom chart: {e}", exc_info=True
                    )
                    st.error(f"Could not generate chart: {e}")
                    st.code(str(e) + "\n\n" + _format_exc())
                    st.session_state.builder_generated_chart = None

        if st.session_state.builder_generated_chart:
            st.plotly_chart(
                st.session_state.builder_generated_chart, use_container_width=True
            )


# --- Ad-hoc SQL Query Section ---
st.divider()
with st.expander("🧑‍🔬 Run an Ad-hoc SQL Query (Advanced)", expanded=False):
    if not DB_PATH.exists():
        st.warning("Database not found. Cannot run SQL queries.")
    else:
        st.markdown("""
        Construct your SQL query. Sidebar filters are **not** automatically applied here.
        Include them in your `WHERE` clause if needed.
        """)
        default_sql_query = "SELECT t.table_name, t.row_count FROM duckdb_tables() t WHERE t.schema_name = 'main' ORDER BY t.table_name;"
        sql_query_input = st.text_area(
            "Enter your SQL query:",
            default_sql_query,
            height=150,
            key="adhoc_sql_query",
        )
        if st.button("▶︎ Run Ad-hoc SQL", key="run_adhoc_sql"):
            if sql_query_input.strip():
                try:
                    con = _connect(read_only=True)
                    adhoc_result_df = con.sql(sql_query_input).df()
                    st.dataframe(adhoc_result_df, use_container_width=True)
                    if not adhoc_result_df.empty:
                        st.download_button(
                            "⬇️ CSV",
                            adhoc_result_df.to_csv(index=False).encode("utf-8-sig"),
                            "adhoc_results.csv",
                            "text/csv",
                        )
                except Exception as e:
                    ui_logger.error(f"❌ Ad-hoc SQL Query Error: {e}", exc_info=True)
                    st.error(f"❌ SQL Query Error: {e}")
                    st.code(str(e) + "\n\n" + _format_exc())
            else:
                st.warning("SQL query cannot be empty.")

# --- Table Update Status ---
st.divider()
with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
    if DB_PATH.exists():
        tables_to_check_status_main = sorted(list(set(TABLES)))
        status_data_main = [
            {
                "Table": t_name,
                "Last Updated (Parquet Mod Time)": _get_last_updated_for_table(t_name),
            }
            for t_name in tables_to_check_status_main
        ]
        if status_data_main:
            st.dataframe(
                pd.DataFrame(status_data_main),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No tables found to display status, or TABLES list is empty.")
    else:
        st.info("Database not found. Table status cannot be displayed.")
