from __future__ import annotations

# Standard Library Imports
import asyncio
import io
import logging
import re # For safe filename generation and SQL injection
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

# Add the 'src' directory to sys.path to allow absolute imports
_CURRENT_FILE_DIR = Path(__file__).resolve().parent 
_SRC_DIR = _CURRENT_FILE_DIR.parent 
_PROJECT_ROOT = _SRC_DIR.parent 

if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
if str(_PROJECT_ROOT) not in sys.path: 
    sys.path.insert(0, str(_PROJECT_ROOT))

# Local Application Imports
from utils.logger_setup import setup_logging # type: ignore
from backend.fetch_table import TABLES # type: ignore # Import TABLES list
import backend.fetch_table as ft # type: ignore

# Initialize logger for the UI module
ui_logger = setup_logging('knesset.ui.data_refresh', console_output=True)

_ALL_TABLE_NAMES_FROM_METADATA = TABLES # Use TABLES list directly for _get_all_table_statuses
_SELECT_ALL_TABLES_OPTION = "🔄 Select/Deselect All Tables" # Define the constant

# Helper to format exceptions for UI display
_DEF_LOG_FORMATTER = logging.Formatter()
def _format_exc():
    return _DEF_LOG_FORMATTER.formatException(sys.exc_info())

# local modules  – keep alias `ft` for simplicity

DB_PATH = Path("data/warehouse.duckdb") 
PARQUET_DIR = Path("data/parquet") 

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


st.set_page_config(page_title="Knesset OData – Refresh & Export", layout="wide")

# Initialize session state variables if they don't exist
# For Predefined Queries
if 'selected_query_name' not in st.session_state:
    st.session_state.selected_query_name = None
if 'executed_query_name' not in st.session_state:
    st.session_state.executed_query_name = None
if 'executed_sql_string' not in st.session_state: # To store the actually executed SQL
    st.session_state.executed_sql_string = ""
if 'query_results_df' not in st.session_state:
    st.session_state.query_results_df = pd.DataFrame()
if 'show_query_results' not in st.session_state:
    st.session_state.show_query_results = False
if 'applied_knesset_filter_to_query' not in st.session_state: # To store applied filter info
    st.session_state.applied_knesset_filter_to_query = []

# For Interactive Table Explorer
if 'selected_table_for_explorer' not in st.session_state:
    st.session_state.selected_table_for_explorer = None
if 'executed_table_explorer_name' not in st.session_state:
    st.session_state.executed_table_explorer_name = None
if 'table_explorer_df' not in st.session_state:
    st.session_state.table_explorer_df = pd.DataFrame()
if 'show_table_explorer_results' not in st.session_state:
    st.session_state.show_table_explorer_results = False

# For Sidebar Filters - ensure they are initialized for direct access
if 'ms_knesset_filter' not in st.session_state:
    st.session_state.ms_knesset_filter = []
if 'ms_faction_filter' not in st.session_state:
    st.session_state.ms_faction_filter = []

# Helper function definitions moved here to ensure they are defined before use

@st.cache_resource # Use cache_resource for connection objects
def _connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Establishes a new connection to the DuckDB database."""
    # Ensure DB_PATH is defined (it should be at the top of the script)
    if not DB_PATH.exists() and not read_only:
        # This info is helpful if the user intends to write data
        st.info(f"Database {DB_PATH} does not exist. It will be created by DuckDB during write operation.")
    elif not DB_PATH.exists() and read_only:
        # For read-only operations, if DB doesn't exist, it's a problem.
        st.warning(f"Database {DB_PATH} does not exist. Please run a data refresh first. Query execution will fail.")
        # Connect to an in-memory DB as a fallback to avoid crashing, though queries will likely be empty.
        return duckdb.connect(database=':memory:', read_only=True) 
    return duckdb.connect(database=DB_PATH.as_posix(), read_only=read_only)

@st.cache_data(ttl=3600) # Cache for 1 hour
def get_db_table_list():
    """Fetches the list of all tables from the database."""
    ui_logger.info("Fetching database table list...")
    if not DB_PATH.exists():
        ui_logger.warning("Database file not found. Returning empty table list.")
        return []
    try:
        con = _connect(read_only=True) # Get cached connection
        tables_df = con.execute("SHOW TABLES;").df()
        table_list = sorted(tables_df['name'].tolist()) if not tables_df.empty else []
        ui_logger.info(f"Database table list fetched: {len(table_list)} tables.")
        return table_list
    except duckdb.Error as e:
        ui_logger.error(f"Database error in get_db_table_list: {e}", exc_info=True)
        st.sidebar.error(f"DB error listing tables: {e}", icon="🔥")
        return []
    except Exception as e:
        ui_logger.error(f"Unexpected error in get_db_table_list: {e}", exc_info=True)
        st.sidebar.error(f"Error listing tables: {e}", icon="🔥")
        return []

# @st.cache_data(ttl="1h", show_spinner="Fetching filter options...") # Potential caching strategy
def get_filter_options_from_db():
    """Fetches distinct Knesset numbers and faction data for filter dropdowns."""
    ui_logger.info("Fetching filter options from database...")
    if not DB_PATH.exists():
        ui_logger.warning("Database file not found. Returning empty filter options.")
        st.sidebar.warning("DB not found. Filters unavailable.", icon="⚠️")
        return [], pd.DataFrame(columns=['FactionName', 'FactionID'])

    try:
        con = _connect(read_only=True) # Get cached connection
        # Get Knesset Numbers
        knesset_nums_query = "SELECT DISTINCT KnessetNum FROM KNS_KnessetDates ORDER BY KnessetNum DESC;" # Corrected table name
        knesset_nums_df = con.execute(knesset_nums_query).df()
        knesset_nums_options = sorted(knesset_nums_df['KnessetNum'].unique().tolist(), reverse=True) if not knesset_nums_df.empty else []
        
        # Get Factions by joining KNS_Faction with UserFactionCoalitionStatus
        factions_query = """
            SELECT DISTINCT
                ufcs.FactionName,
                ufcs.FactionID,
                ufcs.KnessetNum
            FROM
                KNS_Faction AS kf
            INNER JOIN
                UserFactionCoalitionStatus AS ufcs ON kf.FactionID = ufcs.FactionID
            ORDER BY
                ufcs.FactionName;
        """
        factions_df = con.execute(factions_query).df()
        
        ui_logger.info(f"Filter options fetched: {len(knesset_nums_options)} Knesset Nums, {len(factions_df)} Factions.")
        return knesset_nums_options, factions_df
    except duckdb.Error as e: # More specific exception
        ui_logger.error(f"Database error in get_filter_options_from_db: {e}", exc_info=True)
        st.sidebar.error(f"DB error fetching filters: {e}", icon="🔥")
        return [], pd.DataFrame(columns=['FactionName', 'FactionID'])
    except Exception as e:
        ui_logger.error(f"Unexpected error in get_filter_options_from_db: {e}", exc_info=True)
        st.sidebar.error(f"Error fetching filters: {e}", icon="🔥")
        return [], pd.DataFrame(columns=['FactionName', 'FactionID'])

# Define filter options data early, as it's needed by both main page logic and sidebar
knesset_nums_options, factions_options_df = get_filter_options_from_db()
faction_display_map = {f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"] for _, row in factions_options_df.iterrows()}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _human_ts(ts_value):
    """Converts a timestamp or datetime object to a human-readable UTC string."""
    if ts_value is None:
        return "N/A"
    try:
        # If it's a float/int timestamp (seconds since epoch)
        if isinstance(ts_value, (int, float)):
            dt_obj = datetime.fromtimestamp(ts_value, ZoneInfo('UTC'))
        elif isinstance(ts_value, str):
            # Attempt to parse common ISO formats
            try:
                dt_obj = datetime.fromisoformat(ts_value.replace('Z', '+00:00'))
            except ValueError:
                # Add other parsing attempts if needed, e.g., for non-standard formats
                dt_obj = pd.to_datetime(ts_value).to_pydatetime()
        elif isinstance(ts_value, datetime):
            dt_obj = ts_value
        else:
            return "Invalid date format"

        # Ensure datetime is UTC if not already, then format
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=ZoneInfo('UTC'))
        else:
            dt_obj = dt_obj.astimezone(ZoneInfo('UTC'))
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception as e:
        ui_logger.warning(f"Could not parse timestamp '{ts_value}': {e}")
        return str(ts_value) # fallback to original string if parsing fails

def _get_last_updated_from_db(table_name: str) -> str:
    """Fetches the last_updated timestamp for a given table from the metadata table."""
    # Returns a human-readable string or None if not found/error.
    # ui_logger.debug(f"Fetching last_updated for table: {table_name}")
    if not DB_PATH.exists():
        # ui_logger.warning(f"DB not found. Cannot get last_updated for {table_name}")
        return None # Return None if DB doesn't exist, _human_ts will handle it if called with None by other parts
    try:
        con = _connect(read_only=True) # Get cached connection
        query = "SELECT last_updated FROM table_metadata WHERE table_name = ?;"
        result = con.execute(query, [table_name]).fetchone()
        # ui_logger.debug(f"Last updated for {table_name}: {result[0] if result else 'Not found'}")
        return _human_ts(result[0]) if result and result[0] is not None else "Never (or N/A)"
    except duckdb.Error as e:
        ui_logger.error(f"DB error fetching last_updated for {table_name}: {e}")
        return "Error"
    except Exception as e:
        ui_logger.error(f"Unexpected error fetching last_updated for {table_name}: {e}")
        return "Error"
    # No con.close() here

# ──────────────────────────────────────────────────────────────────────────────
# EXPORTS Dictionary - Source for Predefined Queries
# ──────────────────────────────────────────────────────────────────────────────
EXPORTS = {
    "Queries + Full Details": {
        "sql": """
            SELECT
                Q.QueryID, Q.Number, Q.KnessetNum, Q.Name AS QueryName, Q.TypeID AS QueryTypeID, Q.TypeDesc AS QueryTypeDesc,
                Q.StatusID AS QueryStatusID, S.Desc AS QueryStatusDesc,
                Q.PersonID AS MKPersonID, P.FirstName AS MKFirstName, P.LastName AS MKLastName, P.GenderDesc AS MKGender,
                P2P.FactionName AS MKFactionName, P2P.FactionID AS MKFactionID,
                ufs.CoalitionStatus AS MKFactionCoalitionStatus, 
                Q.GovMinistryID, M.Name AS MinistryName,
                strftime(CAST(Q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDateFormatted,
                strftime(CAST(Q.ReplyMinisterDate AS TIMESTAMP), '%Y-%m-%d') AS ReplyMinisterDateFormatted,
                strftime(CAST(Q.ReplyDatePlanned AS TIMESTAMP), '%Y-%m-%d') AS ReplyDatePlannedFormatted,
                strftime(CAST(Q.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d %H:%M') AS LastUpdatedDateFormatted
            FROM KNS_Query Q
            LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
            LEFT JOIN KNS_PersonToPosition P2P ON Q.PersonID = P2P.PersonID
                AND Q.KnessetNum = P2P.KnessetNum
                AND CAST(Q.SubmitDate AS TIMESTAMP) >= CAST(P2P.StartDate AS TIMESTAMP)
                AND CAST(Q.SubmitDate AS TIMESTAMP) <= CAST(COALESCE(P2P.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN KNS_GovMinistry M ON Q.GovMinistryID = M.GovMinistryID
            LEFT JOIN KNS_Status S ON Q.StatusID = S.StatusID
            LEFT JOIN UserFactionCoalitionStatus ufs ON P2P.FactionID = ufs.FactionID AND P2P.KnessetNum = ufs.KnessetNum
            ORDER BY Q.KnessetNum DESC, Q.QueryID DESC
            LIMIT 10000;
        """,
        "knesset_filter_column": "Q.KnessetNum" # Column to use for KnessetNum filtering
    },
    "Agenda Items + Full Details": {
        "sql": """
            SELECT
                A.AgendaID, A.Number AS AgendaNumber, A.KnessetNum, A.Name AS AgendaName,
                A.ClassificationDesc AS AgendaClassification, A.SubTypeDesc AS AgendaSubType,
                S.Desc AS AgendaStatus,
                A.InitiatorPersonID, INIT_P.FirstName AS InitiatorFirstName, INIT_P.LastName AS InitiatorLastName, INIT_P.GenderDesc AS InitiatorGender,
                INIT_P2P.FactionName AS InitiatorFactionName, INIT_P2P.FactionID AS InitiatorFactionID,
                INIT_UFS.CoalitionStatus AS InitiatorFactionCoalitionStatus,
                A.CommitteeID AS HandlingCommitteeID, HC.Name AS HandlingCommitteeName,
                A.RecommendCommitteeID, RC.Name AS RecommendedCommitteeName,
                A.GovRecommendationDesc,
                A.MinisterPersonID, MIN_P.FirstName AS MinisterFirstName, MIN_P.LastName AS MinisterLastName,
                strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted,
                strftime(CAST(A.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d %H:%M') AS LastUpdatedDateFormatted
            FROM KNS_Agenda A
            LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
            LEFT JOIN KNS_Person INIT_P ON A.InitiatorPersonID = INIT_P.PersonID
            LEFT JOIN KNS_PersonToPosition INIT_P2P ON A.InitiatorPersonID = INIT_P2P.PersonID
                AND A.KnessetNum = INIT_P2P.KnessetNum
                AND CAST(COALESCE(A.PresidentDecisionDate, A.LastUpdatedDate) AS TIMESTAMP) >= CAST(INIT_P2P.StartDate AS TIMESTAMP)
                AND CAST(COALESCE(A.PresidentDecisionDate, A.LastUpdatedDate) AS TIMESTAMP) <= CAST(COALESCE(INIT_P2P.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN UserFactionCoalitionStatus INIT_UFS ON INIT_P2P.FactionID = INIT_UFS.FactionID AND INIT_P2P.KnessetNum = INIT_UFS.KnessetNum
            LEFT JOIN KNS_Committee HC ON A.CommitteeID = HC.CommitteeID
            LEFT JOIN KNS_Committee RC ON A.RecommendCommitteeID = RC.CommitteeID
            LEFT JOIN KNS_Person MIN_P ON A.MinisterPersonID = MIN_P.PersonID
            ORDER BY A.KnessetNum DESC, A.AgendaID DESC
            LIMIT 10000;
        """,
        "knesset_filter_column": "A.KnessetNum" # Column to use for KnessetNum filtering
    }
}

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────
def _handle_data_refresh_button_click():
    # Handles the logic when the '🔄 Refresh Selected Data' button is clicked.
    if st.session_state.data_refresh_process_running:
        return

    all_tables = st.session_state.get('ms_tables_to_refresh', []) # Default to empty list if not set
    
    if not all_tables:
        st.sidebar.warning("No tables selected.")
        return
    if not DB_PATH.exists():
        st.sidebar.error("Database not found. Cannot refresh tables.")
        return
    
    tables_to_run = [t for t in all_tables if t != _SELECT_ALL_TABLES_OPTION] # Filter out 'ALL'
    if _SELECT_ALL_TABLES_OPTION in all_tables:
        tables_to_run = TABLES # Use all defined tables if 'ALL' is selected
    
    if not tables_to_run:
        st.sidebar.info("No specific tables chosen for refresh (e.g., only 'ALL' was selected but no tables are defined, or selection was cleared).")
        return # Stop if no actual tables are to be processed

    st.session_state.data_refresh_process_running = True
    # Clear previous progress

    ui_logger.info(f"Starting data refresh for tables: {tables_to_run}")
    progress_bar_sidebar = st.sidebar.progress(0, text="Preparing refresh...")
    status_text_sidebar = st.sidebar.empty()

    def _sidebar_progress_cb(progress_data):
        # progress_data is expected to be a dict like {'percentage': float, 'message': str}
        if 'percentage' in progress_data and 'message' in progress_data:
            percentage = progress_data['percentage']
            message = progress_data['message']
            progress_bar_sidebar.progress(int(percentage), text=message)
            status_text_sidebar.text(message) # Also update the separate status text
        elif 'message' in progress_data: # Handle message-only updates
            status_text_sidebar.text(progress_data['message'])
            ui_logger.info(progress_data['message'])

    async def _refresh_async_wrapper(tables_to_run_async):
        await ft.refresh_tables(tables=tables_to_run_async, progress_cb=_sidebar_progress_cb, db_path=DB_PATH)

    with st.spinner("Fetching data... Please wait."): # This spinner might be redundant if sidebar progress is used
        try:
            asyncio.run(_refresh_async_wrapper(tables_to_run))
            st.sidebar.success("Data refresh process complete!")
            _sidebar_progress_cb({'percentage': 100, 'message': 'Refresh complete!'})
            # Refresh filter options and table list after successful data refresh
            st.cache_data.clear() # Clear data cache to get fresh filter options
            st.cache_resource.clear() # Clear resource cache (db connection)
            st.rerun() # Easiest way to ensure everything reloads with new data

        except Exception as e:
            ui_logger.error(f"❌ Data Refresh Error: {e}", exc_info=True)
            st.sidebar.error(f"❌ Data Refresh Error: {e}")
            st.sidebar.code(str(e) + "\n\n" + _format_exc())
            _sidebar_progress_cb({'percentage': 0, 'message': f'Error: {e}'})
        finally:
            st.session_state.data_refresh_process_running = False
            progress_bar_sidebar.empty() # Clear the progress bar
            status_text_sidebar.empty() # Clear the status text

st.sidebar.header("🔄 Data Refresh Controls")

default_selection = []
if 'ms_tables_to_refresh' in st.session_state:
    default_selection = st.session_state.ms_tables_to_refresh

selected_tables_for_refresh = st.sidebar.multiselect(
    label="Select tables to refresh/fetch:",
    options=TABLES, # Use TABLES list directly
    default=default_selection,
    key="ms_tables_to_refresh",
)

if st.sidebar.button(_SELECT_ALL_TABLES_OPTION, key="btn_select_all_tables_refresh"):
    if st.session_state.get('all_tables_selected_for_refresh', False):
        st.session_state.ms_tables_to_refresh = []
        st.session_state.all_tables_selected_for_refresh = False
    else:
        st.session_state.ms_tables_to_refresh = TABLES[:]
        st.session_state.all_tables_selected_for_refresh = True
    st.rerun()

if st.sidebar.button("🔄 Refresh Selected Data", on_click=_handle_data_refresh_button_click, key="btn_refresh_data"):
    pass # Logic is handled by on_click

# --- Predefined Queries Section ---
st.sidebar.divider()
st.sidebar.header("🔎 Predefined Queries")
query_names_options = [""] + list(EXPORTS.keys()) 
st.session_state.selected_query_name = st.sidebar.selectbox(
    "Select a predefined query:", options=query_names_options, index=0, key="sb_selected_query_name"
)

if st.sidebar.button("▶️ Run Selected Query", disabled=(not st.session_state.selected_query_name)):
    if st.session_state.selected_query_name and DB_PATH.exists():
        try:
            query_info = EXPORTS[st.session_state.selected_query_name]
            base_sql = query_info["sql"]
            knesset_filter_col = query_info.get("knesset_filter_column") # Get the column to filter on
            
            modified_sql = base_sql
            # Strip trailing whitespace and semicolon from base_sql before modification
            modified_sql = modified_sql.strip().rstrip(';')
            applied_filters_info = []

            # Apply Knesset Number filter if selected and applicable to the query
            if knesset_filter_col and st.session_state.ms_knesset_filter:
                selected_knesset_nums = st.session_state.ms_knesset_filter
                filter_clause = f"{knesset_filter_col} IN ({', '.join(map(str, selected_knesset_nums))})"
                applied_filters_info.append(f"KnessetNum IN ({', '.join(map(str, selected_knesset_nums))})")

                # Determine if we need 'WHERE' or 'AND' based on existing WHERE clause
                if re.search(r'\sWHERE\s', modified_sql, re.IGNORECASE):
                    keyword_to_use = "AND"
                else:
                    keyword_to_use = "WHERE"
                
                # filter_string_to_add has a leading space, but no trailing space initially.
                filter_string_to_add = f" {keyword_to_use} {filter_clause}"

                # Clauses to find; search for the start of these keywords.
                # Regex patterns do not require a leading space for the keyword itself.
                clauses_keywords_to_find = [
                    r'GROUP\s+BY', r'HAVING', r'WINDOW', r'ORDER\s+BY', 
                    r'LIMIT', r'OFFSET', r'FETCH'
                ]
                
                insertion_point = -1 # Default: append if no target clause found

                for pattern_str in clauses_keywords_to_find:
                    match = re.search(pattern_str, modified_sql, re.IGNORECASE)
                    if match:
                        current_match_start = match.start()
                        # Find the earliest occurring clause to insert before
                        if insertion_point == -1 or current_match_start < insertion_point:
                            insertion_point = current_match_start
                
                if insertion_point != -1:
                    # Insert the filter string, then a space, then the rest of the original query
                    prefix = modified_sql[:insertion_point]
                    suffix = modified_sql[insertion_point:]
                    modified_sql = prefix + filter_string_to_add + " " + suffix
                else:
                    # If none of the specified clauses were found, append the filter string.
                    # filter_string_to_add already has its necessary leading space.
                    modified_sql += filter_string_to_add
            
            ui_logger.info(f"Executing predefined query: {st.session_state.selected_query_name} with SQL:\n{modified_sql}")
            
            # Pre-execution validation of the modified_sql
            final_check_sql = modified_sql.strip()
            if not final_check_sql or not re.match(r'^(SELECT|WITH|INSERT|UPDATE|DELETE|VALUES|TABLE|EXPLAIN)\b', final_check_sql, re.IGNORECASE):
                error_message = (
                    f"Generated SQL query for '{st.session_state.selected_query_name}' is invalid or empty. "
                    f"Please check its base definition in EXPORTS. Generated SQL attempt:\n{modified_sql}"
                )
                ui_logger.error(error_message)
                st.error(error_message)
                # Optionally, clear previous results or set to empty DataFrame
                st.session_state.query_results_df = pd.DataFrame()
            else:
                con = _connect(read_only=True) # Get cached connection
                st.session_state.query_results_df = con.sql(modified_sql).df()
                st.session_state.executed_query_name = st.session_state.selected_query_name
                st.session_state.show_query_results = True
                st.session_state.show_table_explorer_results = False 
                st.session_state.applied_filters_info_query = applied_filters_info # Store for display
                st.session_state.last_executed_sql = modified_sql # Store for display
                # st.toast(f"✅ Query '{st.session_state.executed_query_name}' executed.", icon="📊")

        except Exception as e:
            ui_logger.error(f"Error executing query '{st.session_state.selected_query_name}': {e}")
            # Log the problematic SQL again, specifically at the point of failure
            ui_logger.error(f"Failed SQL for '{st.session_state.selected_query_name}':\n{modified_sql}")
            st.error(f"Error executing query '{st.session_state.selected_query_name}': {e}")
            st.code(str(e) + "\n\n" + _format_exc()) # Show full traceback in UI for debug
            st.session_state.show_query_results = False # Reset on error
            st.session_state.query_results_df = pd.DataFrame() # Reset on error

    elif not DB_PATH.exists():
        st.error("Database not found. Please ensure 'data/warehouse.duckdb' exists or run data refresh.")
        st.session_state.show_query_results = False

# --- Interactive Table Explorer Section ---
st.sidebar.divider()
st.sidebar.header("🔬 Interactive Table Explorer")
db_tables_list_for_explorer = [""] + get_db_table_list() 
st.session_state.selected_table_for_explorer = st.sidebar.selectbox(
    "Select a table to explore:", options=db_tables_list_for_explorer, index=0, key="sb_selected_table_explorer"
)

if st.sidebar.button("🔍 Explore Selected Table", disabled=(not st.session_state.selected_table_for_explorer)):
    if st.session_state.selected_table_for_explorer and DB_PATH.exists():
        table_to_explore = st.session_state.selected_table_for_explorer
        try:
            con = _connect(read_only=True) # Get cached connection
            base_query = "SELECT * FROM \"" + table_to_explore + "\""
            where_clauses = []
            current_db_tables = [t.lower() for t in get_db_table_list()] 
            table_columns_df = con.execute(f"PRAGMA table_info('{table_to_explore}')").df()
            table_columns = table_columns_df["name"].str.lower().tolist()

            if table_to_explore.lower() == "kns_faction" and "userfactioncoalitionstatus" in current_db_tables:
                base_query = """SELECT f.*, ufs.CoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition
                                 FROM KNS_Faction f LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID AND f.KnessetNum = ufs.KnessetNum"""
            elif table_to_explore.lower() == "kns_persontoposition" and "userfactioncoalitionstatus" in current_db_tables and "factionid" in table_columns and "knessetnum" in table_columns:
                base_query = """SELECT p2p.*, ufs.CoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition
                                 FROM KNS_PersonToPosition p2p LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum"""
                
            k_col_prefix = ""
            if table_to_explore.lower() == "kns_faction" and "userfactioncoalitionstatus" in current_db_tables:
                 k_col_prefix = "f."
            elif table_to_explore.lower() == "kns_persontoposition" and "userfactioncoalitionstatus" in current_db_tables:
                 k_col_prefix = "p2p." 
                
            # Use st.session_state.ms_knesset_filter for consistency
            if "knessetnum" in table_columns and st.session_state.ms_knesset_filter: 
                where_clauses.append(f"{k_col_prefix}KnessetNum IN ({', '.join(map(str, st.session_state.ms_knesset_filter))})")
            
            # Similarly for faction filter, if you decide to use st.session_state.ms_faction_filter
            # For now, selected_faction_ids_filter is derived locally, which is fine for this button's scope
            current_faction_ids_filter = [faction_display_map[name] for name in st.session_state.ms_faction_filter]
            if "factionid" in table_columns and current_faction_ids_filter: 
                where_clauses.append(f"{k_col_prefix}FactionID IN ({', '.join(map(str, current_faction_ids_filter))})")


            final_query = base_query
            if where_clauses:
                final_query += " WHERE " + " AND ".join(where_clauses)
            
            order_by = ""
            if "lastupdateddate" in table_columns:
                order_by = "ORDER BY LastUpdatedDate DESC"
            elif table_columns: # Check if table_columns is not empty
                order_by = f"ORDER BY \"{table_columns[0]}\" DESC" # Quote column name if it might have spaces/special chars
            
            final_query += f" {order_by} LIMIT 1000"
            
            st.session_state.table_explorer_df = con.sql(final_query).df()
            
            # These should only be set on successful execution of the query
            st.session_state.executed_table_explorer_name = table_to_explore
            st.session_state.show_table_explorer_results = True
            st.session_state.show_query_results = False 

        except Exception as e:
            ui_logger.error(f"Error exploring table '{table_to_explore}': {e}", exc_info=True)
            st.error(f"Error exploring table '{table_to_explore}': {e}")
            st.code(str(e) + "\n\n" + _format_exc())
            st.session_state.show_table_explorer_results = False # Reset on error
            st.session_state.table_explorer_df = pd.DataFrame() # Reset on error

    elif not st.session_state.selected_table_for_explorer: 
        st.warning("Please select a table to explore.")

# --- Data Filters (for Ad-hoc SQL & Table Explorer) ---
st.sidebar.divider()
# Updated header for filters
st.sidebar.header("📊 Filters (for Predefined Queries, Table Explorer & Ad-hoc SQL)")

# Ensure multiselects write to and read from session_state
st.session_state.ms_knesset_filter = st.sidebar.multiselect(
    "Knesset Number(s):", 
    options=knesset_nums_options, 
    default=st.session_state.get('ms_knesset_filter', []), # Initialize from session state
    key="ms_knesset_filter_widget" # Use a unique key for the widget itself
)
st.session_state.ms_faction_filter = st.sidebar.multiselect(
    "Faction(s):", 
    options=list(faction_display_map.keys()), 
    default=st.session_state.get('ms_faction_filter', []), # Initialize from session state
    key="ms_faction_filter_widget" # Use a unique key for the widget itself
)
# This derived list is fine as it's used immediately within the Table Explorer button logic
selected_faction_ids_filter = [faction_display_map[name] for name in st.session_state.ms_faction_filter]


# ──────────────────────────────────────────────────────────────────────────────
# Main area
# ──────────────────────────────────────────────────────────────────────────────
st.title("🇮🇱 Knesset Data Warehouse Console")

with st.expander("ℹ️ How This Works", expanded=False):
    st.markdown(dedent(f"""
        * **Data Refresh:** Use sidebar controls to fetch OData tables or update faction statuses from `{ft.FACTION_COALITION_STATUS_FILE.name}`.
        * **Predefined Queries:** Select a query from the sidebar. Apply optional Knesset Number filters, then click "Run". Results appear in "Query Results".
        * **Interactive Table Explorer:** Select a table from the sidebar, apply filters, click "Explore". Results appear in "Table Explorer Results".
        * **Ad-hoc SQL:** Use the sandbox at the bottom to run custom SQL.
    """))

# --- Query Results Area (for Predefined Queries) ---
st.divider()
st.header("📄 Predefined Query Results")

# Check conditions to display this section. Content moved inside this block.
if st.session_state.show_query_results and st.session_state.executed_query_name:
    st.subheader("Query Results") # Moved from outside
    subheader_text = f"Results for: {st.session_state.executed_query_name}"
    if st.session_state.applied_filters_info_query:
        subheader_text += f" (Filters: {'; '.join(st.session_state.applied_filters_info_query)})"
    st.markdown(subheader_text)

    if not st.session_state.query_results_df.empty:
        st.dataframe(st.session_state.query_results_df, use_container_width=True, height=400)
        # CSV and Excel Download Buttons
        safe_query_name_csv = re.sub(r'[^a-zA-Z0-9_\-]+', '_', st.session_state.executed_query_name)
        safe_query_name_excel = re.sub(r'[^a-zA-Z0-9_\-]+', '_', st.session_state.executed_query_name)

        col_csv, col_excel = st.columns(2)
        with col_csv:
            st.download_button(
                label="⬇️ Download Results (CSV)",
                data=st.session_state.query_results_df.to_csv(index=False).encode('utf-8-sig'),
                file_name=f"{safe_query_name_csv}_results.csv",
                mime="text/csv",
                key="csv_download_button_query"
            )
        with col_excel:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                st.session_state.query_results_df.to_excel(writer, index=False, sheet_name='Results')
            st.download_button(
                label="⬇️ Download Results (Excel)",
                data=excel_buffer.getvalue(),
                file_name=f"{safe_query_name_excel}_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="excel_download_button_query"
            )
    else:
        st.info("The query returned no results.")
    
    with st.expander("Show Executed SQL", expanded=False):
        st.code(st.session_state.last_executed_sql, language='sql')

# --- Table Explorer Results Area ---
st.divider()
st.header("📖 Interactive Table Explorer Results")

# Check conditions to display this section. Content moved inside this block.
if st.session_state.show_table_explorer_results and st.session_state.executed_table_explorer_name:
    st.subheader(f"Exploring: {st.session_state.executed_table_explorer_name}") # Moved from outside
    knesset_filter_display = st.session_state.get('ms_knesset_filter', [])
    faction_filter_display = st.session_state.get('ms_faction_filter', [])
    st.markdown(f"Filters Applied: Knesset(s): `{knesset_filter_display or 'All'}`, Faction(s): `{faction_filter_display or 'All'}`")

    if not st.session_state.table_explorer_df.empty:
        st.dataframe(st.session_state.table_explorer_df, use_container_width=True, height=400)
        # CSV and Excel Download Buttons for table explorer
        safe_table_name_csv = re.sub(r'[^a-zA-Z0-9_\-]+', '_', st.session_state.executed_table_explorer_name)
        safe_table_name_excel = re.sub(r'[^a-zA-Z0-9_\-]+', '_', st.session_state.executed_table_explorer_name)

        col_csv_table, col_excel_table = st.columns(2)
        with col_csv_table:
            st.download_button(
                label="⬇️ Download Table Data (CSV)",
                data=st.session_state.table_explorer_df.to_csv(index=False).encode('utf-8-sig'),
                file_name=f"{safe_table_name_csv}_table_data.csv",
                mime="text/csv",
                key="csv_download_button_table"
            )
        with col_excel_table:
            excel_buffer_table = io.BytesIO()
            with pd.ExcelWriter(excel_buffer_table, engine='openpyxl') as writer:
                st.session_state.table_explorer_df.to_excel(writer, index=False, sheet_name='TableData')
            st.download_button(
                label="⬇️ Download Table Data (Excel)",
                data=excel_buffer_table.getvalue(),
                file_name=f"{safe_table_name_excel}_table_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="excel_download_button_table"
            )
    else:
        st.info("The table exploration returned no results or no table was selected.")

# --- Ad-hoc SQL Query Section ---
with st.expander("🧑‍🔬 Run an Ad-hoc SQL Query (Advanced)", expanded=False):
    if not DB_PATH.exists(): st.warning("Database not found. Cannot run SQL queries.")
    else:
        st.markdown("Construct your SQL query. Use sidebar filters (Knesset Number, Faction) as reference for WHERE clauses.")
        default_sql_query = "SELECT t.table_name, t.row_count FROM duckdb_tables() t WHERE t.schema_name = 'main' ORDER BY t.table_name;"
        sql_query_input = st.text_area("Enter your SQL query:", default_sql_query, height=150, key="adhoc_sql_query" )
        if st.button("▶︎ Run Ad-hoc SQL", key="run_adhoc_sql"): 
            try:
                con = _connect(read_only=True) # Get cached connection
                adhoc_result_df = con.sql(sql_query_input).df()
                st.dataframe(adhoc_result_df, use_container_width=True)
                if not adhoc_result_df.empty:
                    st.download_button("⬇️ Download Ad-hoc (CSV)", adhoc_result_df.to_csv(index=False).encode('utf-8-sig'), "adhoc_results.csv", "text/csv", key="adhoc_csv_download" )
            except Exception as e:
                ui_logger.error(f"❌ SQL Query Error: {e}", exc_info=True)
                st.error(f"❌ SQL Query Error: {e}")
                st.code(str(e) + "\n\n" + _format_exc()) # Show full traceback in UI for debug

# --- Table Update Status (Moved to the bottom and put in an expander) ---
st.divider() 
with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
    if DB_PATH.exists():
        tables_to_check_status_main = sorted(list(set(TABLES)))
        status_data_main = []
        for t_name in tables_to_check_status_main:
            status_data_main.append({"Table": t_name, "Last Updated": _get_last_updated_from_db(t_name)})
        
        if status_data_main: # Check if list is not empty before creating DataFrame
            st.dataframe(pd.DataFrame(status_data_main), hide_index=True, use_container_width=True)
        else:
            st.info("No tables found to display status.")
    else: 
        st.info("Database not found. Table status cannot be displayed.")
