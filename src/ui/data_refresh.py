# ──────────────────────────────────────────────────────────────────────────────
#  src/ui/data_refresh.py  – self-service GUI for the researcher
#
#  Launch with:
#     streamlit run src/ui/data_refresh.py
#
#  Key features
#  ------------
#  ▸ Select which tables to refresh (or all of them)
#  ▸ Watch live progress + see "last updated" timestamps
#  ▸ Select and run predefined queries from the sidebar, with dynamic KnessetNum filtering.
#  ▸ Interactively explore raw database tables with filters.
#  ▸ Display results with download options.
#  ▸ (Optional) run ad-hoc SQL against the DuckDB warehouse
#
#  Dependencies:
#     pip install streamlit duckdb pandas openpyxl
# ──────────────────────────────────────────────────────────────────────────────
#
# ------------------------------------------------------------------------------------
# DEVELOPER INSTRUCTIONS: Applying Sidebar Filters to Predefined Queries
# ------------------------------------------------------------------------------------
#
# Request:
# The user wants the "Knesset Number(s)" filter selected in the sidebar to
# dynamically apply to the "Predefined Queries" when they are executed.
#
# Solution:
# This version modifies the application to inject Knesset Number filters into
# the predefined SQL queries before execution.
#
# Changes Made:
# 1. `EXPORTS` Dictionary Structure:
#    - Each entry in the `EXPORTS` dictionary is now a sub-dictionary containing:
#        - `"sql"`: The base SQL query string.
#        - `"knesset_filter_column"`: The fully qualified column name (e.g., "Q.KnessetNum",
#          "A.KnessetNum") in the base query that should be used for filtering by KnessetNum.
#
# 2. "Run Selected Query" Button Logic:
#    - When this button is clicked:
#        - It retrieves the base SQL and the `knesset_filter_column` from the `EXPORTS` entry.
#        - It checks `st.session_state.ms_knesset_filter` (the session state key for the
#          Knesset Number multiselect widget in the sidebar).
#        - If Knesset numbers are selected in the filter:
#            - A `WHERE` or `AND` clause for `knesset_filter_column IN (...)` is
#              dynamically constructed.
#            - This clause is inserted into the base SQL query *before* any existing
#              `ORDER BY` or `LIMIT` clauses using regular expressions for robust insertion.
#        - The (potentially modified) SQL query is then executed.
#        - The executed SQL (including the dynamic filter) is shown in the expander.
#
# 3. Sidebar Filter Header:
#    - The header for the filter section in the sidebar has been updated to
#      "📊 Filters (for Predefined Queries, Table Explorer & Ad-hoc SQL)"
#      to reflect its expanded role.
#
# 4. User Feedback in Main Area:
#    - When displaying results for a predefined query, if Knesset filters were
#      applied, this will be indicated in the subheader.
#
# Developer Actions:
#
# 1. Ensure this version of `data_refresh.py` is being used.
# 2. Review the updated structure of the `EXPORTS` dictionary. Ensure the
#    `knesset_filter_column` is correctly specified for each predefined query.
# 3. Test the functionality:
#    - Select a predefined query.
#    - Select one or more Knesset numbers from the sidebar filter.
#    - Click "▶️ Run Selected Query".
#    - Verify that the results are filtered by the selected Knesset numbers.
#    - Check the "Show Executed SQL" expander to see the modified query.
#    - Test with no Knesset numbers selected (should run the original query).
# 4. The Knesset Number filter widget already uses `key="ms_knesset_filter"`,
#    so its value is accessible via `st.session_state.ms_knesset_filter`.
#
# ------------------------------------------------------------------------------------

from __future__ import annotations

# ─── make sibling packages importable ─────────────────────────────────────────
import sys
import traceback # For more detailed error logging
from pathlib import Path
import io # For BytesIO
import re # For modifying SQL strings

ROOT = Path(__file__).resolve().parents[1]  #  …/src
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
# ──────────────────────────────────────────────────────────────────────────────

import asyncio
from datetime import datetime, timezone
from textwrap import dedent

import duckdb
import pandas as pd
import streamlit as st

# local modules  – keep alias `ft` for simplicity
import backend.fetch_table as ft # type: ignore

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


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Establishes a new connection to the DuckDB database."""
    if not DB_PATH.exists() and not read_only:
        st.info(f"Database {DB_PATH} does not exist. It will be created by DuckDB during write operation.")
    elif not DB_PATH.exists() and read_only:
        st.warning(f"Database {DB_PATH} does not exist. Please run a data refresh first. Query execution will fail.")
        return duckdb.connect(database=':memory:', read_only=True) 
    return duckdb.connect(database=DB_PATH.as_posix(), read_only=read_only)


def _human_ts(ts_value):
    """Converts a timestamp or datetime object to a human-readable UTC string."""
    if pd.isna(ts_value) or ts_value is None: return "never"
    try:
        if isinstance(ts_value, (datetime, pd.Timestamp)):
            dt_object = pd.to_datetime(ts_value) 
            dt_object = dt_object.tz_localize(None) if dt_object.tzinfo else dt_object 
            dt_object = dt_object.tz_localize(timezone.utc)
        else: 
            dt_object = pd.to_datetime(ts_value, unit='s', utc=True, errors='coerce')
            if pd.isna(dt_object): dt_object = pd.to_datetime(ts_value, utc=True, errors='coerce')
            if pd.isna(dt_object): return "invalid date"
        return dt_object.strftime("%Y-%m-%d %H:%M UTC")
    except Exception: return "conversion error"


def _get_last_updated_from_db(table_name: str) -> str:
    if not DB_PATH.exists(): return "never (DB not found)"
    try:
        with _connect(read_only=True) as con:
            tables_in_db_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main';").df()
            if table_name.lower() not in tables_in_db_df['table_name'].str.lower().tolist(): return "table not found"
            cols_df = con.execute(f"PRAGMA table_info('{table_name}');").df()
            if 'lastupdateddate' in cols_df['name'].str.lower().tolist():
                res = con.sql(f"SELECT MAX(LastUpdatedDate) FROM \"{table_name}\"").fetchone()
                return _human_ts(res[0]) if res and res[0] else "empty/no dates"
            elif table_name.lower() == "userfactioncoalitionstatus" and ft.FACTION_COALITION_STATUS_FILE.exists():
                return _human_ts(ft.FACTION_COALITION_STATUS_FILE.stat().st_mtime) + " (CSV mod time)"
            elif (PARQUET_DIR / f"{table_name}.parquet").exists():
                return _human_ts((PARQUET_DIR / f"{table_name}.parquet").stat().st_mtime) + " (Parquet mod)"
            return "no LastUpdatedDate"
    except Exception as e: return f"error ({type(e).__name__})"


@st.cache_data(ttl=3600) 
def get_db_table_list():
    """Fetches the list of all tables from the database."""
    if not DB_PATH.exists():
        return []
    try:
        with _connect(read_only=True) as con:
            return con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main' ORDER BY table_name;").df()["table_name"].tolist()
    except Exception as e:
        st.sidebar.error(f"Error fetching table list for explorer: {e}")
        return []

@st.cache_data(ttl=3600) 
def get_filter_options_from_db():
    knesset_nums, factions_df = [], pd.DataFrame(columns=["FactionID", "Name", "KnessetNum"])
    if not DB_PATH.exists(): return knesset_nums, factions_df
    try:
        with _connect(read_only=True) as con:
            tbls = get_db_table_list() 
            if "kns_knessetdates" in (t.lower() for t in tbls): knesset_nums = con.execute("SELECT DISTINCT KnessetNum FROM KNS_KnessetDates ORDER BY KnessetNum DESC").df()["KnessetNum"].tolist()
            elif "kns_faction" in (t.lower() for t in tbls): knesset_nums = con.execute("SELECT DISTINCT KnessetNum FROM KNS_Faction ORDER BY KnessetNum DESC").df()["KnessetNum"].tolist()
            if "kns_faction" in (t.lower() for t in tbls): factions_df = con.execute("SELECT FactionID, Name, KnessetNum FROM KNS_Faction ORDER BY KnessetNum DESC, Name").df()
    except Exception as e: st.sidebar.error(f"Filter options error: {e}")
    return knesset_nums, factions_df

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
st.sidebar.header("🔄 Data Refresh Controls")
all_available_tables = sorted(list(set(ft.TABLES + list(ft.CURSOR_TABLES.keys())))) 
selected_tables_to_refresh = st.sidebar.multiselect(
    "Select OData tables to refresh (blank = all predefined)", 
    all_available_tables, default=[], placeholder="All predefined tables"
)

if st.sidebar.button("🚀 Fetch Selected OData Tables"):
    tables_to_run = selected_tables_to_refresh if selected_tables_to_refresh else None
    st.sidebar.info(f"Refreshing: {'selected OData tables' if tables_to_run else 'All predefined OData tables'} and faction statuses...")
    progress_area_sidebar = st.sidebar.empty()
    def _sidebar_progress_cb(table_name, rows_done):
        progress_area_sidebar.write(f"✔ **{table_name}** – {rows_done:,} rows.")
    with st.spinner("Fetching data... Please wait."):
        try:
            asyncio.run(ft.refresh_tables(tables=tables_to_run, progress_cb=_sidebar_progress_cb, db_path=DB_PATH))
            st.sidebar.success("Data refresh process complete!")
            st.cache_data.clear(); st.rerun() 
        except Exception as e: st.sidebar.error(f"Refresh failed: {e}"); st.sidebar.code(traceback.format_exc()) 

if st.sidebar.button("🔄 Refresh Faction Status Only"):
    st.sidebar.info(f"Refreshing faction coalition statuses from {ft.FACTION_COALITION_STATUS_FILE.name}...")
    with st.spinner("Refreshing faction statuses..."):
        try:
            ft.load_and_store_faction_statuses(db_path=DB_PATH)
            st.sidebar.success("Faction coalition statuses refreshed!")
            st.cache_data.clear(); st.rerun()
        except Exception as e: st.sidebar.error(f"Faction status refresh failed: {e}"); st.sidebar.code(traceback.format_exc())

# --- Predefined Queries Section ---
st.sidebar.divider()
st.sidebar.header("🔎 Predefined Queries")
query_names_options = [""] + list(EXPORTS.keys()) 
st.session_state.selected_query_name = st.sidebar.selectbox(
    "Select a predefined query:", options=query_names_options, index=0, key="sb_selected_query_name"
)

if st.sidebar.button("▶️ Run Selected Query", disabled=(not st.session_state.selected_query_name)):
    if st.session_state.selected_query_name and DB_PATH.exists():
        query_info = EXPORTS[st.session_state.selected_query_name]
        base_sql = query_info["sql"]
        knesset_filter_col = query_info.get("knesset_filter_column") # Get the column to filter on
        
        modified_sql = base_sql
        applied_filters_info = []

        # Apply Knesset Number filter if selected and applicable to the query
        if knesset_filter_col and st.session_state.ms_knesset_filter:
            knesset_values_str = ", ".join(map(str, st.session_state.ms_knesset_filter))
            knesset_condition = f"{knesset_filter_col} IN ({knesset_values_str})"
            
            # Try to insert before ORDER BY or LIMIT
            order_by_match = re.search(r"\sORDER\s+BY\s", modified_sql, re.IGNORECASE)
            limit_match = re.search(r"\sLIMIT\s", modified_sql, re.IGNORECASE)
            
            insertion_point = len(modified_sql)
            if order_by_match: insertion_point = order_by_match.start()
            if limit_match and limit_match.start() < insertion_point: insertion_point = limit_match.start()

            main_query_part = modified_sql[:insertion_point]
            suffix_part = modified_sql[insertion_point:]

            if re.search(r"\sWHERE\s", main_query_part, re.IGNORECASE):
                modified_sql = f"{main_query_part.rstrip()} AND {knesset_condition} {suffix_part.lstrip()}"
            else:
                modified_sql = f"{main_query_part.rstrip()} WHERE {knesset_condition} {suffix_part.lstrip()}"
            applied_filters_info.append(f"Knesset(s): {st.session_state.ms_knesset_filter}")

        st.session_state.executed_sql_string = modified_sql # Store the actually executed SQL
        st.session_state.applied_knesset_filter_to_query = applied_filters_info


        try:
            with _connect(read_only=True) as con:
                st.session_state.query_results_df = con.sql(modified_sql).df()
            st.session_state.executed_query_name = st.session_state.selected_query_name
            st.session_state.show_query_results = True
            st.session_state.show_table_explorer_results = False 
        except Exception as e:
            st.error(f"Error executing predefined query '{st.session_state.selected_query_name}': {e}")
            st.code(traceback.format_exc())
            st.session_state.show_query_results = False
            st.session_state.query_results_df = pd.DataFrame()
    elif not st.session_state.selected_query_name: st.warning("Please select a query.")
    else: st.error("Database not found. Run data refresh."); st.session_state.show_query_results = False

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
            with _connect(read_only=True) as con:
                base_query = f"SELECT * FROM \"{table_to_explore}\""
                where_clauses = []
                current_db_tables = [t.lower() for t in get_db_table_list()] 
                table_columns_df = con.execute(f"PRAGMA table_info('{table_to_explore}')").df()
                table_columns = table_columns_df["name"].str.lower().tolist()

                if table_to_explore.lower() == "kns_faction" and "userfactioncoalitionstatus" in current_db_tables:
                    base_query = """SELECT f.*, ufs.FactionName AS UserProvidedFactionName, ufs.CoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition
                                    FROM KNS_Faction f LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID AND f.KnessetNum = ufs.KnessetNum"""
                elif table_to_explore.lower() == "kns_persontoposition" and "userfactioncoalitionstatus" in current_db_tables and "factionid" in table_columns and "knessetnum" in table_columns:
                    base_query = f"""SELECT p2p.*, ufs.CoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition
                                     FROM KNS_PersonToPosition p2p LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum"""
                
                k_col_prefix = "f." if table_to_explore.lower() == "kns_faction" and "userfactioncoalitionstatus" in current_db_tables else \
                               "p2p." if table_to_explore.lower() == "kns_persontoposition" and "userfactioncoalitionstatus" in current_db_tables else ""
                
                # Use st.session_state.ms_knesset_filter for consistency
                if "knessetnum" in table_columns and st.session_state.ms_knesset_filter: 
                    where_clauses.append(f"{k_col_prefix}KnessetNum IN ({', '.join(map(str, st.session_state.ms_knesset_filter))})")
                
                # Similarly for faction filter, if you decide to use st.session_state.ms_faction_filter
                # For now, selected_faction_ids_filter is derived locally, which is fine for this button's scope
                current_faction_ids_filter = [faction_display_map[name] for name in st.session_state.ms_faction_filter]
                if "factionid" in table_columns and current_faction_ids_filter: 
                    where_clauses.append(f"{k_col_prefix}FactionID IN ({', '.join(map(str, current_faction_ids_filter))})")


                final_query = base_query + (" WHERE " + " AND ".join(where_clauses) if where_clauses else "")
                order_by = "ORDER BY LastUpdatedDate DESC" if "lastupdateddate" in table_columns else ""
                if not order_by and table_columns: order_by = f"ORDER BY {table_columns[0]} DESC" 
                final_query += f" {order_by} LIMIT 1000"
                
                st.session_state.table_explorer_df = con.sql(final_query).df()
            
            st.session_state.executed_table_explorer_name = table_to_explore
            st.session_state.show_table_explorer_results = True
            st.session_state.show_query_results = False 
        except Exception as e:
            st.error(f"Error exploring table '{table_to_explore}': {e}")
            st.code(traceback.format_exc())
            st.session_state.show_table_explorer_results = False
            st.session_state.table_explorer_df = pd.DataFrame()
    elif not st.session_state.selected_table_for_explorer: 
        st.warning("Please select a table to explore.")
    else: 
        st.error("Database not found. Run data refresh.")
        st.session_state.show_table_explorer_results = False


# --- Data Filters (for Ad-hoc SQL & Table Explorer) ---
knesset_nums_options, factions_options_df = get_filter_options_from_db()
faction_display_map = {f"{row['Name']} (K{row['KnessetNum']})": row["FactionID"] for _, row in factions_options_df.iterrows()}
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
if st.session_state.show_query_results and st.session_state.executed_query_name:
    subheader_text = f"Results for: {st.session_state.executed_query_name}"
    if st.session_state.applied_knesset_filter_to_query:
        subheader_text += f" (Filtered by Knesset(s): {', '.join(map(str,st.session_state.applied_knesset_filter_to_query))})"
    st.subheader(subheader_text)

    with st.expander("Show Executed SQL", expanded=False):
        st.code(st.session_state.executed_sql_string, language="sql") # Show the modified SQL

    if not st.session_state.query_results_df.empty:
        st.dataframe(st.session_state.query_results_df, use_container_width=True, height=400)
        csv_export = st.session_state.query_results_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        excel_buffer = io.BytesIO()
        safe_sheet_name = "".join(c if c.isalnum() else "_" for c in st.session_state.executed_query_name)[:30] or "Export"
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer: st.session_state.query_results_df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
        excel_bytes_export = excel_buffer.getvalue()
        dl_col1, dl_col2 = st.columns(2)
        with dl_col1: st.download_button(f"⬇️ CSV '{st.session_state.executed_query_name}'", csv_export, f"{safe_sheet_name.lower()}.csv", "text/csv", key=f"csv_pq_{safe_sheet_name}")
        with dl_col2: st.download_button(f"⬇️ Excel '{st.session_state.executed_query_name}'", excel_bytes_export, f"{safe_sheet_name.lower()}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"xlsx_pq_{safe_sheet_name}")
    else: st.info("The selected predefined query returned no results with the current filters.")
elif st.session_state.selected_query_name and not st.session_state.show_query_results:
    if not st.session_state.show_table_explorer_results: 
        st.info(f"Click '▶️ Run Selected Query' in the sidebar to execute '{st.session_state.selected_query_name}'. Apply Knesset filters if needed.")
elif not st.session_state.show_table_explorer_results: 
    st.info("Select a predefined query from the sidebar and click 'Run Selected Query'.")


# --- Table Explorer Results Area ---
st.divider()
st.header("📖 Interactive Table Explorer Results")
if st.session_state.show_table_explorer_results and st.session_state.executed_table_explorer_name:
    st.subheader(f"Exploring Table: {st.session_state.executed_table_explorer_name}")
    # Use session state for filters here for consistency in display
    knesset_filter_display = st.session_state.get('ms_knesset_filter', [])
    faction_filter_display = st.session_state.get('ms_faction_filter', [])
    st.markdown(f"Filters Applied: Knesset(s): `{knesset_filter_display or 'All'}`, Faction(s): `{faction_filter_display or 'All'}`")

    if not st.session_state.table_explorer_df.empty:
        st.dataframe(st.session_state.table_explorer_df, use_container_width=True, height=400)
        csv_export_table = st.session_state.table_explorer_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        excel_buffer_table = io.BytesIO()
        safe_table_name = "".join(c if c.isalnum() else "_" for c in st.session_state.executed_table_explorer_name)[:30] or "Table"
        with pd.ExcelWriter(excel_buffer_table, engine="openpyxl") as writer: st.session_state.table_explorer_df.to_excel(writer, index=False, sheet_name=safe_table_name)
        excel_bytes_table = excel_buffer_table.getvalue()
        dl_t_col1, dl_t_col2 = st.columns(2)
        with dl_t_col1: st.download_button(f"⬇️ CSV '{st.session_state.executed_table_explorer_name}'", csv_export_table, f"explored_{safe_table_name.lower()}.csv", "text/csv", key=f"csv_te_{safe_table_name}")
        with dl_t_col2: st.download_button(f"⬇️ Excel '{st.session_state.executed_table_explorer_name}'", excel_bytes_table, f"explored_{safe_table_name.lower()}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"xlsx_te_{safe_table_name}")
    else: st.info("The table exploration returned no results with the current filters.")
elif st.session_state.selected_table_for_explorer and not st.session_state.show_table_explorer_results:
    if not st.session_state.show_query_results: 
         st.info(f"Click '🔍 Explore Selected Table' in the sidebar to view '{st.session_state.selected_table_for_explorer}'.")
elif not st.session_state.show_query_results: 
    st.info("Select a table from the sidebar and click 'Explore Selected Table'.")

# --- Ad-hoc SQL Query Section ---
with st.expander("🧑‍🔬 Run an Ad-hoc SQL Query (Advanced)", expanded=False):
    if not DB_PATH.exists(): st.warning("Database not found. Cannot run SQL queries.")
    else:
        st.markdown("Construct your SQL query. Use sidebar filters (Knesset Number, Faction) as reference for WHERE clauses.")
        default_sql_query = "SELECT t.table_name, t.row_count FROM duckdb_tables() t WHERE t.schema_name = 'main' ORDER BY t.table_name;"
        sql_query_input = st.text_area("Enter your SQL query:", default_sql_query, height=150, key="adhoc_sql_query" )
        if st.button("▶︎ Run Ad-hoc SQL", key="run_adhoc_sql"): 
            try:
                with _connect(read_only=True) as con: adhoc_result_df = con.sql(sql_query_input).df()
                st.dataframe(adhoc_result_df, use_container_width=True)
                if not adhoc_result_df.empty:
                    st.download_button("⬇️ Download Ad-hoc (CSV)", adhoc_result_df.to_csv(index=False).encode('utf-8-sig'), "adhoc_results.csv", "text/csv", key="adhoc_csv_download" )
            except Exception as e: st.error(f"❌ SQL Query Error: {e}"); st.code(traceback.format_exc())

# --- Table Update Status (Moved to the bottom and put in an expander) ---
st.divider() 
with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
    if DB_PATH.exists():
        tables_to_check_status_main = sorted(list(set(all_available_tables + ["UserFactionCoalitionStatus"]))) # Use a different variable name
        status_data_main = [{"Table": t_name, "Last Updated": _get_last_updated_from_db(t_name)} for t_name in tables_to_check_status_main]
        st.dataframe(pd.DataFrame(status_data_main), hide_index=True, use_container_width=True)
    else: 
        st.info("Database not found. Table status cannot be displayed.")
