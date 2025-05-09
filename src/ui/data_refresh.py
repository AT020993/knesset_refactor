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
#  ▸ Select and run predefined queries from the sidebar.
#  ▸ Interactively explore raw database tables with filters.
#  ▸ Display results with download options.
#  ▸ (Optional) run ad-hoc SQL against the DuckDB warehouse
#
#  Dependencies:
#     pip install streamlit duckdb pandas openpyxl
# ──────────────────────────────────────────────────────────────────────────────
#
# ------------------------------------------------------------------------------------
# DEVELOPER INSTRUCTIONS: Debugging "Interactive Table Explorer"
# ------------------------------------------------------------------------------------
#
# Problem:
# Clicking the "🔍 Explore Selected Table" button does not display the results
# in the "📖 Interactive Table Explorer Results" section as expected.
#
# Debugging Steps Added:
# 1. Inside the `if st.sidebar.button("🔍 Explore Selected Table"...)` block:
#    - A `st.sidebar.write` message to confirm the button click is registered.
#    - `st.sidebar.write` messages after session state variables
#      (`show_table_explorer_results`, `executed_table_explorer_name`,
#      `table_explorer_df.shape`) are set within the `try` block.
#    - A `st.sidebar.write` message within the `except` block to see if an
#      error during query execution is resetting the state.
# 2. At the beginning of the main area rendering (after `st.title`):
#    - `st.sidebar.write` messages to show the current values of
#      `show_table_explorer_results` and `executed_table_explorer_name`
#      just before the display logic for the table explorer results is evaluated.
#
# How to Use These Debug Steps:
#
# 1. Ensure this version of `data_refresh.py` is being used.
# 2. Run the Streamlit application: `streamlit run src/ui/data_refresh.py`.
# 3. Perform a hard refresh in your browser (`Ctrl+Shift+R` or `Cmd+Shift+R`).
# 4. In the sidebar:
#    a. Select a table from the "🔬 Interactive Table Explorer" dropdown (e.g., "KNS_Faction").
#    b. Click the "🔍 Explore Selected Table" button.
# 5. Observe the Streamlit Sidebar:
#    - You should see "DEBUG: 'Explore Selected Table' button clicked."
#    - Then, messages showing the state of `show_table_explorer_results`,
#      `executed_table_explorer_name`, and the shape of the resulting DataFrame.
#    - Also, observe the debug messages that appear from the main area rendering
#      (these will appear on every script run, including after the button click).
#
# Analyze and Report:
# - Does the "button clicked" message appear?
# - After clicking, do the "Inside button try" debug messages show that
#   `show_table_explorer_results` is `True` and `executed_table_explorer_name`
#   is set to the table you selected? Is `table_explorer_df.shape` showing a
#   non-empty DataFrame (e.g., (X rows, Y columns))?
# - What are the values of `show_table_explorer_results` and
#   `executed_table_explorer_name` shown by the "Main area render" debug
#   messages immediately after the button click causes the script to rerun?
#
# This will help determine if the state is being set correctly by the button
# and if it persists for the display logic.
#
# ------------------------------------------------------------------------------------

from __future__ import annotations

# ─── make sibling packages importable ─────────────────────────────────────────
import sys
import traceback # For more detailed error logging
from pathlib import Path
import io # For BytesIO

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
if 'query_results_df' not in st.session_state:
    st.session_state.query_results_df = pd.DataFrame()
if 'show_query_results' not in st.session_state:
    st.session_state.show_query_results = False

# For Interactive Table Explorer
if 'selected_table_for_explorer' not in st.session_state:
    st.session_state.selected_table_for_explorer = None
if 'executed_table_explorer_name' not in st.session_state:
    st.session_state.executed_table_explorer_name = None
if 'table_explorer_df' not in st.session_state:
    st.session_state.table_explorer_df = pd.DataFrame()
if 'show_table_explorer_results' not in st.session_state:
    st.session_state.show_table_explorer_results = False


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
            tbls = get_db_table_list() # Use cached table list
            if "kns_knessetdates" in (t.lower() for t in tbls): knesset_nums = con.execute("SELECT DISTINCT KnessetNum FROM KNS_KnessetDates ORDER BY KnessetNum DESC").df()["KnessetNum"].tolist()
            elif "kns_faction" in (t.lower() for t in tbls): knesset_nums = con.execute("SELECT DISTINCT KnessetNum FROM KNS_Faction ORDER BY KnessetNum DESC").df()["KnessetNum"].tolist()
            if "kns_faction" in (t.lower() for t in tbls): factions_df = con.execute("SELECT FactionID, Name, KnessetNum FROM KNS_Faction ORDER BY KnessetNum DESC, Name").df()
    except Exception as e: st.sidebar.error(f"Filter options error: {e}")
    return knesset_nums, factions_df

# ──────────────────────────────────────────────────────────────────────────────
# EXPORTS Dictionary - Source for Predefined Queries
# ──────────────────────────────────────────────────────────────────────────────
EXPORTS = {
    "Factions Detailed Status": """
        SELECT 
            f.FactionID, f.Name AS OfficialFactionName, f.KnessetNum,
            ufs.FactionName AS UserProvidedFactionName, ufs.CoalitionStatus,
            ufs.DateJoinedCoalition, ufs.DateLeftCoalition,
            strftime(CAST(f.StartDate AS TIMESTAMP), '%Y-%m-%d') AS FactionStartDateInKnesset,
            strftime(CAST(f.FinishDate AS TIMESTAMP), '%Y-%m-%d') AS FactionFinishDateInKnesset,
            f.IsCurrent AS IsFactionCurrentlyActiveInAPI 
        FROM KNS_Faction f
        LEFT JOIN UserFactionCoalitionStatus ufs 
            ON f.FactionID = ufs.FactionID AND f.KnessetNum = ufs.KnessetNum
        ORDER BY f.KnessetNum DESC, f.Name;
    """,
    "Queries + Full Details": """
        SELECT
            Q.QueryID, Q.Number, Q.KnessetNum, Q.Name AS QueryName, Q.TypeID AS QueryTypeID, Q.TypeDesc AS QueryTypeDesc,
            Q.StatusID AS QueryStatusID, S.Desc AS QueryStatusDesc,
            Q.PersonID AS MKPersonID, P.FirstName AS MKFirstName, P.LastName AS MKLastName, P.GenderDesc AS MKGender,
            P2P.FactionName AS MKFactionName, P2P.FactionID AS MKFactionID,
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
        ORDER BY Q.KnessetNum DESC, Q.QueryID DESC
        LIMIT 10000;
    """
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
        selected_sql = EXPORTS[st.session_state.selected_query_name]
        try:
            with _connect(read_only=True) as con:
                st.session_state.query_results_df = con.sql(selected_sql).df()
            st.session_state.executed_query_name = st.session_state.selected_query_name
            st.session_state.show_query_results = True
            st.session_state.show_table_explorer_results = False # Hide table explorer results
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
db_tables_list_for_explorer = [""] + get_db_table_list() # Add blank option
st.session_state.selected_table_for_explorer = st.sidebar.selectbox(
    "Select a table to explore:", options=db_tables_list_for_explorer, index=0, key="sb_selected_table_explorer"
)

if st.sidebar.button("🔍 Explore Selected Table", disabled=(not st.session_state.selected_table_for_explorer)):
    # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
    # DEBUGGING LINE 1: Confirm button click is registered
    # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
    st.sidebar.write("DEBUG: 'Explore Selected Table' button clicked.")
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
                
                if "knessetnum" in table_columns and selected_knessets_filter: 
                    where_clauses.append(f"{k_col_prefix}KnessetNum IN ({', '.join(map(str, selected_knessets_filter))})")
                if "factionid" in table_columns and selected_faction_ids_filter: 
                    where_clauses.append(f"{k_col_prefix}FactionID IN ({', '.join(map(str, selected_faction_ids_filter))})")

                final_query = base_query + (" WHERE " + " AND ".join(where_clauses) if where_clauses else "")
                order_by = "ORDER BY LastUpdatedDate DESC" if "lastupdateddate" in table_columns else ""
                if not order_by and table_columns: order_by = f"ORDER BY {table_columns[0]} DESC" 
                final_query += f" {order_by} LIMIT 1000"
                
                st.session_state.table_explorer_df = con.sql(final_query).df()
            
            st.session_state.executed_table_explorer_name = table_to_explore
            st.session_state.show_table_explorer_results = True
            st.session_state.show_query_results = False 
            # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
            # DEBUGGING LINES 2: Check state after successful execution
            # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
            st.sidebar.write(f"DEBUG: Inside button try - show_table_explorer_results: {st.session_state.show_table_explorer_results}")
            st.sidebar.write(f"DEBUG: Inside button try - executed_table_explorer_name: {st.session_state.executed_table_explorer_name}")
            st.sidebar.write(f"DEBUG: Inside button try - table_explorer_df shape: {st.session_state.table_explorer_df.shape}")
        except Exception as e:
            st.error(f"Error exploring table '{table_to_explore}': {e}")
            st.code(traceback.format_exc())
            st.session_state.show_table_explorer_results = False
            st.session_state.table_explorer_df = pd.DataFrame()
            # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
            # DEBUGGING LINE 3: Check state after exception
            # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
            st.sidebar.write(f"DEBUG: Inside button except - show_table_explorer_results: {st.session_state.show_table_explorer_results}")
    elif not st.session_state.selected_table_for_explorer: 
        st.warning("Please select a table to explore.")
    else: 
        st.error("Database not found. Run data refresh.")
        st.session_state.show_table_explorer_results = False


# --- Data Filters (for Ad-hoc SQL & Table Explorer) ---
knesset_nums_options, factions_options_df = get_filter_options_from_db()
faction_display_map = {f"{row['Name']} (K{row['KnessetNum']})": row["FactionID"] for _, row in factions_options_df.iterrows()}
st.sidebar.divider()
st.sidebar.header("📊 Filters (for Table Explorer & Ad-hoc SQL)")
selected_knessets_filter = st.sidebar.multiselect("Knesset Number(s):", options=knesset_nums_options, default=[], key="ms_knesset_filter")
selected_faction_names_filter = st.sidebar.multiselect("Faction(s):", options=list(faction_display_map.keys()), default=[], key="ms_faction_filter")
selected_faction_ids_filter = [faction_display_map[name] for name in selected_faction_names_filter]

# ──────────────────────────────────────────────────────────────────────────────
# Main area
# ──────────────────────────────────────────────────────────────────────────────
st.title("🇮🇱 Knesset Data Warehouse Console")
# VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
# DEBUGGING LINES 4: Check state at the start of main area render
# VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
st.sidebar.write(f"DEBUG: Main area render - show_table_explorer_results: {st.session_state.get('show_table_explorer_results', 'Not Set')}")
st.sidebar.write(f"DEBUG: Main area render - executed_table_explorer_name: {st.session_state.get('executed_table_explorer_name', 'Not Set')}")
st.sidebar.write(f"DEBUG: Main area render - show_query_results: {st.session_state.get('show_query_results', 'Not Set')}")
st.sidebar.write(f"DEBUG: Main area render - executed_query_name: {st.session_state.get('executed_query_name', 'Not Set')}")


st.subheader("🗓️ Table Update Status")
if DB_PATH.exists():
    tables_to_check_status = sorted(list(set(all_available_tables + ["UserFactionCoalitionStatus"])))
    status_data = [{"Table": t_name, "Last Updated": _get_last_updated_from_db(t_name)} for t_name in tables_to_check_status]
    st.dataframe(pd.DataFrame(status_data), hide_index=True, use_container_width=True)
else: st.info("Database not found. Please run a data refresh from the sidebar.")

with st.expander("ℹ️ How This Works", expanded=False):
    st.markdown(dedent(f"""
        * **Data Refresh:** Use sidebar controls to fetch OData tables or update faction statuses from `{ft.FACTION_COALITION_STATUS_FILE.name}`.
        * **Predefined Queries:** Select a query from the sidebar, click "Run". Results appear in "Query Results".
        * **Interactive Table Explorer:** Select a table from the sidebar, apply filters, click "Explore". Results appear in "Table Explorer Results".
        * **Ad-hoc SQL:** Use the sandbox at the bottom to run custom SQL.
    """))

# --- Query Results Area (for Predefined Queries) ---
st.divider()
st.header("📄 Predefined Query Results")
if st.session_state.show_query_results and st.session_state.executed_query_name:
    st.subheader(f"Results for: {st.session_state.executed_query_name}")
    with st.expander("Show Executed SQL", expanded=False):
        st.code(EXPORTS[st.session_state.executed_query_name], language="sql")
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
    else: st.info("The selected predefined query returned no results.")
elif st.session_state.selected_query_name and not st.session_state.show_query_results:
    st.info(f"Click '▶️ Run Selected Query' in the sidebar to execute '{st.session_state.selected_query_name}'.")
else: st.info("Select a predefined query from the sidebar and click 'Run Selected Query'.")

# --- Table Explorer Results Area ---
st.divider()
st.header("📖 Interactive Table Explorer Results")
if st.session_state.show_table_explorer_results and st.session_state.executed_table_explorer_name:
    st.subheader(f"Exploring Table: {st.session_state.executed_table_explorer_name}")
    st.markdown(f"Filters Applied: Knesset(s): `{selected_knessets_filter or 'All'}`, Faction(s): `{selected_faction_names_filter or 'All'}`")
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
     st.info(f"Click '🔍 Explore Selected Table' in the sidebar to view '{st.session_state.selected_table_for_explorer}'.")
else: st.info("Select a table from the sidebar and click 'Explore Selected Table'.")

# ──────────────────────────────────────────────────────────────────────────────
# Ad-hoc SQL Query Section
# ──────────────────────────────────────────────────────────────────────────────
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
