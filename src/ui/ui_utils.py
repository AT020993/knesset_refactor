from __future__ import annotations

# Standard Library Imports
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Third-Party Imports
import duckdb
import pandas as pd
import streamlit as st # For st.cache_resource, st.cache_data, st.error, st.info, st.warning

# Add the 'src' directory to sys.path if needed for other utils, though not strictly for these functions
_CURRENT_FILE_DIR = Path(__file__).resolve().parent
_SRC_DIR = _CURRENT_FILE_DIR.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# --- Database Connection and Utility Functions ---
# REMOVED @st.cache_resource(ttl=300) - This was causing issues with closed connections being reused.
# Each function will now get a fresh connection and manage its lifecycle.
def connect_db(db_path: Path, read_only: bool = True, _logger_obj: logging.Logger | None = None) -> duckdb.DuckDBPyConnection:
    """Establishes a new connection to the DuckDB database."""
    if not db_path.exists() and not read_only:
        if _logger_obj: _logger_obj.info(f"Database {db_path} does not exist. It will be created.")
        st.info(f"Database {db_path} does not exist. It will be created by DuckDB during write operation.")
    elif not db_path.exists() and read_only:
        if _logger_obj: _logger_obj.warning(f"Database {db_path} does not exist. Query execution will fail.")
        st.warning(f"Database {db_path} does not exist. Please run a data refresh first. Query execution will fail.")
        return duckdb.connect(database=":memory:", read_only=True)
    try:
        con = duckdb.connect(database=db_path.as_posix(), read_only=read_only)
        con.execute("SELECT 1") # Test connection
        if _logger_obj: _logger_obj.debug(f"Successfully connected to DuckDB at {db_path} (read_only={read_only}).")
        return con
    except Exception as e:
        if _logger_obj: _logger_obj.error(f"Error connecting to database at {db_path}: {e}", exc_info=True)
        st.error(f"Database connection error: {e}")
        return duckdb.connect(database=":memory:", read_only=True) # Fallback

def safe_execute_query(con: duckdb.DuckDBPyConnection, query: str, _logger_obj: logging.Logger | None = None) -> pd.DataFrame:
    """Safely execute a query with proper error handling."""
    try:
        if _logger_obj: _logger_obj.debug(f"Executing query: {query[:200]}...") # Log snippet
        return con.execute(query).df()
    except Exception as e:
        if _logger_obj: _logger_obj.error(f"Query execution error: {e}\nQuery: {query}", exc_info=True)
        st.error(f"Query execution error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_db_table_list(db_path: Path, _logger_obj: logging.Logger | None = None) -> list[str]:
    """Fetches the list of all tables from the database."""
    if _logger_obj: _logger_obj.info("Fetching database table list...")
    if not db_path.exists():
        if _logger_obj: _logger_obj.warning("Database file not found. Returning empty table list.")
        return []
    
    con = None
    try:
        con = connect_db(db_path, read_only=True, _logger_obj=_logger_obj)
        tables_df = safe_execute_query(con, "SHOW TABLES;", _logger_obj=_logger_obj)
        table_list = sorted(tables_df["name"].tolist()) if not tables_df.empty else []
        if _logger_obj: _logger_obj.info(f"Database table list fetched: {len(table_list)} tables.")
        return table_list
    except Exception as e:
        if _logger_obj: _logger_obj.error(f"Error in get_db_table_list: {e}", exc_info=True)
        st.sidebar.error(f"DB error listing tables: {e}", icon="🔥") 
        return []
    finally:
        if con:
            con.close()
            if _logger_obj: _logger_obj.debug("Connection closed in get_db_table_list.")


@st.cache_data(ttl=3600)
def get_table_columns(db_path: Path, table_name: str, _logger_obj: logging.Logger | None = None) -> tuple[list[str], list[str], list[str]]:
    """Fetches all column names, numeric column names, and categorical column names for a table."""
    if not table_name or not db_path.exists():
        return [], [], []
    
    con = None
    try:
        con = connect_db(db_path, read_only=True, _logger_obj=_logger_obj)
        columns_df = safe_execute_query(con, f"PRAGMA table_info('{table_name}');", _logger_obj=_logger_obj)

        if columns_df.empty:
            return [], [], []

        all_cols = columns_df["name"].tolist()
        numeric_cols = columns_df[
            columns_df["type"].str.contains(
                "INTEGER|FLOAT|DOUBLE|DECIMAL|NUMERIC|BIGINT|SMALLINT|TINYINT|REAL|NUMBER",
                case=False,
                na=False,
            )
        ]["name"].tolist()
        categorical_cols = [col for col in all_cols if col not in numeric_cols]
        if _logger_obj: _logger_obj.debug(f"Fetched columns for table '{table_name}': All({len(all_cols)}), Num({len(numeric_cols)}), Cat({len(categorical_cols)})")
        return all_cols, numeric_cols, categorical_cols
    except Exception as e:
        if _logger_obj: _logger_obj.error(f"Error getting columns for table {table_name}: {e}", exc_info=True)
        return [], [], []
    finally:
        if con:
            con.close()
            if _logger_obj: _logger_obj.debug(f"Connection closed in get_table_columns for table {table_name}.")


@st.cache_data(ttl=3600)
def get_filter_options_from_db(db_path: Path, _logger_obj: logging.Logger | None = None) -> tuple[list, pd.DataFrame]:
    """Fetches distinct Knesset numbers and faction data for filter dropdowns."""
    if _logger_obj: _logger_obj.info("Fetching filter options from database...")
    if not db_path.exists():
        if _logger_obj: _logger_obj.warning("Database file not found. Returning empty filter options.")
        return [], pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])
    
    con = None
    try:
        con = connect_db(db_path, read_only=True, _logger_obj=_logger_obj) 
        knesset_nums_df = safe_execute_query(con, "SELECT DISTINCT KnessetNum FROM KNS_KnessetDates ORDER BY KnessetNum DESC;", _logger_obj)
        knesset_nums_options = sorted(knesset_nums_df["KnessetNum"].unique().tolist(), reverse=True) if not knesset_nums_df.empty else []

        db_tables_df = safe_execute_query(con, "SELECT table_name FROM duckdb_tables() WHERE schema_name='main';", _logger_obj)
        db_tables_list = db_tables_df["table_name"].str.lower().tolist() if not db_tables_df.empty else []


        factions_query = ""
        if "userfactioncoalitionstatus" in db_tables_list and "kns_faction" in db_tables_list:
            factions_query = """
                SELECT DISTINCT COALESCE(ufcs.FactionName, kf.Name) AS FactionName, kf.FactionID, kf.KnessetNum
                FROM KNS_Faction AS kf
                LEFT JOIN UserFactionCoalitionStatus AS ufcs ON kf.FactionID = ufcs.FactionID AND kf.KnessetNum = ufcs.KnessetNum
                ORDER BY FactionName;
            """
        elif "kns_faction" in db_tables_list:
            if _logger_obj: _logger_obj.info("UserFactionCoalitionStatus table not found, fetching faction names from KNS_Faction.")
            factions_query = "SELECT DISTINCT Name AS FactionName, FactionID, KnessetNum FROM KNS_Faction ORDER BY FactionName;"
        else:
            if _logger_obj: _logger_obj.warning("KNS_Faction table not found. Cannot fetch faction filter options.")
            return knesset_nums_options, pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])

        factions_df = safe_execute_query(con, factions_query, _logger_obj)
        if _logger_obj: _logger_obj.info(f"Filter options fetched: {len(knesset_nums_options)} Knesset Nums, {len(factions_df)} Factions.")
        return knesset_nums_options, factions_df
    except Exception as e:
        if _logger_obj: _logger_obj.error(f"Error in get_filter_options_from_db: {e}", exc_info=True)
        return [], pd.DataFrame(columns=["FactionName", "FactionID", "KnessetNum"])
    finally:
        if con:
            con.close()
            if _logger_obj: _logger_obj.debug("Connection closed in get_filter_options_from_db.")


def format_exception_for_ui(exc_info=None):
    """Formats an exception for display in the UI, similar to logger."""
    if exc_info is None:
        exc_info = sys.exc_info()
    if exc_info[0] is None:
        return "No exception information available."
    return f"{exc_info[0].__name__}: {exc_info[1]}"


def human_readable_timestamp(ts_value, _logger_obj: logging.Logger | None = None) -> str: 
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
        if _logger_obj: _logger_obj.warning(f"Could not parse timestamp '{ts_value}': {e}")
        return str(ts_value) 

def get_last_updated_for_table(parquet_dir: Path, table_name: str, _logger_obj: logging.Logger | None = None) -> str: 
    """Gets the last updated timestamp for a table (Parquet file modification time)."""
    parquet_file = parquet_dir / f"{table_name}.parquet"
    if parquet_file.exists():
        try:
            return human_readable_timestamp(parquet_file.stat().st_mtime, _logger_obj)
        except Exception as e:
            if _logger_obj: _logger_obj.warning(f"Could not get mod_time for {parquet_file}: {e}")
            return "Error reading timestamp"
    return "Never (or N/A)"

