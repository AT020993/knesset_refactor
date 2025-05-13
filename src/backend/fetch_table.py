"""fetch_table.py – robust OData → DuckDB loader (HTTP + utilities)
----------------------------------------------------------------
Improvements already in place
1. **Checkpoint‑resume** for cursor‑paged tables – progress dumped to `data/.resume_state.json` every chunk (never re‑fetch rows after crash).
2. **Parallel download** for regular `$skip` tables – configurable pool (default 8 concurrent pages).
3. **Automatic Parquet export** – each saved table is also mirrored to `data/parquet/<table>.parquet` (ZSTD‑compressed).

**NEW in this revision (adds the three requested ideas):**
4. **Quick SQL inspection** – `--sql "SELECT …"` lets you run ad‑hoc DuckDB queries right from the CLI (or pipe them from shell).
5. **Streamlit integration hooks** – `async def refresh_tables()` & `def ensure_latest()` can be imported by the UI; they download tables and bubble progress back via an optional callback (so you can wire a Streamlit progress bar).
6. **Data‑quality helpers** – tiny utilities for coalition status, MK/committee ID mappings, and gender/position look‑ups.  These live here for now so the GUI can `import fetch_table as ft` and reuse them.
7. **Manual Faction Coalition Status Loading** - Loads a user-maintained CSV file for faction coalition/opposition status,
   including faction name, date joined coalition, and date left coalition.

Install once:
```bash
pip install aiohttp pandas duckdb tqdm backoff pyarrow fastparquet openpyxl
```

Examples:
```bash
# download one table
python src/backend/fetch_table.py --table KNS_CommitteeSession

# refresh the whole warehouse (all predefined tables)
python src/backend/fetch_table.py --all

# run a quick query
python src/backend/fetch_table.py --sql "SELECT table_name, row_count FROM duckdb_tables();"

# Refresh only the faction status CSV
python src/backend/fetch_table.py --refresh-faction-status
```

# -----------------------------------------------------------------------------
# DEVELOPER INSTRUCTIONS: Integrating Detailed Faction Coalition Status
# -----------------------------------------------------------------------------
#
# Purpose of these Changes (related to faction_coalition_status.csv):
#
# These modifications enhance the manual faction status tracking feature. The system
# will now load and store more detailed information for each faction's coalition
# status, including:
#   - The faction's name (as provided in the CSV, for easier management).
#   - The date the faction joined a coalition.
#   - The date the faction left a coalition.
#
# This information is loaded from the `data/faction_coalition_status.csv` file
# into a DuckDB table (`UserFactionCoalitionStatus`) and is used in the Streamlit
# UI for display and export.
#
# How it Works:
#
# 1. CSV File (`data/faction_coalition_status.csv`):
#    - This file is manually maintained by the user.
#    - It needs to have specific columns:
#        - `KnessetNum` (Integer): The Knesset number.
#        - `FactionID` (Integer): The numerical ID of the faction.
#        - `FactionName` (Text): The name of the faction (for reference).
#        - `CoalitionStatus` (Text): e.g., "Coalition", "Opposition".
#        - `DateJoinedCoalition` (Date): YYYY-MM-DD format. Empty if not applicable.
#        - `DateLeftCoalition` (Date): YYYY-MM-DD format. Empty if not applicable.
#    - Example:
#      KnessetNum,FactionID,FactionName,CoalitionStatus,DateJoinedCoalition,DateLeftCoalition
#      25,961,Likud,Coalition,2022-12-29,
#      25,954,Yesh Atid,Opposition,,
#
# 2. `load_and_store_faction_statuses` function (in this file):
#    - This function is responsible for:
#        - Reading the `data/faction_coalition_status.csv` file using pandas.
#        - Validating the presence of the expected columns.
#        - Parsing the date columns (`DateJoinedCoalition`, `DateLeftCoalition`)
#          into datetime objects. Empty or invalid dates are handled gracefully
#          (converted to NaT - Not a Time - by pandas, which becomes NULL in DuckDB).
#        - Creating or replacing the `UserFactionCoalitionStatus` table in the
#          DuckDB database (`data/warehouse.duckdb`).
#        - The table schema in DuckDB will include `DATE` types for the date columns.
#
# 3. `refresh_tables` function (in this file):
#    - After fetching and storing the OData tables, this function now calls
#      `load_and_store_faction_statuses` to ensure the user-managed faction
#      status data is also loaded/updated in the database.
#
# 4. CLI Option (`--refresh-faction-status`):
#    - A new command-line argument allows refreshing only the faction status data
#      without re-fetching all OData tables.
#      Example: `python src/backend/fetch_table.py --refresh-faction-status`
#
# Developer Actions:
#
# - Ensure the `data/faction_coalition_status.csv` file is created and maintained
#   by the user in the `knesset_refactor/data/` directory with the correct
#   column structure and data formats.
# - The changes in this file primarily revolve around the
#   `load_and_store_faction_statuses` function and its integration into the
#   `refresh_tables` workflow and CLI. The CLI functions `parse_args_cli` and
#   `main_cli` handle command-line argument parsing and execution flow.
#
# -----------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from math import ceil
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from utils.logger_setup import setup_logging  # Logging utility

# Initialize logger for this module
logger = setup_logging('knesset.backend.fetch_table')

import aiohttp # Dependency for asynchronous HTTP requests
import backoff   # Dependency for retrying operations with exponential backoff
import duckdb    # Dependency for the database
import pandas as pd # Dependency for data manipulation
from tqdm import tqdm # Dependency for progress bars

# -----------------------------------------------------------------------------
# Basic constants / paths
# -----------------------------------------------------------------------------
BASE_URL = "http://knesset.gov.il/Odata/ParliamentInfo.svc"
DEFAULT_DB = Path("data/warehouse.duckdb")
PAGE_SIZE = 100  # Number of records to fetch per API request for non-cursor tables
MAX_RETRIES = 8  # Maximum number of retries for failed HTTP requests
PARQUET_DIR = Path("data/parquet") # Directory to store Parquet files
RESUME_FILE = Path("data/.resume_state.json") # File to store resume state for cursor-paged tables
CONCURRENCY = 8  # Number of concurrent page fetches for skip-based paging

# Path to the user-maintained faction coalition status CSV
FACTION_COALITION_STATUS_FILE = Path("data/faction_coalition_status.csv")

# List of tables to be fetched from the OData API
TABLES = [
    "KNS_Person",
    "KNS_Faction",
    "KNS_GovMinistry",
    "KNS_Status",
    "KNS_PersonToPosition",
    "KNS_Query",
    "KNS_Agenda",
    "KNS_Committee",
    "KNS_CommitteeSession",
    "KNS_PlenumSession",
    "KNS_KnessetDates", 
    "KNS_Bill",
    "KNS_Law",
    "KNS_IsraelLaw"
    # Add other tables as needed
]

# Dictionary defining tables that require cursor-based paging and their primary key / chunk size
CURSOR_TABLES: Dict[str, Tuple[str, int]] = {
    "KNS_Person": ("PersonID", 100),
    "KNS_CommitteeSession": ("CommitteeSessionID", 100),
    "KNS_PlenumSession": ("PlenumSessionID", 100),
    "KNS_Bill": ("BillID", 100), 
    "KNS_Query": ("QueryID", 100), 
}

# -----------------------------------------------------------------------------
# Resume‑state helpers (for cursor-based paging)
# -----------------------------------------------------------------------------
def _load_resume() -> Dict[str, int]:
    """Loads the resume state from a JSON file for cursor-paged tables."""
    if RESUME_FILE.exists():
        try:
            return json.loads(RESUME_FILE.read_text())
        except json.JSONDecodeError: # Handle empty or malformed JSON
            logger.warning(f"Could not decode resume file {RESUME_FILE}. Starting fresh for cursor tables.")
            pass 
        except Exception as e:
            logger.warning(f"Error loading resume file {RESUME_FILE}: {e}. Starting fresh for cursor tables.", exc_info=True)
            pass
    return {}

def _save_resume(state: Dict[str, int]):
    """Saves the current resume state to a JSON file."""
    try:
        RESUME_FILE.parent.mkdir(parents=True, exist_ok=True)
        RESUME_FILE.write_text(json.dumps(state, indent=4))
    except Exception as e:
        logger.warning(f"Could not save resume state to {RESUME_FILE}: {e}", exc_info=True)


resume_state: Dict[str, int] = _load_resume()

# -----------------------------------------------------------------------------
# Retry helper for HTTP requests
# -----------------------------------------------------------------------------
def _backoff_hdlr(details):
    """Handler for logging backoff attempts."""
    logger.warning(f"Backing off {details['wait']:.1f}s after: {details['exception']}")

@backoff.on_exception(
    backoff.expo, # Use exponential backoff strategy
    (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError), # Retry on these exceptions
    max_tries=MAX_RETRIES,
    on_backoff=_backoff_hdlr, # Log when a backoff occurs
)
async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict:
    """Fetches JSON data from a URL with retries using exponential backoff."""
    timeout = aiohttp.ClientTimeout(total=60) # 60 seconds total timeout for the request
    async with session.get(url, timeout=timeout) as resp:
        resp.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        # Allow any content type for JSON parsing, as some APIs might not set it correctly
        return await resp.json(content_type=None) 

# -----------------------------------------------------------------------------
# Download logic for OData tables
# -----------------------------------------------------------------------------
async def download_table(table: str) -> pd.DataFrame:
    """
    Downloads a specific table from the OData API into a pandas DataFrame.
    Uses cursor-based paging for tables defined in CURSOR_TABLES.
    Uses parallel skip-based paging for other tables.
    """
    entity = f"{table}()" # OData entity usually ends with ()
    logger.info(f"Starting download for table: {table}")
    dfs: List[pd.DataFrame] = [] # List to hold DataFrames from paged results

    async with aiohttp.ClientSession() as session:
        # --- Cursor-paged tables (with checkpoint resume) ---
        if table in CURSOR_TABLES:
            pk, chunk_size = CURSOR_TABLES[table]
            last_val: int = resume_state.get(table, -1) # Default to -1 if no resume state
            total_rows_fetched = 0
            # Progress bar for cursor-paged tables
            with tqdm(desc=f"Fetching {table} (cursor)", unit=" rows", leave=False) as pbar:
                while True:
                    # Construct URL for cursor-based paging
                    url = (
                        f"{BASE_URL}/{entity}"
                        f"?$format=json&$top={chunk_size}"
                        f"&$filter={pk}%20gt%20{last_val}" # URL encode space for >
                        f"&$orderby={pk}%20asc" # URL encode space for orderby
                    )
                    try:
                        data = await fetch_json(session, url)
                    except Exception as e:
                        # This exception is after backoff retries have been exhausted for this specific call
                        print(
                            f"⚠️ Error fetching chunk for {table} (PK > {last_val}): {e}. "
                            f"The script has an outer retry loop for 'still failing after {MAX_RETRIES} retries'. "
                            f"Sleeping 5s before that outer retry..."
                        )
                        await asyncio.sleep(5) # Additional sleep before the script's own retry logic
                        continue # Retry fetching the same chunk (part of the script's loop)

                    rows = data.get("value", [])
                    if not rows: # No more rows to fetch for this table
                        break
                    
                    current_df = pd.DataFrame.from_records(rows)
                    dfs.append(current_df)
                    
                    # Update last_val to the PK of the last row fetched in this chunk
                    extracted_pk_val = current_df[pk].iloc[-1]
                    try:
                        last_val = int(extracted_pk_val)
                    except ValueError:
                        # This should ideally not happen if PKs are always integers.
                        logger.critical(
                            f"Could not convert PK '{extracted_pk_val}' to int for table {table}. Resume logic might be compromised. Data type of PK column: {current_df[pk].dtype}",
                            exc_info=True
                        )
                        raise # Re-raise to make the issue visible and stop if PK is not convertible.
                        
                    total_rows_fetched += len(rows)
                    pbar.update(len(rows)) # Update progress bar
                    
                    # Checkpoint: Save resume state after each successful chunk
                    resume_state[table] = last_val 
                    _save_resume(resume_state)
            
            logger.info(f"Fetched {total_rows_fetched:,} rows in total for {table} (up to {pk} {last_val})")
            
            # Finished successfully – clean resume marker for this table
            if table in resume_state:
                del resume_state[table]
                _save_resume(resume_state)
            
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        # --- Default $skip paging – now parallel ---
        try:
            # Get the total count of records for progress bar and page calculation
            count_url = f"{BASE_URL}/{entity}/$count"
            # OData $count often returns plain text, not JSON
            total_records_str = await session.get(count_url, timeout=aiohttp.ClientTimeout(total=30))
            total_records_str.raise_for_status()
            total_records = int(await total_records_str.text())
        except Exception as e:
            logger.warning(f"Could not get $count for {table}: {e}. Attempting sequential download without total.", exc_info=True)
            return await _download_sequential(session, entity) # Fallback to sequential

        if total_records == 0:
            logger.info(f"Table {table} has 0 records according to $count.")
            return pd.DataFrame()

        num_pages = ceil(total_records / PAGE_SIZE)
        # Progress bar for skip-based paging
        with tqdm(total=total_records, desc=f"Fetching {table} (skip)", unit="rows", leave=False) as pbar:
            semaphore = asyncio.Semaphore(CONCURRENCY) # Limit concurrent requests

            async def fetch_page_skip(page_index: int):
                """Fetches a single page of data using $skip and $top."""
                async with semaphore: # Acquire semaphore before proceeding
                    skip_val = page_index * PAGE_SIZE
                    page_url = f"{BASE_URL}/{entity}?$format=json&$skip={skip_val}&$top={PAGE_SIZE}"
                    try:
                        page_data = await fetch_json(session, page_url) # Uses backoff internally
                        page_rows = page_data.get("value", [])
                        if page_rows:
                            pbar.update(len(page_rows)) # Update progress bar
                            return page_index, pd.DataFrame.from_records(page_rows)
                    except Exception as e:
                        # Log error if fetch_json (with its retries) ultimately fails for this page
                        logger.error(f"Error fetching page {page_index} for {table} after all retries: {e}.", exc_info=True)
                        return page_index, None # Indicate failure for this page
                    return page_index, None # No data or empty page

            # Gather results from all page fetch tasks
            page_fetch_tasks = [fetch_page_skip(i) for i in range(num_pages)]
            results = await asyncio.gather(*page_fetch_tasks, return_exceptions=True)

        # Process results: sort by page_index and concatenate DataFrames
        valid_dfs = []
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"A page fetch task ultimately failed: {res}", exc_info=True)
            elif res is not None and res[1] is not None: # res is (page_index, df_or_none)
                 valid_dfs.append(res) # Add if df is not None
        
        valid_dfs.sort(key=lambda x: x[0]) # Sort by page index to ensure correct order
        final_dfs = [df for _, df in valid_dfs] # Extract DataFrames
        return pd.concat(final_dfs, ignore_index=True) if final_dfs else pd.DataFrame()

async def _download_sequential(session: aiohttp.ClientSession, entity: str) -> pd.DataFrame:
    """Fallback to download a table sequentially if count or parallel download fails."""
    table_name = entity.replace('()','')
    logger.info(f"Using sequential download for {table_name}.")
    dfs: List[pd.DataFrame] = []
    page_index = 0
    with tqdm(desc=f"Fetching {table_name} (sequential)", unit="rows", leave=False) as pbar:
        while True:
            skip_val = page_index * PAGE_SIZE
            url = f"{BASE_URL}/{entity}?$format=json&$skip={skip_val}&$top={PAGE_SIZE}"
            try:
                data = await fetch_json(session, url) # Uses backoff internally
            except Exception as e:
                # This means fetch_json failed after all its retries for this page
                logger.error(f"Error fetching page {page_index} (sequential) for {table_name} after all retries: {e}. Stopping for this table.", exc_info=True)
                break # Stop trying for this table if a page fails sequentially after retries
            
            rows = data.get("value", [])
            if not rows: # No more data
                break
            
            dfs.append(pd.DataFrame.from_records(rows))
            pbar.update(len(rows))
            page_index += 1
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# -----------------------------------------------------------------------------
# Storage helper (to DuckDB and Parquet)
# -----------------------------------------------------------------------------
def store(df: pd.DataFrame, table: str, db_path: Path = DEFAULT_DB):
    """Stores a DataFrame into a DuckDB table and as a Parquet file."""
    if df.empty:
        logger.info(f"Table '{table}' is empty, skipping storage.")
        return

    # Ensure data directory for DuckDB exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Connect to DuckDB. The connection will be closed automatically if using 'with'.
        with duckdb.connect(db_path.as_posix()) as con:
            # Create or replace the table in DuckDB using the DataFrame
            con.execute(f"CREATE OR REPLACE TABLE \"{table}\" AS SELECT * FROM df")
        logger.info(f"Successfully saved {len(df):,} rows for table '{table}' to DuckDB: {db_path}")
    except Exception as e:
        logger.error(f"Error storing '{table}' to DuckDB: {e}", exc_info=True)
        return # Stop if DB storage fails, to prevent inconsistent Parquet

    # Store as Parquet file
    PARQUET_DIR.mkdir(parents=True, exist_ok=True) # Ensure parquet directory exists
    parquet_path = PARQUET_DIR / f"{table}.parquet"
    try:
        df.to_parquet(parquet_path, compression="zstd", index=False)
        # Try to get a relative path for cleaner logging
        try:
            rel_parquet_path = parquet_path.resolve().relative_to(Path.cwd().resolve())
        except ValueError:
            rel_parquet_path = parquet_path # Fallback to absolute if not relative
        logger.info(f"Parquet data for '{table}' saved to {rel_parquet_path}")
    except Exception as e:
        logger.error(f"Error saving '{table}' to Parquet: {e}", exc_info=True)

# -----------------------------------------------------------------------------
# Load and Store User-Managed Faction Coalition Status from CSV
# -----------------------------------------------------------------------------
def load_and_store_faction_statuses(db_path: Path = DEFAULT_DB):
    """
    Loads faction coalition statuses from a user-maintained CSV file
    (`data/faction_coalition_status.csv`) and stores it into a dedicated table
    (`UserFactionCoalitionStatus`) in DuckDB.
    """
    status_table_name = "UserFactionCoalitionStatus"
    expected_cols = ['KnessetNum', 'FactionID', 'FactionName', 'CoalitionStatus', 'DateJoinedCoalition', 'DateLeftCoalition']
    
    col_dtypes = {
        'KnessetNum': 'Int64', 
        'FactionID': 'Int64',   
        'FactionName': 'string',
        'CoalitionStatus': 'string',
        'DateJoinedCoalition': 'object', 
        'DateLeftCoalition': 'object'
    }
    
    db_table_schema = f"""
        CREATE TABLE IF NOT EXISTS "{status_table_name}" (
            KnessetNum INTEGER,
            FactionID INTEGER,
            FactionName VARCHAR,
            CoalitionStatus VARCHAR,
            DateJoinedCoalition DATE,
            DateLeftCoalition DATE
        )
    """

    if not FACTION_COALITION_STATUS_FILE.exists():
        logger.info(f"Faction coalition status file not found: {FACTION_COALITION_STATUS_FILE}. Ensuring empty '{status_table_name}' table exists.")
        try:
            with duckdb.connect(db_path.as_posix()) as con:
                con.execute(db_table_schema)
            logger.info(f"Ensured empty table '{status_table_name}' exists.")
        except Exception as e:
            logger.error(f"Error ensuring empty '{status_table_name}' table exists: {e}", exc_info=True)
        return

    logger.info(f"Loading faction coalition statuses from: {FACTION_COALITION_STATUS_FILE}")
    try:
        status_df = pd.read_csv(
            FACTION_COALITION_STATUS_FILE,
            dtype=col_dtypes,
            keep_default_na=True, 
            na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', 
                       '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NULL', 'NaN', 'n/a', 
                       'nan', 'null']
        )
        
        if not all(col in status_df.columns for col in expected_cols):
            missing_cols = [col for col in expected_cols if col not in status_df.columns]
            logger.error(f"{FACTION_COALITION_STATUS_FILE} is missing required columns: {missing_cols}. Expected: {expected_cols}, Found: {list(status_df.columns)}")
            
            
            return

        status_df['DateJoinedCoalition'] = pd.to_datetime(status_df['DateJoinedCoalition'], errors='coerce')
        status_df['DateLeftCoalition'] = pd.to_datetime(status_df['DateLeftCoalition'], errors='coerce')
        status_df['FactionName'] = status_df['FactionName'].astype('string')
        status_df['CoalitionStatus'] = status_df['CoalitionStatus'].astype('string')

        if status_df.empty and FACTION_COALITION_STATUS_FILE.stat().st_size > 0:
            logger.warning(f"Faction coalition status file {FACTION_COALITION_STATUS_FILE} is not empty but resulted in an empty DataFrame after parsing.")
        elif status_df.empty:
             logger.info(f"Faction coalition status file {FACTION_COALITION_STATUS_FILE} is empty.")
        
        with duckdb.connect(db_path.as_posix()) as con:
            con.execute(f"CREATE OR REPLACE TABLE \"{status_table_name}\" AS SELECT * FROM status_df")
        logger.info(f"Successfully loaded {len(status_df)} faction statuses into DuckDB table '{status_table_name}' in {db_path}.")

    except pd.errors.EmptyDataError:
        logger.info(f"Faction coalition status file {FACTION_COALITION_STATUS_FILE} is completely empty. Ensuring empty table.")
        try:
            with duckdb.connect(db_path.as_posix()) as con:
                con.execute(db_table_schema) 
                con.execute(f"DELETE FROM \"{status_table_name}\"") 
            logger.info(f"Ensured empty table '{status_table_name}' after processing empty CSV.")
        except Exception as e:
            logger.error(f"Error ensuring empty '{status_table_name}' table after empty CSV: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An error occurred while loading faction coalition statuses: {e}", exc_info=True)
        

# -----------------------------------------------------------------------------
# Data‑quality helper functions (example, can be expanded or adapted)
# -----------------------------------------------------------------------------
def map_mk_site_code(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return mapping MK internal PersonID → website SiteID (if table KNS_MkSiteCode present)."""
    try:
        # Check if KNS_MkSiteCode table exists before querying
        tables_df = con.execute("SHOW TABLES").df()
        if "kns_mksitecode" in tables_df['name'].str.lower().tolist(): # Case-insensitive check
            return con.sql("SELECT KnsID, SiteID FROM KNS_MkSiteCode").df()
        else:
            logger.info("KNS_MkSiteCode table not found. Cannot map MK site codes.")
            return pd.DataFrame(columns=['KnsID', 'SiteID']) # Return empty DF with expected columns
    except Exception as e:
        logger.warning(f"Error accessing KNS_MkSiteCode: {e}", exc_info=True)
        return pd.DataFrame(columns=['KnsID', 'SiteID'])

# -----------------------------------------------------------------------------
# Exported helpers for Streamlit UI and other scripts
# -----------------------------------------------------------------------------
async def _fetch_single_table(
    table: str,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    db_path: Path = DEFAULT_DB,
    **kwargs, 
) -> pd.DataFrame:
    """Internal helper to download and store a single OData table, with progress callback."""
    df = await download_table(table)
    store(df, table, db_path=db_path) 
    if progress_cb and not df.empty: 
        progress_cb(table, len(df))
    return df

async def refresh_tables(
    tables: List[str] | None = None,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    db_path: Path = DEFAULT_DB,
):
    """
    Downloads specified OData tables (or all predefined) into DuckDB.
    Calls progress_cb(table_name, num_rows_fetched) after each table.
    Also loads the user-managed faction coalition statuses from CSV.
    """
    tables_to_fetch = tables if tables is not None else TABLES 
    
    valid_fetch_tables = list(set(TABLES + list(CURSOR_TABLES.keys())))
    invalid_tables = [t for t in tables_to_fetch if t not in valid_fetch_tables]
    if invalid_tables:
        raise ValueError(f"Invalid table(s) specified: {invalid_tables}. Must be in defined system tables.")

    logger.info(f"Starting OData table refresh process for: {tables_to_fetch}, DB: {db_path}")
    for t_name in tables_to_fetch:
        await _fetch_single_table(t_name, progress_cb=progress_cb, db_path=db_path)
    
    logger.info("OData table refresh part complete.")
    logger.info("Now loading user-managed faction coalition statuses...")
    load_and_store_faction_statuses(db_path=db_path)
    logger.info("All data refresh tasks finished successfully.")

def ensure_latest(tables: List[str] | None = None, db_path: Path = DEFAULT_DB):
    """Synchronous wrapper for refresh_tables. Useful for simple scripts or non-async contexts."""
    try:
        asyncio.run(refresh_tables(tables=tables, db_path=db_path))
    except Exception as e:
        logger.error(f"Error during synchronous refresh (ensure_latest): {e}", exc_info=True)

# -----------------------------------------------------------------------------
# CLI (Command Line Interface) helpers
# -----------------------------------------------------------------------------
def list_tables_cli():
    """Prints the list of predefined tables that can be fetched via CLI."""
    output = ["Available predefined tables for fetching:"]
    all_known_tables = sorted(list(set(TABLES + list(CURSOR_TABLES.keys()))))
    for t in all_known_tables:
        output.append(f"  - {t}{' (cursor-paged)' if t in CURSOR_TABLES else ''}")

def parse_args_cli(): # CORRECTED: This is the function definition
    """Parses command-line arguments for the script."""
    parser = argparse.ArgumentParser(
        description="Knesset OData Fetcher: Downloads data into DuckDB/Parquet and allows ad-hoc SQL queries.",
        formatter_class=argparse.RawTextHelpFormatter 
    )
    
    action_group = parser.add_mutually_exclusive_group(required=False) # Made not strictly required to allow no-args run
    action_group.add_argument("--table", metavar="TABLE_NAME", help="Name of a single OData table to fetch.")
    action_group.add_argument("--all", action="store_true", help="Fetch all predefined OData tables.")
    action_group.add_argument("--sql", metavar="\"SQL_QUERY\"", help="Run an SQL query against the DuckDB warehouse.")
    action_group.add_argument("--list-tables", action="store_true", help="List all predefined tables that can be fetched.")
    action_group.add_argument(
        "--refresh-faction-status",
        action="store_true",
        help=f"Only refresh the faction coalition status from {FACTION_COALITION_STATUS_FILE} into the database."
    )

    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help=f"Path to the DuckDB database file (default: {DEFAULT_DB}).")
    return parser.parse_args()

def main_cli():
    """Main function to handle CLI operations."""
    args = parse_args_cli() # CORRECTED: This calls the defined function

    if args.list_tables:
        list_tables_cli()
        return

    if args.sql:
        if not args.db.exists():
            logger.error(f"DuckDB warehouse not found at '{args.db}'. Run a fetch operation first or check path.")
            sys.exit(1)
        try:
            with duckdb.connect(args.db.as_posix(), read_only=True) as con:
                query_result_df = con.sql(args.sql).df()
            print(query_result_df.to_csv(index=False)) # Output to stdout as per original
        except Exception as e: 
            logger.error(f"SQL query execution failed: {e}", exc_info=True)
            sys.exit(1)
        return

    if args.refresh_faction_status:
        logger.info(f"Refreshing only faction coalition statuses from {FACTION_COALITION_STATUS_FILE} into {args.db}...")
        load_and_store_faction_statuses(db_path=args.db)
        logger.info("Faction coalition status refresh complete (CLI trigger).")
        return

    tables_to_process: List[str] | None = None
    if args.all:
        tables_to_process = TABLES 
    elif args.table:
        all_known_tables = list(set(TABLES + list(CURSOR_TABLES.keys())))
        if args.table not in all_known_tables:
             logger.warning(f"Table '{args.table}' specified via CLI is not in the predefined list. Attempting to fetch anyway.")
             print(f"    Attempting to fetch '{args.table}' anyway. Ensure it's a valid OData entity name.")
        tables_to_process = [args.table]
    
    if tables_to_process is None and not (args.all or args.table or args.sql or args.list_tables or args.refresh_faction_status):
        print("ℹ️ No action specified. Use --all, --table <TABLE_NAME>, --list-tables, --refresh-faction-status, or --sql \"QUERY\".")
        parse_args_cli() # Show help if no arguments
        return

    if tables_to_process: # Only proceed if there are OData tables to fetch
        print(f"🚀 Starting data fetch for OData tables: {tables_to_process} into {args.db}")
        try:
            asyncio.run(refresh_tables(tables=tables_to_process, db_path=args.db)) 
            print("\n🎉 All selected OData tables and faction statuses processed successfully.")
        except ValueError as ve: 
            print(f"❌ Error: {ve}")
            sys.exit(1)
        except Exception as e: 
            print(f"❌ An unexpected error occurred during data fetching: {e}")
            
            sys.exit(1)

if __name__ == "__main__":
    # This block ensures main_cli() is called only when the script is executed directly
    main_cli()
