from pathlib import Path
from typing import Dict, List, Tuple

# -----------------------------------------------------------------------------
# API Related URLs and Parameters
# -----------------------------------------------------------------------------
BASE_URL = "http://knesset.gov.il/Odata/ParliamentInfo.svc"
PAGE_SIZE = 100  # Number of records to fetch per API request for non-cursor tables

# -----------------------------------------------------------------------------
# Paths for Data Storage
# -----------------------------------------------------------------------------
DEFAULT_DB_NAME = "warehouse.duckdb"
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / DEFAULT_DB_NAME
PARQUET_DIR = DATA_DIR / "parquet"
RESUME_FILE = DATA_DIR / ".resume_state.json"
FACTION_COALITION_STATUS_FILE = DATA_DIR / "faction_coalition_status.csv"

# -----------------------------------------------------------------------------
# Retry Mechanism Parameters
# -----------------------------------------------------------------------------
MAX_RETRIES = 8  # Maximum number of retries for failed HTTP requests
INITIAL_BACKOFF = 1  # Initial backoff delay in seconds for retries

# -----------------------------------------------------------------------------
# Download Concurrency
# -----------------------------------------------------------------------------
CONCURRENCY = 8  # Number of concurrent page fetches for skip-based paging

# -----------------------------------------------------------------------------
# Table Definitions and Metadata
# -----------------------------------------------------------------------------
# List of tables to be fetched from the OData API
TABLES: List[str] = [
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

# You can add any other hardcoded values here that might need adjustment
# For example, logging configurations, specific timeouts not covered by backoff, etc.
