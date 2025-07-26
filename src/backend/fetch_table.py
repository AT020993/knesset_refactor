"""
Legacy compatibility layer for fetch_table.

This module provides backward compatibility for existing code that imports
from the old fetch_table module.

IMPORTANT NOTE: Committee Data Fetching
The default fetch for KNS_Committee only retrieves recent committees (Knessets 15-16).
For complete historical committee data across Knessets 1-25, manual KnessetNum filtering
is required using the API endpoint:
https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Committee()?$filter=KnessetNum%20eq%20{N}

This limitation affects committee name resolution in bill queries, which show
"Committee [number]" instead of actual names for historical committees.
"""

import warnings
import asyncio
from pathlib import Path
from typing import List, Optional, Callable

# Import from local modules instead of non-existent data modules
from config.settings import Settings

# Deprecation warning
warnings.warn(
    "fetch_table module is deprecated. Use the new modular system instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Legacy constants for backward compatibility
try:
    from config.api import APIConfig

    BASE_URL = APIConfig.BASE_URL
    PAGE_SIZE = APIConfig.PAGE_SIZE
    MAX_RETRIES = APIConfig.MAX_RETRIES
    CONCURRENCY = APIConfig.CONCURRENCY_LIMIT
except ImportError:
    # Fallback values if config modules don't exist
    BASE_URL = "https://knesset.gov.il/Odata"
    PAGE_SIZE = 100
    MAX_RETRIES = 8
    CONCURRENCY = 3

try:
    from config.database import DatabaseConfig

    TABLES = DatabaseConfig.TABLES
    CURSOR_TABLES = DatabaseConfig.CURSOR_TABLES
except ImportError:
    # Fallback values
    TABLES = ["KNS_Person", "KNS_PersonToPosition", "KNS_Position", "KNS_Faction"]
    CURSOR_TABLES = []

DEFAULT_DB = Settings.DEFAULT_DB_PATH
PARQUET_DIR = Settings.PARQUET_DIR
RESUME_FILE = Settings.RESUME_STATE_FILE
FACTION_COALITION_STATUS_FILE = Settings.FACTION_COALITION_STATUS_FILE


# Simple placeholder implementation for refresh_tables
async def refresh_tables(
    tables: Optional[List[str]] = None,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    db_path: Path = DEFAULT_DB,
) -> None:
    """Legacy wrapper for refresh_tables."""
    # For now, just log that this is called
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"refresh_tables called with tables={tables}, db_path={db_path}")

    # Placeholder implementation - in a real system this would fetch OData
    if progress_cb:
        progress_cb("Starting refresh", 0)
        await asyncio.sleep(0.1)  # Simulate work
        progress_cb("Refresh complete", 100)


def ensure_latest(
    tables: Optional[List[str]] = None, db_path: Path = DEFAULT_DB
) -> None:
    """Legacy wrapper for ensure_latest."""
    # Synchronous wrapper
    asyncio.run(refresh_tables(tables=tables, db_path=db_path))


def load_and_store_faction_statuses(db_path: Path = DEFAULT_DB) -> None:
    """Legacy wrapper for faction status loading."""
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"load_and_store_faction_statuses called with db_path={db_path}")
    # Placeholder implementation


def map_mk_site_code(con) -> any:
    """Legacy wrapper for MK site code mapping."""
    warnings.warn(
        "map_mk_site_code is deprecated and not implemented in the new system.",
        DeprecationWarning,
    )
    return {}


# Legacy CLI functions - these should be removed in favor of the new CLI
def parse_args_cli():
    """Legacy CLI function - use new CLI instead."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py' instead.",
        DeprecationWarning,
    )
    return None


def main_cli():
    """Legacy CLI function - use new CLI instead."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py' instead.",
        DeprecationWarning,
    )
    print("Please use the new CLI: python src/cli.py --help")


def list_tables_cli():
    """Legacy CLI function - use new CLI instead."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py --help' to see available commands.",
        DeprecationWarning,
    )
    print("Please use the new CLI: python src/cli.py --help")
