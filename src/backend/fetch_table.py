"""
Legacy compatibility layer for fetch_table.

This module provides backward compatibility for existing code that imports
from the old fetch_table module. All functionality has been moved to
the new modular data system.

For new code, use:
- data.services.data_refresh_service.DataRefreshService
- api.odata_client.ODataClient
"""

import warnings
import asyncio
from pathlib import Path
from typing import List, Optional, Callable

from data.services.data_refresh_service import DataRefreshService
from config.settings import Settings

# Deprecation warning
warnings.warn(
    "fetch_table module is deprecated. Use data.services.data_refresh_service.DataRefreshService instead.",
    DeprecationWarning,
    stacklevel=2
)

# Legacy constants for backward compatibility
from config.api import APIConfig
from config.database import DatabaseConfig

BASE_URL = APIConfig.BASE_URL
DEFAULT_DB = Settings.DEFAULT_DB_PATH
PAGE_SIZE = APIConfig.PAGE_SIZE
MAX_RETRIES = APIConfig.MAX_RETRIES
PARQUET_DIR = Settings.PARQUET_DIR
RESUME_FILE = Settings.RESUME_STATE_FILE
CONCURRENCY = APIConfig.CONCURRENCY_LIMIT
FACTION_COALITION_STATUS_FILE = Settings.FACTION_COALITION_STATUS_FILE
TABLES = DatabaseConfig.TABLES
CURSOR_TABLES = DatabaseConfig.CURSOR_TABLES


# Minimal helpers used by tests ------------------------------------------------
import aiohttp
import pandas as pd
import duckdb
from api.error_handling import categorize_error, ErrorCategory
from api.circuit_breaker import CircuitBreaker, CircuitBreakerState

resume_state: dict = {}


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict:
    """Simplified JSON fetch used in tests."""
    resp = await session.get(url)
    resp.raise_for_status()
    return await resp.json()


async def _download_sequential(session: aiohttp.ClientSession, entity: str) -> pd.DataFrame:
    """Sequential download fallback used by tests."""
    dfs: list[pd.DataFrame] = []
    page = 0
    while True:
        url = f"{BASE_URL}/{entity}?$format=json&$skip={page * PAGE_SIZE}&$top={PAGE_SIZE}"
        try:
            data = await fetch_json(session, url)
        except Exception:
            return pd.DataFrame()
        rows = data.get("value", [])
        if not rows:
            break
        dfs.append(pd.DataFrame.from_records(rows))
        page += 1
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


async def download_table(table: str) -> pd.DataFrame:
    """Basic table downloader used in tests."""
    entity = f"{table}()"
    async with aiohttp.ClientSession() as session:
        if table in CURSOR_TABLES:
            pk, _ = CURSOR_TABLES[table]
            url = f"{BASE_URL}/{entity}?$format=json&$orderby={pk}%20asc"
            data = await fetch_json(session, url)
            return pd.DataFrame.from_records(data.get("value", []))
        else:
            return await _download_sequential(session, entity)


def store(df: pd.DataFrame, table: str, db_path: Path = DEFAULT_DB) -> None:
    """Store dataframe into DuckDB and Parquet (simplified)."""
    if df.empty:
        return
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path.as_posix()) as con:
        con.execute(f"CREATE OR REPLACE TABLE \"{table}\" AS SELECT * FROM df")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET_DIR / f"{table}.parquet", index=False)


def _load_resume() -> dict:
    if Path(RESUME_FILE).exists():
        import json
        try:
            with open(RESUME_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_resume(state: dict) -> None:
    import json
    Path(RESUME_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(RESUME_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

# Legacy function wrappers
async def refresh_tables(
    tables: Optional[List[str]] = None,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    db_path: Path = DEFAULT_DB,
) -> None:
    """Legacy wrapper for refresh_tables."""
    service = DataRefreshService(db_path)
    await service.refresh_tables(tables, progress_cb)


def ensure_latest(tables: Optional[List[str]] = None, db_path: Path = DEFAULT_DB) -> None:
    """Legacy wrapper for ensure_latest."""
    service = DataRefreshService(db_path)
    service.refresh_tables_sync(tables)


def load_and_store_faction_statuses(db_path: Path = DEFAULT_DB) -> None:
    """Legacy wrapper for faction status loading."""
    from data.repositories.database_repository import DatabaseRepository
    repo = DatabaseRepository(db_path)
    repo.load_faction_coalition_status()


def map_mk_site_code(con) -> any:
    """Legacy wrapper for MK site code mapping."""
    from backend.utils import map_mk_site_code as new_map_mk_site_code
    # This is a hack since the new function needs db_path instead of connection
    # In practice, existing code should be updated to use the new pattern
    warnings.warn(
        "map_mk_site_code signature has changed. Use backend.utils.map_mk_site_code with db_path parameter.",
        DeprecationWarning
    )
    return new_map_mk_site_code(DEFAULT_DB)


# Legacy CLI functions - these should be removed in favor of the new CLI
def parse_args_cli():
    """Legacy CLI function - use new CLI instead."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py' instead.",
        DeprecationWarning
    )
    return None


def main_cli():
    """Legacy CLI function - use new CLI instead."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py' instead.",
        DeprecationWarning
    )
    print("Please use the new CLI: python src/cli.py --help")


def list_tables_cli():
    """Legacy CLI function - use new CLI instead."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py --help' to see available commands.",
        DeprecationWarning
    )
    print("Please use the new CLI: python src/cli.py --help")
