"""Legacy compatibility layer for data refresh operations.

This module preserves the historical fetch_table API while delegating to the
current data services implementation.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Callable, Optional

from config.settings import Settings
from data.repositories.database_repository import DatabaseRepository
from data.services.data_refresh_service import DataRefreshService

warnings.warn(
    "fetch_table module is deprecated. Use the modular data services instead.",
    DeprecationWarning,
    stacklevel=2,
)

try:
    from config.api import APIConfig

    BASE_URL = APIConfig.BASE_URL
    PAGE_SIZE = APIConfig.PAGE_SIZE
    MAX_RETRIES = APIConfig.MAX_RETRIES
    CONCURRENCY = APIConfig.CONCURRENCY_LIMIT
except ImportError:
    BASE_URL = "https://knesset.gov.il/Odata"
    PAGE_SIZE = 100
    MAX_RETRIES = 8
    CONCURRENCY = 3

try:
    from config.database import DatabaseConfig

    TABLES: list[str] = list(DatabaseConfig.TABLES)
    CURSOR_TABLES: dict[str, tuple[str, int]] = dict(DatabaseConfig.CURSOR_TABLES)
except ImportError:
    TABLES = ["KNS_Person", "KNS_PersonToPosition", "KNS_Position", "KNS_Faction"]
    CURSOR_TABLES = {}

DEFAULT_DB = Settings.DEFAULT_DB_PATH
PARQUET_DIR = Settings.PARQUET_DIR
RESUME_FILE = Settings.RESUME_STATE_FILE
FACTION_COALITION_STATUS_FILE = Settings.FACTION_COALITION_STATUS_FILE


def _adapt_progress_callback(
    progress_cb: Optional[Callable[..., None]],
) -> Optional[Callable[[str, int], None]]:
    """Normalize legacy progress callbacks to current callback shape."""
    if progress_cb is None:
        return None

    def _wrapped(message: str, count: int) -> None:
        try:
            progress_cb(message, count)
        except TypeError:
            progress_cb(message)

    return _wrapped


async def refresh_tables(
    tables: Optional[list[str]] = None,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    db_path: Path = DEFAULT_DB,
) -> bool:
    """Legacy async API delegating to DataRefreshService.refresh_tables."""
    logger = logging.getLogger(__name__)
    service = DataRefreshService(db_path=db_path, logger_obj=logger)
    adapted_progress = _adapt_progress_callback(progress_cb)

    try:
        return await service.refresh_tables(tables=tables, progress_callback=adapted_progress)
    except Exception as exc:
        logger.error("Legacy refresh_tables failed: %s", exc, exc_info=True)
        return False


def ensure_latest(
    tables: Optional[list[str]] = None,
    db_path: Path = DEFAULT_DB,
) -> bool:
    """Legacy sync API delegating to DataRefreshService.refresh_tables_sync."""
    logger = logging.getLogger(__name__)
    service = DataRefreshService(db_path=db_path, logger_obj=logger)

    try:
        return service.refresh_tables_sync(tables=tables)
    except Exception as exc:
        logger.error("Legacy ensure_latest failed: %s", exc, exc_info=True)
        return False


def load_and_store_faction_statuses(db_path: Path = DEFAULT_DB) -> bool:
    """Legacy faction status loader delegating to DatabaseRepository."""
    logger = logging.getLogger(__name__)
    repository = DatabaseRepository(db_path, logger)

    try:
        return repository.load_faction_coalition_status()
    except Exception as exc:
        logger.error(
            "Legacy load_and_store_faction_statuses failed: %s",
            exc,
            exc_info=True,
        )
        return False


def map_mk_site_code(con) -> dict:
    """Deprecated legacy compatibility function."""
    warnings.warn(
        "map_mk_site_code is deprecated and not implemented in the new system.",
        DeprecationWarning,
    )
    return {}


def parse_args_cli():
    """Deprecated legacy CLI shim."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py' instead.",
        DeprecationWarning,
    )
    return None


def main_cli():
    """Deprecated legacy CLI shim."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py' instead.",
        DeprecationWarning,
    )
    print("Please use the new CLI: python src/cli.py --help")


def list_tables_cli():
    """Deprecated legacy CLI shim."""
    warnings.warn(
        "CLI functionality has moved to src/cli.py. Use 'python src/cli.py --help' "
        "to see available commands.",
        DeprecationWarning,
    )
    print("Please use the new CLI: python src/cli.py --help")


__all__ = [
    "BASE_URL",
    "PAGE_SIZE",
    "MAX_RETRIES",
    "CONCURRENCY",
    "TABLES",
    "CURSOR_TABLES",
    "DEFAULT_DB",
    "PARQUET_DIR",
    "RESUME_FILE",
    "FACTION_COALITION_STATUS_FILE",
    "refresh_tables",
    "ensure_latest",
    "load_and_store_faction_statuses",
    "map_mk_site_code",
    "parse_args_cli",
    "main_cli",
    "list_tables_cli",
]
