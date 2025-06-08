"""Simplified data refresh service used for tests."""

from pathlib import Path
from typing import Optional, Iterable, Callable
import logging


class DataRefreshService:
    """Placeholder service providing refresh interface."""

    def __init__(self, db_path: Path | None = None, logger_obj: Optional[logging.Logger] = None) -> None:
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)

    async def refresh_tables(self, tables: Optional[Iterable[str]] = None, progress_cb: Optional[Callable[[str, int], None]] = None) -> None:
        """Async placeholder for table refresh."""
        if progress_cb:
            for i, _ in enumerate(tables or []):
                progress_cb("fetch", i)

    def refresh_tables_sync(self, tables: Optional[Iterable[str]] = None, progress_callback: Optional[Callable[[str, int], None]] = None) -> bool:
        """Synchronous placeholder used in tests."""
        if progress_callback:
            for i, _ in enumerate(tables or []):
                progress_callback("fetch", i)
        return True

    def refresh_faction_status_only(self) -> bool:
        """Placeholder for refreshing faction status."""
        return True

