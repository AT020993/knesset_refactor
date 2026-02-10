"""Synchronous wrapper around asynchronous data refresh operations."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Callable, List, Optional

from config.settings import Settings
from data.repositories.database_repository import DatabaseRepository


class SyncDataRefreshService:
    """Sync entrypoint for data refresh from UI/CLI contexts."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None,
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)
        self._async_service: Any | None = None

    def _get_async_service(self) -> Any | None:
        """Lazy-load async service to avoid import cycles."""
        if self._async_service is None:
            try:
                from data.services.data_refresh_service import DataRefreshService

                self._async_service = DataRefreshService(self.db_path, self.logger)
            except ImportError as exc:
                self.logger.warning("Could not import DataRefreshService: %s", exc)
        return self._async_service

    def refresh_tables_sync(
        self,
        tables: Optional[List[str]] = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> bool:
        """Refresh tables via async service with a sync API."""
        try:
            async_service = self._get_async_service()
            if async_service:
                result = asyncio.run(async_service.refresh_tables(tables, progress_callback))
                return bool(result)

            from backend.fetch_table import ensure_latest

            ensure_latest(tables=tables, db_path=self.db_path)
            return True
        except Exception as exc:
            self.logger.error("Error in refresh_tables_sync: %s", exc, exc_info=True)
            return False

    def refresh_faction_status_only(self) -> bool:
        """Refresh only faction coalition status lookup data."""
        try:
            repo = DatabaseRepository(self.db_path, self.logger)
            return bool(repo.load_faction_coalition_status())
        except Exception as exc:
            self.logger.error("Error refreshing faction status: %s", exc, exc_info=True)
            return False
