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

    def _run_async(self, coro: Any) -> Any:
        """Run an async coroutine from sync context, handling Streamlit's event loop."""
        try:
            asyncio.get_running_loop()
            # Inside Streamlit's event loop — run in a separate thread
            import concurrent.futures

            service = self

            def _thread_target() -> Any:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(coro)
                except Exception as exc:
                    service.logger.error("Error in async thread: %s", exc, exc_info=True)
                    return False
                finally:
                    loop.close()

            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(_thread_target).result(timeout=600)
        except RuntimeError:
            # No running loop (CLI context) — safe to use asyncio.run()
            return asyncio.run(coro)

    def refresh_tables_sync(
        self,
        tables: Optional[List[str]] = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> bool:
        """Refresh tables via async service with a sync API."""
        try:
            async_service = self._get_async_service()
            if async_service:
                result = self._run_async(
                    async_service.refresh_tables(tables, progress_callback)
                )
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
