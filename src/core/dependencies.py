"""Dependency injection container for the application."""

from pathlib import Path
from typing import Optional
import logging
import asyncio

from config.settings import Settings
from utils.logger_setup import setup_logging
from backend.fetch_table import refresh_tables


class DataRefreshService:
    """Simple wrapper for the refresh functionality."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger

    async def refresh_tables(self, tables: Optional[list] = None) -> bool:
        """Refresh tables using the backend functionality."""
        try:
            await refresh_tables(tables=tables, db_path=self.db_path)
            return True
        except Exception as e:
            self.logger.error(f"Error refreshing tables: {e}")
            return False

    def refresh_faction_status_only(self) -> bool:
        """Refresh faction status - placeholder for now."""
        self.logger.warning("Faction status refresh not implemented yet")
        return True


class DependencyContainer:
    """Container for managing application dependencies."""

    def __init__(
        self, db_path: Optional[Path] = None, logger_name: str = "knesset_app"
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = setup_logging(logger_name)
        Settings.ensure_directories()

        # Initialize services
        self._data_refresh_service = None

    @property
    def data_refresh_service(self) -> DataRefreshService:
        """Get or create the data refresh service."""
        if self._data_refresh_service is None:
            self._data_refresh_service = DataRefreshService(self.db_path, self.logger)
        return self._data_refresh_service

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Get a logger instance."""
        if name:
            return logging.getLogger(name)
        return self.logger
