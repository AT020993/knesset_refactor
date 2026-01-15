"""Dependency injection container for the application."""

from pathlib import Path
from typing import Optional
import logging

from config.settings import Settings
from utils.logger_setup import setup_logging

# Import the sync wrapper from UI services (works with both sync and async contexts)
from ui.services.data_service import SyncDataRefreshService


class DependencyContainer:
    """Container for managing application dependencies."""

    def __init__(
        self, db_path: Optional[Path] = None, logger_name: str = "knesset_app"
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = setup_logging(logger_name)
        Settings.ensure_directories()

        # Initialize services lazily
        self._data_refresh_service = None

    @property
    def data_refresh_service(self) -> SyncDataRefreshService:
        """Get or create the data refresh service."""
        if self._data_refresh_service is None:
            self._data_refresh_service = SyncDataRefreshService(
                self.db_path, self.logger
            )
        return self._data_refresh_service

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Get a logger instance."""
        if name:
            return logging.getLogger(name)
        return self.logger
