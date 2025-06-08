"""Dependency injection container for the application."""

from pathlib import Path
from typing import Optional
import logging

from config.settings import Settings
from data.services.data_refresh_service import DataRefreshService
from utils.logger_setup import setup_logging


class DependencyContainer:
    """Container for managing application dependencies."""
    
    def __init__(self, db_path: Optional[Path] = None, logger_name: str = "knesset_app"):
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