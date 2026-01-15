"""Data service for UI layer."""

import asyncio
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd

# Use the real repository from data layer
from data.repositories.database_repository import DatabaseRepository
from config.settings import Settings


class SyncDataRefreshService:
    """
    Synchronous wrapper for data refresh operations.

    This provides a sync interface for UI code that needs to call
    refresh operations. It wraps the async DataRefreshService or
    falls back to legacy refresh methods.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)
        self._async_service = None

    def _get_async_service(self):
        """Lazy-load the async service to avoid circular imports."""
        if self._async_service is None:
            try:
                from data.services.data_refresh_service import DataRefreshService
                self._async_service = DataRefreshService(self.db_path, self.logger)
            except ImportError as e:
                self.logger.warning(f"Could not import DataRefreshService: {e}")
        return self._async_service

    def refresh_tables_sync(
        self,
        tables: Optional[List[str]] = None,
        progress_callback=None
    ) -> bool:
        """
        Synchronously refresh data tables.

        Args:
            tables: List of table names to refresh. If None, refreshes all.
            progress_callback: Optional callback for progress updates.

        Returns:
            True if refresh successful, False otherwise.
        """
        try:
            async_service = self._get_async_service()
            if async_service:
                # Run async refresh in event loop
                return asyncio.run(
                    async_service.refresh_tables(tables, progress_callback)
                )
            else:
                # Fallback to legacy method
                from backend.fetch_table import ensure_latest
                ensure_latest(tables=tables, db_path=self.db_path)
                return True
        except Exception as e:
            self.logger.error(f"Error in refresh_tables_sync: {e}", exc_info=True)
            return False

    def refresh_faction_status_only(self) -> bool:
        """
        Refresh only the faction coalition status data.

        Returns:
            True if refresh successful, False otherwise.
        """
        try:
            # Use the repository to load faction status from CSV
            repo = DatabaseRepository(self.db_path, self.logger)
            return repo.load_faction_coalition_status()
        except Exception as e:
            self.logger.error(f"Error refreshing faction status: {e}", exc_info=True)
            return False


class DataService:
    """Service layer for data operations in the UI."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None,
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)

        # Use real repository and sync refresh wrapper
        self.db_repository = DatabaseRepository(self.db_path, self.logger)
        self.refresh_service = SyncDataRefreshService(self.db_path, self.logger)

    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute a database query."""
        return self.db_repository.execute_query(query)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return self.db_repository.table_exists(table_name)

    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        return self.db_repository.get_table_count(table_name)

    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        try:
            return self.db_repository.get_tables()
        except Exception as e:
            self.logger.error(f"Error listing tables: {e}")
            return []

    def get_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a table."""
        try:
            from backend.duckdb_io import DuckDBIO

            db_io = DuckDBIO(self.db_path, self.logger)
            return db_io.get_table_statistics(table_name)
        except ImportError:
            return {}

    def get_available_tables(self) -> List[str]:
        """Get list of available database tables."""
        try:
            return self.db_repository.get_tables()
        except Exception as e:
            self.logger.error(f"Error getting tables: {e}")
            return []

    def refresh_data(
        self, tables: Optional[List[str]] = None, progress_callback=None
    ) -> bool:
        """Refresh data tables."""
        try:
            return self.refresh_service.refresh_tables_sync(tables, progress_callback)
        except Exception as e:
            self.logger.error(f"Error refreshing data: {e}")
            return False

    def refresh_faction_status(self) -> bool:
        """Refresh faction coalition status."""
        return self.refresh_service.refresh_faction_status_only()

    def get_database_summary(self) -> Dict[str, Any]:
        """Get database summary."""
        try:
            from backend.utils import get_database_summary

            return get_database_summary(self.db_path, self.logger)
        except ImportError:
            return {}

    def validate_database(self) -> Dict[str, Any]:
        """Validate database integrity."""
        try:
            from backend.utils import validate_database_integrity

            return validate_database_integrity(self.db_path, self.logger)
        except ImportError:
            return {}

    def get_table_info(self, table_name: str) -> dict:
        """Get information about a specific table."""
        try:
            row_count = self.get_table_count(table_name)
            return {
                "name": table_name,
                "row_count": row_count,
                "exists": self.table_exists(table_name)
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            return {"name": table_name, "row_count": 0, "exists": False}
