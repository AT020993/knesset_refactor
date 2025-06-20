"""Data service for UI layer."""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd


# from data.repositories.database_repository import DatabaseRepository
# Temporary workaround for missing data module
class DatabaseRepository:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_tables(self):
        return []

    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute a database query."""
        return pd.DataFrame()

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return False

    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        return 0


class DataRefreshService:
    """Placeholder data refresh service."""

    def __init__(self, db_path):
        self.db_path = db_path

    def refresh_tables_sync(self, tables=None, progress_callback=None):
        return True

    def refresh_faction_status_only(self):
        return True


from config.settings import Settings


class DataService:
    """Service layer for data operations in the UI."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None,
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)

        # Initialize repositories
        self.db_repository = DatabaseRepository(self.db_path)
        self.refresh_service = DataRefreshService(self.db_path)

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
            from backend.duckdb_io import DuckDBIO

            db_io = DuckDBIO(self.db_path, self.logger)
            return db_io.list_tables()
        except ImportError:
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
            # Placeholder implementation
            return {"name": table_name, "columns": [], "row_count": 0}
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            return {}
