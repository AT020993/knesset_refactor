"""Data service for UI layer."""

from pathlib import Path
from typing import Optional, List, Dict, Any
import logging
import pandas as pd

from data.repositories.database_repository import DatabaseRepository
from data.services.data_refresh_service import DataRefreshService
from config.settings import Settings


class DataService:
    """Service layer for data operations in the UI."""
    
    def __init__(self, db_path: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)
        
        self.db_repository = DatabaseRepository(self.db_path, self.logger)
        self.refresh_service = DataRefreshService(self.db_path, self.logger)
    
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
        from backend.duckdb_io import DuckDBIO
        db_io = DuckDBIO(self.db_path, self.logger)
        return db_io.list_tables()
    
    def get_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a table."""
        from backend.duckdb_io import DuckDBIO
        db_io = DuckDBIO(self.db_path, self.logger)
        return db_io.get_table_statistics(table_name)
    
    def refresh_data(self, tables: Optional[List[str]] = None, progress_callback=None) -> bool:
        """Refresh data from OData API."""
        return self.refresh_service.refresh_tables_sync(tables, progress_callback)
    
    def refresh_faction_status(self) -> bool:
        """Refresh faction coalition status."""
        return self.refresh_service.refresh_faction_status_only()
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get database summary."""
        from backend.utils import get_database_summary
        return get_database_summary(self.db_path, self.logger)
    
    def validate_database(self) -> Dict[str, Any]:
        """Validate database integrity."""
        from backend.utils import validate_database_integrity
        return validate_database_integrity(self.db_path, self.logger)