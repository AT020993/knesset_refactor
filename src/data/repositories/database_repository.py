"""Database repository for DuckDB operations."""

from pathlib import Path
from typing import Optional
import logging
import pandas as pd

from config.settings import Settings
from backend.connection_manager import get_db_connection, safe_execute_query


class DatabaseRepository:
    """Repository for database operations and storage."""
    
    def __init__(self, db_path: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)
        Settings.ensure_directories()
    
    def store_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """Store a DataFrame as a table in DuckDB."""
        if df.empty:
            self.logger.info(f"Table '{table_name}' is empty, skipping storage.")
            return True
        
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as con:
                con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM df')

            self.logger.info(f"Successfully saved {len(df):,} rows for table '{table_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing '{table_name}' to DuckDB: {e}", exc_info=True)
            return False
    
    def store_as_parquet(self, df: pd.DataFrame, table_name: str) -> bool:
        """Store a DataFrame as a Parquet file."""
        if df.empty:
            return True
        
        parquet_path = Settings.PARQUET_DIR / f"{table_name}.parquet"
        
        try:
            df.to_parquet(parquet_path, compression="zstd", index=False)
            self.logger.info(f"Parquet data for '{table_name}' saved to {parquet_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving '{table_name}' to Parquet: {e}", exc_info=True)
            return False
    
    def store_table(self, df: pd.DataFrame, table_name: str) -> bool:
        """Store a table in both DuckDB and Parquet formats."""
        db_success = self.store_dataframe(df, table_name)
        parquet_success = self.store_as_parquet(df, table_name)
        return db_success and parquet_success
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute a query and return results."""
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = safe_execute_query(conn, query, self.logger)
                if isinstance(result, pd.DataFrame):
                    return result
                return None
        except Exception as e:
            self.logger.error(f"Error executing query: {e}", exc_info=True)
            return None
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Uses parameterized query to prevent SQL injection.
        """
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                # Use parameterized query to prevent SQL injection
                result = conn.execute(
                    "SELECT COUNT(*) as count FROM duckdb_tables() WHERE table_name = ?",
                    [table_name]
                ).fetchdf()
                return not result.empty and result.iloc[0]['count'] > 0
        except Exception as e:
            self.logger.error(f"Error checking if table exists: {e}", exc_info=True)
            return False
    
    def get_table_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        if not self.table_exists(table_name):
            return 0

        query = f'SELECT COUNT(*) as count FROM "{table_name}"'
        result = self.execute_query(query)
        return result.iloc[0]['count'] if result is not None and not result.empty else 0

    def get_tables(self) -> list:
        """Get list of all tables in the database."""
        query = "SELECT table_name FROM duckdb_tables() ORDER BY table_name"
        result = self.execute_query(query)
        if result is not None and not result.empty:
            return result['table_name'].tolist()
        return []
    
    def load_faction_coalition_status(self) -> bool:
        """Load faction coalition status from CSV file."""
        status_file = Settings.FACTION_COALITION_STATUS_FILE
        
        if not status_file.exists():
            self.logger.info(f"Faction coalition status file not found: {status_file}")
            return self._create_empty_faction_status_table()
        
        try:
            # Read CSV with proper data types
            df = pd.read_csv(
                status_file,
                dtype={
                    'KnessetNum': 'Int64',
                    'FactionID': 'Int64',
                    'FactionName': 'string',
                    'CoalitionStatus': 'string',
                    'NewFactionName': 'string',
                },
                parse_dates=['DateJoinedCoalition', 'DateLeftCoalition']
            )
            
            return self.store_dataframe(df, "UserFactionCoalitionStatus")
            
        except Exception as e:
            self.logger.error(f"Error loading faction coalition status: {e}", exc_info=True)
            return False
    
    def _create_empty_faction_status_table(self) -> bool:
        """Create an empty faction status table."""
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as con:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS "UserFactionCoalitionStatus" (
                        KnessetNum INTEGER,
                        FactionID INTEGER,
                        FactionName VARCHAR,
                        CoalitionStatus VARCHAR,
                        NewFactionName VARCHAR,
                        DateJoinedCoalition DATE,
                        DateLeftCoalition DATE
                    )
                """)
            self.logger.info("Created empty UserFactionCoalitionStatus table")
            return True
        except Exception as e:
            self.logger.error(f"Error creating empty faction status table: {e}", exc_info=True)
            return False
