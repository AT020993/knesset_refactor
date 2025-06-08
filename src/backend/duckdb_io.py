"""DuckDB I/O utilities and helper functions."""

from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import logging
import pandas as pd
import duckdb

from backend.connection_manager import get_db_connection, safe_execute_query


class DuckDBIO:
    """Utilities for DuckDB input/output operations."""
    
    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
    
    def export_table_to_csv(self, table_name: str, output_path: Path, delimiter: str = ',') -> bool:
        """Export a table to CSV format."""
        try:
            query = f'COPY "{table_name}" TO \'{output_path}\' (DELIMITER \'{delimiter}\', HEADER)'
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                conn.execute(query)
            
            self.logger.info(f"Exported table {table_name} to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting table {table_name}: {e}", exc_info=True)
            return False
    
    def export_table_to_parquet(self, table_name: str, output_path: Path) -> bool:
        """Export a table to Parquet format."""
        try:
            query = f'COPY "{table_name}" TO \'{output_path}\' (FORMAT PARQUET)'
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                conn.execute(query)
            
            self.logger.info(f"Exported table {table_name} to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting table {table_name}: {e}", exc_info=True)
            return False
    
    def import_csv_to_table(self, csv_path: Path, table_name: str, delimiter: str = ',') -> bool:
        """Import CSV file into a table."""
        try:
            query = f"""
            CREATE OR REPLACE TABLE "{table_name}" AS 
            SELECT * FROM read_csv_auto('{csv_path}', delim='{delimiter}')
            """
            
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                conn.execute(query)
            
            self.logger.info(f"Imported CSV {csv_path} to table {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error importing CSV {csv_path}: {e}", exc_info=True)
            return False
    
    def get_table_info(self, table_name: str) -> Optional[pd.DataFrame]:
        """Get table schema information."""
        try:
            query = f'DESCRIBE "{table_name}"'
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                return safe_execute_query(conn, query, self.logger)
                
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}", exc_info=True)
            return None
    
    def get_table_sample(self, table_name: str, sample_size: int = 100) -> Optional[pd.DataFrame]:
        """Get a sample of rows from a table."""
        try:
            query = f'SELECT * FROM "{table_name}" LIMIT {sample_size}'
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                return safe_execute_query(conn, query, self.logger)
                
        except Exception as e:
            self.logger.error(f"Error getting sample from {table_name}: {e}", exc_info=True)
            return None
    
    def get_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """Get basic statistics for a table."""
        stats = {
            'table_name': table_name,
            'row_count': 0,
            'column_count': 0,
            'columns': [],
            'size_bytes': 0
        }
        
        try:
            # Get row count
            count_query = f'SELECT COUNT(*) as count FROM "{table_name}"'
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                count_result = safe_execute_query(conn, count_query, self.logger)
                if count_result is not None and not count_result.empty:
                    stats['row_count'] = count_result.iloc[0]['count']
                
                # Get column info
                info_result = safe_execute_query(conn, f'DESCRIBE "{table_name}"', self.logger)
                if info_result is not None and not info_result.empty:
                    stats['column_count'] = len(info_result)
                    stats['columns'] = info_result['column_name'].tolist() if 'column_name' in info_result.columns else []
                
                # Get table size (approximate)
                size_query = f"""
                SELECT 
                    pg_total_relation_size('{table_name}') as size_bytes
                FROM (SELECT 1) t
                WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')
                """
                
                # Note: DuckDB doesn't have pg_total_relation_size, this is a placeholder
                # In practice, you might need to estimate size differently
                
        except Exception as e:
            self.logger.error(f"Error getting statistics for {table_name}: {e}", exc_info=True)
        
        return stats
    
    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        try:
            query = "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = safe_execute_query(conn, query, self.logger)
                return result['table_name'].tolist() if result is not None and not result.empty else []
                
        except Exception as e:
            self.logger.error(f"Error listing tables: {e}", exc_info=True)
            return []
    
    def vacuum_database(self) -> bool:
        """Optimize the database by running VACUUM."""
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                conn.execute("VACUUM")
            
            self.logger.info("Database vacuum completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during vacuum: {e}", exc_info=True)
            return False
    
    def analyze_database(self) -> bool:
        """Update database statistics by running ANALYZE."""
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                conn.execute("ANALYZE")
            
            self.logger.info("Database analyze completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during analyze: {e}", exc_info=True)
            return False