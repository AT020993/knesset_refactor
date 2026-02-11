"""Chart data mixin for database operations and query execution.

This mixin provides database-related functionality for chart classes:
- Database existence checking
- Table existence validation
- Query execution with caching
- Secure query execution with parameter binding
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from utils.performance_utils import optimize_dataframe_dtypes
from utils.query_builder import SecureQueryBuilder


class ChartDataMixin:
    """Mixin providing database operations and query execution for charts.

    Requires the following attributes from the host class:
    - db_path: Path to the database file
    - logger: logging.Logger instance
    - show_error: method to display errors

    This mixin handles:
    - Checking if the database file exists
    - Validating required tables exist in the database
    - Executing SQL queries with optional caching
    - Executing parameterized queries securely
    """

    # Type hints for required attributes from host class
    db_path: Path
    logger: logging.Logger

    def show_error(self, message: str, level: str = "error") -> None:
        """Display error - must be implemented by host class."""
        raise NotImplementedError("Host class must implement show_error")

    def check_database_exists(self) -> bool:
        """Check if database file exists.

        Returns:
            True if database exists, False otherwise.
        """
        if not self.db_path.exists():
            self.show_error("Database not found. Cannot generate visualization.")
            self.logger.error(f"Database not found: {self.db_path}")
            return False
        return True

    def check_tables_exist(
        self, con: duckdb.DuckDBPyConnection, required_tables: List[str]
    ) -> bool:
        """Check if all required tables exist in the database.

        Args:
            con: Active DuckDB connection.
            required_tables: List of table names that must exist.

        Returns:
            True if all tables exist, False otherwise.
        """
        try:
            db_tables_df = con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name='main';"
            ).df()
            db_tables_list = db_tables_df["table_name"].str.lower().tolist()
            missing_tables = [
                table
                for table in required_tables
                if table.lower() not in db_tables_list
            ]

            if missing_tables:
                self.show_error(
                    f"Visualization skipped: Required table(s) '{', '.join(missing_tables)}' not found. Please refresh data.",
                    level="warning"
                )
                self.logger.warning(
                    f"Required table(s) '{', '.join(missing_tables)}' not found for visualization."
                )
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}", exc_info=True)
            self.show_error(f"Error checking table existence: {e}")
            return False

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[pd.DataFrame]:
        """Execute a query safely with optional parameters.

        Args:
            query: SQL query to execute.
            params: Optional dictionary of parameters for parameterized queries.

        Returns:
            DataFrame with query results, or None on error.
        """
        return self._execute_query_cached(query, json.dumps(params) if params else None)

    @st.cache_data(ttl=1800, show_spinner=False)
    def _execute_query_cached(
        _self, query: str, params_str: Optional[str]
    ) -> Optional[pd.DataFrame]:
        """Cached query execution to avoid redundant database queries.

        Uses Streamlit's caching mechanism with a 10-minute TTL.

        Args:
            query: SQL query to execute.
            params_str: JSON-serialized parameters (for cache key stability).

        Returns:
            DataFrame with query results, or None on error.
        """
        try:
            params = json.loads(params_str) if params_str else None
            with get_db_connection(
                _self.db_path, read_only=True, logger_obj=_self.logger
            ) as con:
                if params:
                    # DuckDB uses ? placeholders, convert $param_N format
                    processed_query = query
                    param_values = []

                    # Sort parameters by their number to maintain order
                    sorted_params = []
                    for param_name, value in params.items():
                        if param_name.startswith('param_'):
                            try:
                                param_num = int(param_name.split('_')[1])
                                sorted_params.append((param_num, param_name, value))
                            except (ValueError, IndexError):
                                sorted_params.append((999999, param_name, value))
                        else:
                            sorted_params.append((999999, param_name, value))

                    sorted_params.sort(key=lambda x: x[0])

                    # Replace $param_name with ? in correct order
                    for _, param_name, value in sorted_params:
                        processed_query = processed_query.replace(f'${param_name}', '?', 1)
                        param_values.append(value)

                    result = safe_execute_query(
                        con, processed_query, _self.logger, param_values
                    )
                    if isinstance(result, pd.DataFrame) and len(result) > 1000:
                        return optimize_dataframe_dtypes(result)
                    return result if isinstance(result, pd.DataFrame) else None
                else:
                    result = safe_execute_query(con, query, _self.logger)
                    if isinstance(result, pd.DataFrame) and len(result) > 1000:
                        return optimize_dataframe_dtypes(result)
                    return result if isinstance(result, pd.DataFrame) else None
        except Exception as e:
            _self.logger.error(f"Error executing query: {e}", exc_info=True)
            _self.show_error(f"Error executing query: {e}")
            return None

    def execute_secure_query(
        self, query: str, builder: SecureQueryBuilder
    ) -> Optional[pd.DataFrame]:
        """Execute a query with a SecureQueryBuilder's parameters.

        Args:
            query: SQL query with parameter placeholders.
            builder: SecureQueryBuilder instance containing parameters.

        Returns:
            DataFrame with query results, or None on error.
        """
        try:
            params = builder.get_parameters()
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Error executing secure query: {e}", exc_info=True)
            self.show_error(f"Error executing secure query: {e}")
            return None
