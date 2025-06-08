from __future__ import annotations

"""Database repository providing common database operations."""

from pathlib import Path
from typing import Optional, Iterable, List
import logging
import time
import pandas as pd

from backend.connection_manager import get_db_connection, safe_execute_query
from config.settings import Settings


class DatabaseRepository:
    """Encapsulates DuckDB access patterns."""

    COMMON_INDEXES: dict[str, List[str]] = {
        "KNS_PersonToPosition": ["PersonID", "KnessetNum"],
        "KNS_Query": ["PersonID", "KnessetNum"],
        "KNS_Agenda": ["InitiatorPersonID", "KnessetNum"],
    }

    def __init__(self, db_path: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None) -> None:
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)

    # ----------------------- Query Execution -----------------------
    def execute_query(self, query: str, explain: bool = False) -> pd.DataFrame:
        """Execute a SQL query with optional EXPLAIN and timing."""
        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
            if explain:
                plan = conn.execute(f"EXPLAIN {query}").fetchall()
                for row in plan:
                    self.logger.debug(str(row[0]))
            start = time.perf_counter()
            result = safe_execute_query(conn, query, self.logger)
            duration = time.perf_counter() - start
            self.logger.info("Query executed in %.3f sec", duration)
            return result

    def explain_query(self, query: str) -> pd.DataFrame:
        """Return the DuckDB query plan for a SQL statement."""
        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
            return conn.execute(f"EXPLAIN {query}").fetchdf()

    # --------------------------- Tables ---------------------------
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        sql = "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?"
        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
            val = conn.execute(sql, [table_name]).fetchone()[0]
            return val > 0

    def get_table_count(self, table_name: str) -> int:
        """Return the number of rows in a table."""
        sql = f'SELECT COUNT(*) FROM "{table_name}"'
        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
            result = safe_execute_query(conn, sql, self.logger)
            if not result.empty:
                return int(result.iloc[0, 0])
            return 0

    # --------------------------- Indexes ---------------------------
    def create_index(self, table: str, columns: Iterable[str]) -> None:
        """Create an index on the given table/columns if it doesn't exist."""
        cols = ", ".join(f'"{c}"' for c in columns)
        index_name = f"idx_{table}_{'_'.join(columns)}"
        sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}({cols})"
        with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
            conn.execute(sql)
            self.logger.info("Ensured index %s on %s(%s)", index_name, table, cols)

    def ensure_common_indexes(self) -> None:
        """Create indexes for commonly queried columns."""
        for table, cols in self.COMMON_INDEXES.items():
            self.create_index(table, cols)
