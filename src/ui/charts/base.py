"""Base chart class for all chart types."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
import logging
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import duckdb

from backend.connection_manager import get_db_connection, safe_execute_query
from config.charts import ChartConfig


class BaseChart(ABC):
    """Base class for all chart generators."""

    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj
        self.config = ChartConfig()

    def check_database_exists(self) -> bool:
        """Check if database file exists."""
        if not self.db_path.exists():
            st.error("Database not found. Cannot generate visualization.")
            self.logger.error(f"Database not found: {self.db_path}")
            return False
        return True

    def check_tables_exist(
        self, con: duckdb.DuckDBPyConnection, required_tables: List[str]
    ) -> bool:
        """Check if all required tables exist in the database."""
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
                st.warning(
                    f"Visualization skipped: Required table(s) '{', '.join(missing_tables)}' not found. Please refresh data."
                )
                self.logger.warning(
                    f"Required table(s) '{', '.join(missing_tables)}' not found for visualization."
                )
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}", exc_info=True)
            st.error(f"Error checking table existence: {e}")
            return False

    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute a query safely and return the result."""
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                return safe_execute_query(con, query, self.logger)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}", exc_info=True)
            st.error(f"Error executing query: {e}")
            return None

    def build_filters(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        table_prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> Dict[str, Any]:
        """Build common filter conditions."""
        filters = {}

        # Add table prefix with dot if provided
        prefix = f"{table_prefix}." if table_prefix else ""

        if knesset_filter:
            filters["knesset_condition"] = (
                f"{prefix}KnessetNum IN ({','.join(map(str, knesset_filter))})"
            )
            if len(knesset_filter) == 1:
                filters["is_single_knesset"] = True
                filters["knesset_title"] = f"Knesset {knesset_filter[0]}"
            else:
                filters["is_single_knesset"] = False
                filters["knesset_title"] = (
                    f"Knessets: {', '.join(map(str, knesset_filter))}"
                )
        else:
            filters["knesset_condition"] = "1=1"  # No filter
            filters["is_single_knesset"] = False
            filters["knesset_title"] = "All Knessets"

        if faction_filter:
            faction_list = ", ".join(
                [f"'{self._escape_sql_string(f)}'" for f in faction_filter]
            )
            filters["faction_condition"] = f"FactionName IN ({faction_list})"
        else:
            filters["faction_condition"] = "1=1"  # No filter

        # Add advanced filters
        self._add_advanced_filters(filters, prefix, date_column=date_column, **kwargs)

        return filters

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes for SQL injection prevention."""
        return value.replace("'", "''")

    def _build_in_clause(self, field: str, values: List[str]) -> str:
        """Safely build an SQL IN clause for a list of string values."""
        if not values:
            return "1=1"
        # Only allow alphanumeric and basic punctuation to prevent injection
        safe_values = [self._escape_sql_string(str(v)) for v in values]
        quoted = ", ".join([f"'" + v + "'" for v in safe_values])
        return f"{field} IN ({quoted})"

    def _add_advanced_filters(
        self,
        filters: Dict[str, Any],
        prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> None:
        """Add advanced filter conditions based on provided kwargs."""

        # Query-specific filters
        query_type_filter = kwargs.get("query_type_filter", [])
        filters["query_type_condition"] = self._build_in_clause(
            f"{prefix}TypeDesc", query_type_filter
        )

        # Status filters should use s."Desc" (from joined KNS_Status table)
        query_status_filter = kwargs.get("query_status_filter", [])
        filters["query_status_condition"] = self._build_in_clause(
            's."Desc"', query_status_filter
        )

        # Agenda-specific filters - use SubTypeDesc instead of SessionType
        session_type_filter = kwargs.get("session_type_filter", [])
        filters["session_type_condition"] = self._build_in_clause(
            f"{prefix}SubTypeDesc", session_type_filter
        )

        agenda_status_filter = kwargs.get("agenda_status_filter", [])
        filters["agenda_status_condition"] = self._build_in_clause(
            's."Desc"', agenda_status_filter
        )

        # Bill-specific filters - use SubTypeDesc instead of BillTypeDesc
        bill_type_filter = kwargs.get("bill_type_filter", [])
        filters["bill_type_condition"] = self._build_in_clause(
            f"{prefix}SubTypeDesc", bill_type_filter
        )

        bill_status_filter = kwargs.get("bill_status_filter", [])
        filters["bill_status_condition"] = self._build_in_clause(
            's."Desc"', bill_status_filter
        )

        # Bill origin filter (Private vs Governmental)
        bill_origin_filter = kwargs.get("bill_origin_filter", "All Bills")
        if bill_origin_filter == "Private Bills Only":
            filters["bill_origin_condition"] = f"{prefix}PrivateNumber IS NOT NULL"
        elif bill_origin_filter == "Governmental Bills Only":
            filters["bill_origin_condition"] = f"{prefix}PrivateNumber IS NULL"
        else:
            filters["bill_origin_condition"] = "1=1"  # No filter

        # Date filters
        start_date = kwargs.get("start_date")
        if start_date:
            filters["start_date_condition"] = (
                f"{prefix}{date_column} >= '"
                + self._escape_sql_string(start_date)
                + "'"
            )
        else:
            filters["start_date_condition"] = "1=1"

        end_date = kwargs.get("end_date")
        if end_date:
            filters["end_date_condition"] = (
                f"date({prefix}{date_column}) <= '"
                + self._escape_sql_string(end_date)
                + "'"
            )
        else:
            filters["end_date_condition"] = "1=1"

    @abstractmethod
    def generate(self, **kwargs) -> Optional[go.Figure]:
        """Generate the chart. Must be implemented by subclasses."""
        pass
