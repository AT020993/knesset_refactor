"""Base chart class for all chart types."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Tuple
import logging
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import duckdb

from backend.connection_manager import get_db_connection, safe_execute_query
from config.charts import ChartConfig
from utils.query_builder import SecureQueryBuilder, FilterOperator, QueryTemplate


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

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """Execute a query safely with optional parameters and return the result."""
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if params:
                    # DuckDB uses ? placeholders, so we need to convert $param_N to ?
                    # and provide values in correct order
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
                                sorted_params.append((999999, param_name, value))  # Put non-numbered at end
                        else:
                            sorted_params.append((999999, param_name, value))
                    
                    sorted_params.sort(key=lambda x: x[0])
                    
                    # Replace $param_name with ? in correct order
                    for _, param_name, value in sorted_params:
                        processed_query = processed_query.replace(f'${param_name}', '?', 1)
                        param_values.append(value)
                    
                    return safe_execute_query(con, processed_query, self.logger, param_values)
                else:
                    return safe_execute_query(con, query, self.logger)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}", exc_info=True)
            st.error(f"Error executing query: {e}")
            return None

    def execute_secure_query(self, query: str, builder: SecureQueryBuilder) -> Optional[pd.DataFrame]:
        """Execute a query with a SecureQueryBuilder's parameters."""
        try:
            params = builder.get_parameters()
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Error executing secure query: {e}", exc_info=True)
            st.error(f"Error executing secure query: {e}")
            return None

    def build_secure_filters(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        table_prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> Tuple[Dict[str, Any], SecureQueryBuilder]:
        """Build common filter conditions using secure parameter binding."""
        builder = SecureQueryBuilder()
        filters = {}

        # Add table prefix with dot if provided
        prefix = f"{table_prefix}." if table_prefix else ""

        # Build Knesset filter
        knesset_condition, is_single = builder.build_knesset_filter(
            knesset_filter, f"{prefix}KnessetNum"
        )
        filters["knesset_condition"] = knesset_condition
        filters["is_single_knesset"] = is_single
        
        if knesset_filter:
            if is_single:
                filters["knesset_title"] = f"Knesset {knesset_filter[0]}"
            else:
                filters["knesset_title"] = f"Knessets: {', '.join(map(str, knesset_filter))}"
        else:
            filters["knesset_title"] = "All Knessets"

        # Build faction filter
        if faction_filter:
            filters["faction_condition"] = builder.build_faction_filter(
                faction_filter, "FactionName"
            )
        else:
            filters["faction_condition"] = "1=1"

        # Add advanced filters with secure parameter binding
        filters["advanced_conditions"] = self._build_secure_advanced_filters(
            builder, prefix, date_column, **kwargs
        )

        return filters, builder

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes for SQL injection prevention."""
        return value.replace("'", "''")

    # Keep legacy methods for backward compatibility
    def build_filters(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        table_prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> Dict[str, Any]:
        """Legacy filter building method - converts secure filters to simple string conditions."""
        # For legacy compatibility, convert parameterized conditions to simple string conditions
        builder = SecureQueryBuilder()
        filters = {}

        # Add table prefix with dot if provided
        prefix = f"{table_prefix}." if table_prefix else ""

        # Build Knesset filter - convert to simple condition
        if knesset_filter:
            if len(knesset_filter) == 1:
                filters["knesset_condition"] = f"{prefix}KnessetNum = {knesset_filter[0]}"
                filters["is_single_knesset"] = True
                filters["knesset_title"] = f"Knesset {knesset_filter[0]}"
            else:
                knesset_str = ", ".join(map(str, knesset_filter))
                filters["knesset_condition"] = f"{prefix}KnessetNum IN ({knesset_str})"
                filters["is_single_knesset"] = False
                filters["knesset_title"] = f"Knessets: {', '.join(map(str, knesset_filter))}"
        else:
            filters["knesset_condition"] = "1=1"
            filters["is_single_knesset"] = False
            filters["knesset_title"] = "All Knessets"

        # Build faction filter - convert to simple condition  
        if faction_filter:
            escaped_factions = [f"'{self._escape_sql_string(f)}'" for f in faction_filter]
            faction_str = ", ".join(escaped_factions)
            filters["faction_condition"] = f"FactionName IN ({faction_str})"
        else:
            filters["faction_condition"] = "1=1"

        # Add advanced filters as named conditions for legacy compatibility
        advanced_conditions = self._build_legacy_advanced_filters(prefix, date_column, **kwargs)
        
        # Create specific named conditions that charts expect
        filters["query_type_condition"] = "1=1"
        filters["query_status_condition"] = "1=1" 
        filters["session_type_condition"] = "1=1"
        filters["agenda_status_condition"] = "1=1"
        filters["bill_type_condition"] = "1=1"
        filters["bill_status_condition"] = "1=1"
        filters["bill_origin_condition"] = "1=1"
        filters["start_date_condition"] = "1=1"
        filters["end_date_condition"] = "1=1"
        
        # Apply actual conditions based on filter types
        query_type_filter = kwargs.get("query_type_filter", [])
        if query_type_filter:
            escaped_types = [f"'{self._escape_sql_string(t)}'" for t in query_type_filter]
            type_str = ", ".join(escaped_types)
            filters["query_type_condition"] = f"{prefix}TypeDesc IN ({type_str})"
            
        query_status_filter = kwargs.get("query_status_filter", [])
        if query_status_filter:
            escaped_statuses = [f"'{self._escape_sql_string(s)}'" for s in query_status_filter]
            status_str = ", ".join(escaped_statuses)
            filters["query_status_condition"] = f's.\"Desc\" IN ({status_str})'
            
        session_type_filter = kwargs.get("session_type_filter", [])
        if session_type_filter:
            escaped_session_types = [f"'{self._escape_sql_string(st)}'" for st in session_type_filter]
            session_str = ", ".join(escaped_session_types)
            filters["session_type_condition"] = f"{prefix}SubTypeDesc IN ({session_str})"
            
        bill_type_filter = kwargs.get("bill_type_filter", [])
        if bill_type_filter:
            escaped_bill_types = [f"'{self._escape_sql_string(bt)}'" for bt in bill_type_filter]
            bill_str = ", ".join(escaped_bill_types)
            filters["bill_type_condition"] = f"{prefix}SubTypeDesc IN ({bill_str})"
            
        bill_status_filter = kwargs.get("bill_status_filter", [])
        if bill_status_filter:
            escaped_bill_statuses = [f"'{self._escape_sql_string(bs)}'" for bs in bill_status_filter]
            bill_status_str = ", ".join(escaped_bill_statuses)
            filters["bill_status_condition"] = f's.\"Desc\" IN ({bill_status_str})'
            
        bill_origin_filter = kwargs.get("bill_origin_filter", "All Bills")
        if bill_origin_filter == "Private Bills Only":
            filters["bill_origin_condition"] = f"{prefix}PrivateNumber IS NOT NULL"
        elif bill_origin_filter == "Governmental Bills Only":
            filters["bill_origin_condition"] = f"{prefix}PrivateNumber IS NULL"
            
        start_date = kwargs.get("start_date")
        if start_date:
            filters["start_date_condition"] = f"{prefix}{date_column} >= '{self._escape_sql_string(str(start_date))}'"
            
        end_date = kwargs.get("end_date")
        if end_date:
            filters["end_date_condition"] = f"date({prefix}{date_column}) <= '{self._escape_sql_string(str(end_date))}'"
            
        # Also add numbered conditions for compatibility
        for i, condition in enumerate(advanced_conditions):
            filters[f"advanced_condition_{i}"] = condition
            
        return filters

    def _build_legacy_advanced_filters(
        self,
        prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> List[str]:
        """Build advanced filter conditions as simple string conditions for legacy compatibility."""
        conditions = []

        # Query-specific filters
        query_type_filter = kwargs.get("query_type_filter", [])
        if query_type_filter:
            escaped_types = [f"'{self._escape_sql_string(t)}'" for t in query_type_filter]
            type_str = ", ".join(escaped_types)
            conditions.append(f"{prefix}TypeDesc IN ({type_str})")

        # Status filters using joined table alias
        query_status_filter = kwargs.get("query_status_filter", [])
        if query_status_filter:
            escaped_statuses = [f"'{self._escape_sql_string(s)}'" for s in query_status_filter]
            status_str = ", ".join(escaped_statuses)
            conditions.append(f's.\"Desc\" IN ({status_str})')

        # Agenda-specific filters
        session_type_filter = kwargs.get("session_type_filter", [])
        if session_type_filter:
            escaped_session_types = [f"'{self._escape_sql_string(st)}'" for st in session_type_filter]
            session_str = ", ".join(escaped_session_types)
            conditions.append(f"{prefix}SubTypeDesc IN ({session_str})")

        # Bill-specific filters
        bill_type_filter = kwargs.get("bill_type_filter", [])
        if bill_type_filter:
            escaped_bill_types = [f"'{self._escape_sql_string(bt)}'" for bt in bill_type_filter]
            bill_str = ", ".join(escaped_bill_types)
            conditions.append(f"{prefix}SubTypeDesc IN ({bill_str})")

        bill_status_filter = kwargs.get("bill_status_filter", [])
        if bill_status_filter:
            escaped_bill_statuses = [f"'{self._escape_sql_string(bs)}'" for bs in bill_status_filter]
            bill_status_str = ", ".join(escaped_bill_statuses)
            conditions.append(f's.\"Desc\" IN ({bill_status_str})')

        # Bill origin filter
        bill_origin_filter = kwargs.get("bill_origin_filter", "All Bills")
        if bill_origin_filter == "Private Bills Only":
            conditions.append(f"{prefix}PrivateNumber IS NOT NULL")
        elif bill_origin_filter == "Governmental Bills Only":
            conditions.append(f"{prefix}PrivateNumber IS NULL")

        # Date filters
        start_date = kwargs.get("start_date")
        if start_date:
            conditions.append(f"{prefix}{date_column} >= '{self._escape_sql_string(str(start_date))}'")

        end_date = kwargs.get("end_date")
        if end_date:
            conditions.append(f"date({prefix}{date_column}) <= '{self._escape_sql_string(str(end_date))}'")

        return conditions

    def _build_secure_advanced_filters(
        self,
        builder: SecureQueryBuilder,
        prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> List[str]:
        """Build advanced filter conditions using secure parameter binding."""
        conditions = []

        # Query-specific filters
        query_type_filter = kwargs.get("query_type_filter", [])
        if query_type_filter:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}TypeDesc", FilterOperator.IN, values=query_type_filter
                )
            )

        # Status filters using joined table alias
        query_status_filter = kwargs.get("query_status_filter", [])
        if query_status_filter:
            conditions.append(
                builder.build_filter_condition(
                    's."Desc"', FilterOperator.IN, values=query_status_filter
                )
            )

        # Agenda-specific filters
        session_type_filter = kwargs.get("session_type_filter", [])
        if session_type_filter:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}SubTypeDesc", FilterOperator.IN, values=session_type_filter
                )
            )

        # Bill-specific filters
        bill_type_filter = kwargs.get("bill_type_filter", [])
        if bill_type_filter:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}SubTypeDesc", FilterOperator.IN, values=bill_type_filter
                )
            )

        bill_status_filter = kwargs.get("bill_status_filter", [])
        if bill_status_filter:
            conditions.append(
                builder.build_filter_condition(
                    's."Desc"', FilterOperator.IN, values=bill_status_filter
                )
            )

        # Bill origin filter
        bill_origin_filter = kwargs.get("bill_origin_filter", "All Bills")
        if bill_origin_filter == "Private Bills Only":
            conditions.append(f"{prefix}PrivateNumber IS NOT NULL")
        elif bill_origin_filter == "Governmental Bills Only":
            conditions.append(f"{prefix}PrivateNumber IS NULL")

        # Date filters with parameter binding
        start_date = kwargs.get("start_date")
        if start_date:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}{date_column}", FilterOperator.GREATER_EQUAL, start_date
                )
            )

        end_date = kwargs.get("end_date")
        if end_date:
            conditions.append(
                builder.build_filter_condition(
                    f"date({prefix}{date_column})", FilterOperator.LESS_EQUAL, end_date
                )
            )

        return conditions

    @abstractmethod
    def generate(self, **kwargs) -> Optional[go.Figure]:
        """Generate the chart. Must be implemented by subclasses."""
        pass
