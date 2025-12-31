"""Base chart class for all chart types.

This module provides the BaseChart abstract class and related utilities:
- chart_error_handler: Decorator for standardized error handling
- BaseChart: Abstract base class for all chart generators

The FilterBuilder class has been extracted to ui.queries.filter_builder
for reusability across the codebase.
"""

from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Tuple
import logging
import json
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import duckdb

from backend.connection_manager import get_db_connection, safe_execute_query
from config.charts import ChartConfig
from utils.query_builder import SecureQueryBuilder, FilterOperator, QueryTemplate

# Import FilterBuilder from its new location
from ui.queries.filter_builder import FilterBuilder


def chart_error_handler(chart_name: str):
    """Decorator for standardized chart error handling.

    Wraps chart generation methods to catch exceptions and provide
    consistent error logging and user feedback.

    Uses the instance's show_error() method for testability - in tests,
    a custom error handler can be injected to capture errors without Streamlit.

    Args:
        chart_name: Human-readable name of the chart for error messages.

    Returns:
        Decorator function that wraps the chart method.

    Example:
        @chart_error_handler("queries per faction chart")
        def plot_queries_per_faction(self, ...):
            # chart logic here (no try/except needed)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self.logger.error(f"Error generating {chart_name}: {e}", exc_info=True)
                # Use instance's show_error for testability
                if hasattr(self, 'show_error'):
                    self.show_error(f"Could not generate {chart_name}: {e}")
                else:
                    st.error(f"Could not generate {chart_name}: {e}")
                return None
        return wrapper
    return decorator


# FilterBuilder is now imported from ui.queries.filter_builder
# Re-exported here for backward compatibility


class BaseChart(ABC):
    """Base class for all chart generators.

    Provides common functionality for database access, query execution,
    filter building, and error handling for all chart types.

    The error_handler parameter allows decoupling from Streamlit for testing:
        - In production: Uses st.error()/st.warning() by default
        - In tests: Pass a custom handler to capture errors without Streamlit

    Example:
        # Production usage (default Streamlit handler)
        chart = MyChart(db_path, logger)

        # Test usage (custom handler)
        errors = []
        chart = MyChart(db_path, logger, error_handler=lambda msg, lvl: errors.append((msg, lvl)))
    """

    def __init__(
        self,
        db_path: Path,
        logger_obj: logging.Logger,
        error_handler: Optional[Callable[[str, str], None]] = None
    ):
        """Initialize the chart generator.

        Args:
            db_path: Path to the DuckDB database file.
            logger_obj: Logger instance for this chart.
            error_handler: Optional callback for error/warning display.
                           Signature: (message: str, level: str) -> None
                           where level is "error" or "warning".
                           If None, uses Streamlit st.error()/st.warning().
        """
        self.db_path = db_path
        self.logger = logger_obj
        self.config = ChartConfig()
        self._error_handler = error_handler

    def show_error(self, message: str, level: str = "error") -> None:
        """Display an error or warning message.

        Uses the custom error_handler if provided, otherwise falls back to Streamlit.

        Args:
            message: The message to display.
            level: Either "error" or "warning".
        """
        if self._error_handler:
            self._error_handler(message, level)
        elif level == "warning":
            st.warning(message)
        else:
            st.error(message)

    def check_database_exists(self) -> bool:
        """Check if database file exists."""
        if not self.db_path.exists():
            self.show_error("Database not found. Cannot generate visualization.")
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

    @staticmethod
    def ensure_numeric_columns(
        df: pd.DataFrame,
        columns: List[str],
        fillna: Any = 0
    ) -> pd.DataFrame:
        """Convert columns to numeric type with safe error handling.

        Reduces code duplication for the common pattern:
            df["Col"] = pd.to_numeric(df["Col"], errors="coerce").fillna(0)

        Args:
            df: The DataFrame to modify.
            columns: List of column names to convert.
            fillna: Value to use for NaN values (default: 0).

        Returns:
            DataFrame with specified columns converted to numeric.
        """
        df = df.copy()
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(fillna)
        return df

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """Execute a query safely with optional parameters and return the result."""
        return self._execute_query_cached(query, json.dumps(params) if params else None)

    @st.cache_data(ttl=600, show_spinner=False)
    def _execute_query_cached(_self, query: str, params_str: Optional[str]) -> Optional[pd.DataFrame]:
        """Cached query execution to avoid redundant database queries."""
        try:
            # Use json.loads instead of eval for security
            params = json.loads(params_str) if params_str else None
            with get_db_connection(
                _self.db_path, read_only=True, logger_obj=_self.logger
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

                    return safe_execute_query(con, processed_query, _self.logger, param_values)
                else:
                    return safe_execute_query(con, query, _self.logger)
        except Exception as e:
            _self.logger.error(f"Error executing query: {e}", exc_info=True)
            _self.show_error(f"Error executing query: {e}")
            return None

    def execute_secure_query(self, query: str, builder: SecureQueryBuilder) -> Optional[pd.DataFrame]:
        """Execute a query with a SecureQueryBuilder's parameters."""
        try:
            params = builder.get_parameters()
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Error executing secure query: {e}", exc_info=True)
            self.show_error(f"Error executing secure query: {e}")
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

    def build_filters(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        table_prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> Dict[str, Any]:
        """Build common filter conditions for SQL queries.

        Uses FilterBuilder internally for consistent filter generation.

        Args:
            knesset_filter: List of Knesset numbers to filter by.
            faction_filter: List of faction names to filter by.
            table_prefix: Table alias prefix (e.g., "q" for "q.KnessetNum").
            date_column: Column name for date filters (default: "SubmitDate").
            **kwargs: Additional filter parameters (query_type_filter, bill_status_filter, etc.)

        Returns:
            Dictionary with all filter conditions and metadata.
        """
        # Delegate to FilterBuilder for consistent filter generation
        builder = FilterBuilder(table_prefix=table_prefix, date_column=date_column)
        builder.add_knesset(knesset_filter).add_faction(faction_filter).from_kwargs(**kwargs)
        filters = builder.build()

        # Add agenda_status_condition for backward compatibility
        filters["agenda_status_condition"] = "1=1"

        return filters

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

    # ============== Time Series Helpers ==============

    @staticmethod
    def get_time_period_config(date_column: str) -> Dict[str, Dict[str, str]]:
        """Return time period SQL configurations for time series charts.

        Consolidates the duplicated time_configs dict that was repeated
        in plot_queries_by_time_period, plot_agendas_by_time_period, and
        plot_bills_by_time_period.

        Args:
            date_column: The date column to use in SQL (e.g., "q.SubmitDate")

        Returns:
            Dict with Monthly, Quarterly, Yearly configurations containing
            'sql' (SQL expression) and 'label' (axis label) keys.
        """
        return {
            "Monthly": {
                "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y-%m')",
                "label": "Year-Month"
            },
            "Quarterly": {
                "sql": (
                    f"strftime(CAST({date_column} AS TIMESTAMP), '%Y') || '-Q' || "
                    f"CAST((CAST(strftime(CAST({date_column} AS TIMESTAMP), '%m') AS INTEGER) - 1) / 3 + 1 AS VARCHAR)"
                ),
                "label": "Year-Quarter"
            },
            "Yearly": {
                "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y')",
                "label": "Year"
            }
        }

    @staticmethod
    def normalize_time_series_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame columns for time series charts.

        Converts KnessetNum and TimePeriod columns to strings for proper
        chart rendering.

        Args:
            df: DataFrame with time series data.

        Returns:
            DataFrame with normalized column types.
        """
        df = df.copy()
        if "KnessetNum" in df.columns:
            df["KnessetNum"] = df["KnessetNum"].astype(str)
        if "TimePeriod" in df.columns:
            df["TimePeriod"] = df["TimePeriod"].astype(str)
        return df

    # ============== Result Handling Helpers ==============

    def handle_empty_result(
        self,
        df: pd.DataFrame,
        entity_type: str,
        filters: Dict[str, Any],
        chart_context: str = ""
    ) -> bool:
        """Check if DataFrame is empty and show appropriate message.

        Args:
            df: The DataFrame to check.
            entity_type: Type of data (e.g., "query", "agenda", "bill").
            filters: Filter dict containing 'knesset_title'.
            chart_context: Additional context for the message (e.g., "by Year").

        Returns:
            True if DataFrame is empty (caller should return None), False otherwise.
        """
        if df.empty:
            context_str = f" to visualize '{chart_context}'" if chart_context else ""
            message = f"No {entity_type} data found for '{filters.get('knesset_title', 'selected filters')}'{context_str} with the current filters."
            self.show_error(message, level="info")
            return True
        return False

    # ============== Chart Styling Helpers ==============

    @staticmethod
    def apply_pie_chart_defaults(fig: go.Figure) -> go.Figure:
        """Apply standard pie chart styling.

        Consolidates the repeated pattern:
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(title_x=0.5)

        Args:
            fig: Plotly Figure to style.

        Returns:
            Styled Figure.
        """
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(title_x=0.5)
        return fig

    @abstractmethod
    def generate(self, **kwargs) -> Optional[go.Figure]:
        """Generate the chart. Must be implemented by subclasses."""
        pass
