"""Base chart class for all chart types.

This module provides the BaseChart abstract class and related utilities:
- chart_error_handler: Decorator for standardized error handling
- BaseChart: Abstract base class for all chart generators

The BaseChart class uses mixins to organize functionality:
- ChartDataMixin: Database operations and query execution
- ChartFilterMixin: Filter building for SQL queries
- ChartStylingMixin: Time series helpers, styling, and result handling

The FilterBuilder class has been extracted to ui.queries.filter_builder
for reusability across the codebase.
"""

from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional
import logging

import plotly.graph_objects as go
import streamlit as st

from config.charts import ChartConfig

# Import mixins for organized functionality
from ui.charts.mixins import ChartDataMixin, ChartFilterMixin, ChartStylingMixin

# Import FilterBuilder from its location (re-export for backward compatibility)
from ui.queries.filter_builder import FilterBuilder

# Re-export SecureQueryBuilder and related items for backward compatibility
from utils.query_builder import SecureQueryBuilder, FilterOperator, QueryTemplate


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


class BaseChart(ChartDataMixin, ChartFilterMixin, ChartStylingMixin, ABC):
    """Base class for all chart generators.

    Provides common functionality for database access, query execution,
    filter building, and error handling for all chart types.

    Inherits from three mixins:
    - ChartDataMixin: Database operations (check_database_exists, execute_query, etc.)
    - ChartFilterMixin: Filter building (build_filters, build_secure_filters, etc.)
    - ChartStylingMixin: Styling helpers (get_time_period_config, apply_pie_chart_defaults, etc.)

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
            level: Either "error", "warning", or "info".
        """
        if self._error_handler:
            self._error_handler(message, level)
        elif level == "warning":
            st.warning(message)
        elif level == "info":
            st.info(message)
        else:
            st.error(message)

    @abstractmethod
    def generate(self, **kwargs) -> Optional[go.Figure]:
        """Generate the chart. Must be implemented by subclasses."""
        pass
