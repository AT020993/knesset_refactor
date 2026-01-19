"""Chart factory for creating different chart types.

This module provides a factory pattern for creating charts without
tight coupling to Streamlit. Caching is applied conditionally
when Streamlit is available.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import plotly.graph_objects as go

from .comparison import ComparisonCharts
from .distribution import DistributionCharts
from .time_series import TimeSeriesCharts
from .network import NetworkCharts


def _get_cache_resource_decorator():
    """Get a cache decorator that uses Streamlit if available, otherwise no-op.

    This allows the factory to work without Streamlit in CLI or test contexts.
    """
    try:
        import streamlit as st
        if hasattr(st, "cache_resource"):
            return st.cache_resource(show_spinner=False)
    except ImportError:
        pass
    # Return identity decorator if Streamlit not available
    return lambda func: func


# Create the cached function at module level
# The decorator is applied at import time, using Streamlit if available
@_get_cache_resource_decorator()
def _create_chart_generators(db_path_str: str) -> Dict:
    """Create chart generator instances (cached when Streamlit available).

    Args:
        db_path_str: String path to database (string for cache key hashability).

    Returns:
        Dictionary mapping category names to chart generator instances.
    """
    db_path = Path(db_path_str)
    logger = logging.getLogger("knesset.ui.charts.factory")

    return {
        "time_series": TimeSeriesCharts(db_path, logger),
        "distribution": DistributionCharts(db_path, logger),
        "comparison": ComparisonCharts(db_path, logger),
        "network": NetworkCharts(db_path, logger),
    }


class ChartFactory:
    """Factory class for creating different types of charts.

    Uses conditional caching: When Streamlit is available, chart generators
    are cached to avoid redundant instantiation. In CLI/test contexts,
    generators are created fresh each time.
    """

    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        """Initialize the chart factory.

        Args:
            db_path: Path to the DuckDB database file.
            logger_obj: Logger instance for error reporting.
        """
        self.db_path = db_path
        self.logger = logger_obj
        self._generators = None

    @property
    def generators(self) -> Dict:
        """Lazy-load chart generators with caching."""
        if self._generators is None:
            self._generators = _create_chart_generators(str(self.db_path))
        return self._generators

    def create_chart(
        self, chart_category: str, chart_type: str, **kwargs
    ) -> Optional[go.Figure]:
        """
        Create a chart of the specified type.

        Args:
            chart_category: Category of chart (time_series, distribution, etc.)
            chart_type: Specific chart type within the category
            **kwargs: Chart-specific parameters

        Returns:
            Plotly figure or None if error
        """
        generator = self.generators.get(chart_category)
        if not generator:
            self.logger.warning(f"Unknown chart category: {chart_category}")
            return None

        try:
            return generator.generate(chart_type, **kwargs)
        except Exception as e:
            self.logger.error(
                f"Error creating chart {chart_category}.{chart_type}: {e}",
                exc_info=True,
            )
            return None

    def get_available_charts(self) -> Dict[str, list]:
        """Get list of available chart types by category."""
        return {
            "time_series": ["queries_by_time", "agendas_by_time", "bills_by_time"],
            "distribution": [
                "query_types_distribution",
                "agenda_classifications_pie",
                # "query_status_distribution",  # TODO: Not yet implemented
                "agenda_status_distribution",
                "bill_subtype_distribution",
            ],
            "comparison": [
                "queries_per_faction",
                # "queries_by_coalition_status",  # TODO: Not yet implemented
                "queries_by_ministry",
                "query_status_by_faction",
                "agendas_per_faction",
                "agendas_by_coalition_status",
                "bills_per_faction",
                "bills_by_coalition_status",
                "top_bill_initiators",
            ],
            "network": [
                "mk_collaboration_network",
                "faction_collaboration_network",
                "faction_collaboration_matrix",
                "faction_coalition_breakdown",
            ],
        }

    # Legacy compatibility methods for existing code
    def plot_queries_by_time_period(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("time_series", "queries_by_time", **kwargs)

    def plot_query_types_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("distribution", "query_types_distribution", **kwargs)

    def plot_queries_per_faction_in_knesset(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("comparison", "queries_per_faction", **kwargs)

    def plot_query_status_by_faction(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("comparison", "query_status_by_faction", **kwargs)

    def plot_agendas_per_faction_in_knesset(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("comparison", "agendas_per_faction", **kwargs)

    def plot_agendas_by_coalition_status(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("comparison", "agendas_by_coalition_status", **kwargs)
