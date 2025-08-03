"""Chart factory for creating different chart types."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import plotly.graph_objects as go

from .comparison import ComparisonCharts
from .distribution import DistributionCharts
from .time_series import TimeSeriesCharts


class ChartFactory:
    """Factory class for creating different types of charts."""

    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj

        # Initialize chart generators
        self._generators = {
            "time_series": TimeSeriesCharts(db_path, logger_obj),
            "distribution": DistributionCharts(db_path, logger_obj),
            "comparison": ComparisonCharts(db_path, logger_obj),
        }

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
        generator = self._generators.get(chart_category)
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
                "query_status_distribution",
                "query_status_by_faction",
                "agenda_status_distribution",
                "bill_status_distribution",
                "bill_subtype_distribution",
            ],
            "comparison": [
                "queries_per_faction",
                "queries_by_coalition_status",
                "queries_by_ministry",
                "agendas_per_faction",
                "agendas_by_coalition_status",
                "bills_per_faction",
                "bills_by_coalition_status",
                "top_bill_initiators",
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
        return self.create_chart("distribution", "query_status_by_faction", **kwargs)

    def plot_agendas_per_faction_in_knesset(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("comparison", "agendas_per_faction", **kwargs)

    def plot_agendas_by_coalition_status(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart("comparison", "agendas_by_coalition_status", **kwargs)
