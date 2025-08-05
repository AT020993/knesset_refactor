"""Chart service for UI layer."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import plotly.graph_objects as go

from config.settings import Settings
from ui.charts.factory import ChartFactory


class ChartService:
    """Service layer for chart generation in the UI."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None,
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)
        self.chart_factory = ChartFactory(self.db_path, self.logger)

    def create_chart(
        self, chart_category: str, chart_type: str, **kwargs
    ) -> Optional[go.Figure]:
        """Create a chart using the chart factory."""
        return self.chart_factory.create_chart(chart_category, chart_type, **kwargs)

    def get_available_charts(self) -> Dict[str, list]:
        """Get available chart types."""
        return self.chart_factory.get_available_charts()

    # Legacy compatibility methods for existing UI code
    def plot_queries_by_time_period(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for queries by time period."""
        return self.chart_factory.plot_queries_by_time_period(**kwargs)

    def plot_query_types_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for query types distribution."""
        return self.chart_factory.plot_query_types_distribution(**kwargs)

    def plot_queries_per_faction_in_knesset(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for queries per faction."""
        return self.chart_factory.plot_queries_per_faction_in_knesset(**kwargs)

    def plot_query_status_by_faction(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for query status by faction."""
        return self.chart_factory.plot_query_status_by_faction(**kwargs)

    # Agenda chart methods
    def plot_agendas_by_time_period(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for agendas by time period."""
        return self.create_chart("time_series", "agendas_by_time", **kwargs)

    def plot_agenda_classifications_pie(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for agenda classifications pie chart."""
        return self.create_chart("distribution", "agenda_classifications_pie", **kwargs)

    def plot_agenda_status_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for agenda status distribution."""
        return self.create_chart("distribution", "agenda_status_distribution", **kwargs)

    def plot_agendas_per_faction(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for agendas per faction."""
        return self.create_chart("comparison", "agendas_per_faction", **kwargs)

    def plot_agendas_by_coalition_status(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for agendas by coalition status."""
        return self.create_chart("comparison", "agendas_by_coalition_status", **kwargs)

    # Bill chart methods
    def plot_bill_status_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for bill status distribution."""
        return self.create_chart("distribution", "bill_status_distribution", **kwargs)

    def plot_bills_by_time_period(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for bills by time period."""
        return self.create_chart("time_series", "bills_by_time", **kwargs)

    def plot_bill_subtype_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for bill subtype distribution."""
        return self.create_chart("distribution", "bill_subtype_distribution", **kwargs)

    def plot_bills_per_faction(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for bills per faction."""
        return self.create_chart("comparison", "bills_per_faction", **kwargs)

    def plot_bills_by_coalition_status(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for bills by coalition status."""
        return self.create_chart("comparison", "bills_by_coalition_status", **kwargs)

    def plot_top_bill_initiators(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for top bill initiators."""
        return self.create_chart("comparison", "top_bill_initiators", **kwargs)

    def plot_bill_initiators_by_faction(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for bill initiators by faction."""
        return self.create_chart("comparison", "bill_initiators_by_faction", **kwargs)

    def plot_total_bills_per_faction(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for total bills per faction."""
        return self.create_chart("comparison", "total_bills_per_faction", **kwargs)

    # Network chart methods
    def plot_mk_collaboration_network(self, **kwargs) -> Optional[go.Figure]:
        """Generate MK collaboration network chart."""
        return self.create_chart("network", "mk_collaboration_network", **kwargs)

    def plot_faction_collaboration_network(self, **kwargs) -> Optional[go.Figure]:
        """Generate faction collaboration network chart."""
        return self.create_chart("network", "faction_collaboration_network", **kwargs)

    def plot_coalition_opposition_network(self, **kwargs) -> Optional[go.Figure]:
        """Generate coalition/opposition collaboration network chart."""
        return self.create_chart("network", "coalition_opposition_network", **kwargs)
