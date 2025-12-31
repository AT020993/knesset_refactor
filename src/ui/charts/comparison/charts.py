"""Comparison and faction analysis chart generators.

This module serves as a facade for backward compatibility.
The actual implementations have been refactored into separate modules:
- query_charts.py - Query comparison charts
- agenda_charts.py - Agenda comparison charts
- bill_charts.py - Bill comparison charts
"""

import logging
from typing import List, Optional

import plotly.graph_objects as go

from ..base import BaseChart

# Import the individual chart classes
from .query_charts import QueryComparisonCharts
from .agenda_charts import AgendaComparisonCharts
from .bill_charts import BillComparisonCharts


class ComparisonCharts(BaseChart):
    """Comparison charts for factions, ministries, etc.

    This class serves as a facade that delegates to specialized chart classes.
    For direct usage, consider importing the specific chart classes instead.
    """

    def __init__(self, db_path: str, logger: Optional[logging.Logger] = None):
        """Initialize ComparisonCharts with database path.

        Args:
            db_path: Path to the DuckDB database file
            logger: Optional logger instance
        """
        super().__init__(db_path, logger)
        # Initialize specialized chart instances
        self._query_charts = QueryComparisonCharts(db_path, logger)
        self._agenda_charts = AgendaComparisonCharts(db_path, logger)
        self._bill_charts = BillComparisonCharts(db_path, logger)

    # Query chart methods - delegate to QueryComparisonCharts
    def plot_queries_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate queries per faction chart with date-based faction attribution.

        Delegates to QueryComparisonCharts.plot_queries_per_faction()
        """
        return self._query_charts.plot_queries_per_faction(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    def plot_queries_by_coalition_status(self, **kwargs) -> Optional[go.Figure]:
        """Generate queries by coalition/opposition status chart.

        Delegates to QueryComparisonCharts.plot_queries_by_coalition_status()
        """
        return self._query_charts.plot_queries_by_coalition_status(**kwargs)

    def plot_queries_by_ministry(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate queries by ministry and status chart.

        Delegates to QueryComparisonCharts.plot_queries_by_ministry()
        """
        return self._query_charts.plot_queries_by_ministry(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    def plot_query_status_by_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate query status by faction as a stacked bar chart.

        Delegates to QueryComparisonCharts.plot_query_status_by_faction()
        """
        return self._query_charts.plot_query_status_by_faction(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )

    # Agenda chart methods - delegate to AgendaComparisonCharts
    def plot_agendas_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda items per initiating faction chart.

        Delegates to AgendaComparisonCharts.plot_agendas_per_faction()
        """
        return self._agenda_charts.plot_agendas_per_faction(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    def plot_agendas_by_coalition_status(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda distribution by coalition/opposition status.

        Delegates to AgendaComparisonCharts.plot_agendas_by_coalition_status()
        """
        return self._agenda_charts.plot_agendas_by_coalition_status(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    # Bill chart methods - delegate to BillComparisonCharts
    def plot_bills_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate bills per initiating faction chart.

        Delegates to BillComparisonCharts.plot_bills_per_faction()
        """
        return self._bill_charts.plot_bills_per_faction(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    def plot_bills_by_coalition_status(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate bill distribution by coalition/opposition status.

        Delegates to BillComparisonCharts.plot_bills_by_coalition_status()
        """
        return self._bill_charts.plot_bills_by_coalition_status(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    def plot_top_bill_initiators(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate top 10 Knesset members who were main initiators of bills.

        Delegates to BillComparisonCharts.plot_top_bill_initiators()
        """
        return self._bill_charts.plot_top_bill_initiators(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested comparison chart.

        Args:
            chart_type: Type of chart to generate
            **kwargs: Chart-specific arguments

        Returns:
            Plotly Figure object or None if unknown chart type
        """
        chart_methods = {
            "queries_per_faction": self.plot_queries_per_faction,
            "queries_by_coalition_status": self.plot_queries_by_coalition_status,
            "queries_by_ministry": self.plot_queries_by_ministry,
            "query_status_by_faction": self.plot_query_status_by_faction,
            "agendas_per_faction": self.plot_agendas_per_faction,
            "agendas_by_coalition_status": self.plot_agendas_by_coalition_status,
            "bills_per_faction": self.plot_bills_per_faction,
            "bills_by_coalition_status": self.plot_bills_by_coalition_status,
            "top_bill_initiators": self.plot_top_bill_initiators,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown comparison chart type: {chart_type}")
            return None
