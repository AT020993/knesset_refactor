"""Network and connection chart generators.

This module serves as a facade for backward compatibility.
The actual implementations have been refactored into separate modules:
- mk_network.py - MK collaboration network chart
- faction_network.py - Faction collaboration network chart
- coalition_breakdown.py - Coalition breakdown chart
- collaboration_matrix.py - Collaboration matrix chart

The ForceDirectedLayout class has been moved to utils/graph_layout.py
Common utilities are in network_utils.py
"""

import logging
from pathlib import Path
from typing import Any, Callable, List, Optional

import plotly.graph_objects as go

from ..base import BaseChart

# Re-export ForceDirectedLayout from its new location for backward compatibility
from utils.graph_layout import ForceDirectedLayout, get_layout_explanation

# Import the individual chart classes
from .mk_network import MKCollaborationNetwork
from .faction_network import FactionCollaborationNetwork
from .coalition_breakdown import CoalitionBreakdownChart
from .collaboration_matrix import CollaborationMatrixChart


class NetworkCharts(BaseChart):
    """Network analysis charts (connection maps, collaboration networks, etc.).

    This class serves as a facade that delegates to specialized chart classes.
    For direct usage, consider importing the specific chart classes instead.
    """

    def __init__(self, db_path: Path, logger: Optional[logging.Logger] = None):
        """Initialize NetworkCharts with database path.

        Args:
            db_path: Path to the DuckDB database file
            logger: Optional logger instance
        """
        logger_obj = logger or logging.getLogger(__name__)
        super().__init__(db_path, logger_obj)
        # Initialize specialized chart instances
        self._mk_network = MKCollaborationNetwork(db_path, logger_obj)
        self._faction_network = FactionCollaborationNetwork(db_path, logger_obj)
        self._coalition_breakdown = CoalitionBreakdownChart(db_path, logger_obj)
        self._collaboration_matrix = CollaborationMatrixChart(db_path, logger_obj)

    def plot_mk_collaboration_network(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 3,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate MK collaboration network chart showing individual member connections.

        Delegates to MKCollaborationNetwork.plot()

        Args:
            knesset_filter: Optional list of Knesset numbers to filter by
            faction_filter: Optional list of faction names to filter by
            min_collaborations: Minimum number of collaborations required for an edge
            **kwargs: Additional filter arguments

        Returns:
            Plotly Figure object or None if no data
        """
        return self._mk_network.plot(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            min_collaborations=min_collaborations,
            **kwargs
        )

    def plot_faction_collaboration_network(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate faction collaboration network chart showing inter-faction connections.

        Delegates to FactionCollaborationNetwork.plot()

        Distance between factions represents collaboration strength - more collaborations = closer together.

        Args:
            knesset_filter: Optional list of Knesset numbers to filter by
            faction_filter: Optional list of faction names to filter by
            **kwargs: Additional filter arguments

        Returns:
            Plotly Figure object or None if no data
        """
        return self._faction_network.plot(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            **kwargs
        )

    def plot_faction_coalition_breakdown(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 5,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate faction collaboration breakdown chart showing Coalition vs Opposition collaboration percentages.

        Delegates to CoalitionBreakdownChart.plot()

        Args:
            knesset_filter: Optional list of Knesset numbers to filter by
            faction_filter: Optional list of faction names to filter by
            min_collaborations: Minimum number of collaborations required for a faction
            **kwargs: Additional filter arguments

        Returns:
            Plotly Figure object or None if no data
        """
        return self._coalition_breakdown.plot(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            min_collaborations=min_collaborations,
            **kwargs
        )

    def plot_faction_collaboration_matrix(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 3,
        show_solo_bills: bool = True,
        min_total_bills: int = 1,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate enhanced faction collaboration matrix showing both collaborations and solo bill activity.

        Delegates to CollaborationMatrixChart.plot()

        Args:
            knesset_filter: Optional list of Knesset numbers to filter by
            faction_filter: Optional list of faction names to filter by
            min_collaborations: Minimum number of collaborations required for an edge
            show_solo_bills: Whether to show solo bills on the diagonal
            min_total_bills: Minimum number of total bills for a faction to appear
            **kwargs: Additional filter arguments

        Returns:
            Plotly Figure object or None if no data
        """
        return self._collaboration_matrix.plot(
            knesset_filter=knesset_filter,
            faction_filter=faction_filter,
            min_collaborations=min_collaborations,
            show_solo_bills=show_solo_bills,
            min_total_bills=min_total_bills,
            **kwargs
        )

    @staticmethod
    def get_layout_explanation() -> str:
        """Get a verbal explanation of how distance is calculated in the network charts.

        Delegates to utils.graph_layout.get_layout_explanation()

        Returns:
            Markdown-formatted explanation of the force-directed layout algorithm.
        """
        return get_layout_explanation()

    def generate(self, chart_type: str = "", **kwargs: Any) -> Optional[go.Figure]:
        """Generate the requested network chart.

        Args:
            chart_type: Type of chart to generate
            **kwargs: Chart-specific arguments

        Returns:
            Plotly Figure object or None if unknown chart type
        """
        chart_methods: dict[str, Callable[..., Optional[go.Figure]]] = {
            "mk_collaboration_network": self.plot_mk_collaboration_network,
            "faction_collaboration_network": self.plot_faction_collaboration_network,
            "faction_collaboration_matrix": self.plot_faction_collaboration_matrix,
            "faction_coalition_breakdown": self.plot_faction_coalition_breakdown,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown network chart type: {chart_type}")
            return None
