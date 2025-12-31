"""Network charts package.

This package contains network analysis charts including:
- MK collaboration networks
- Faction collaboration networks
- Collaboration matrices
- Coalition breakdown charts

The charts have been refactored into separate modules for better maintainability:
- mk_network.py - MKCollaborationNetwork class
- faction_network.py - FactionCollaborationNetwork class
- coalition_breakdown.py - CoalitionBreakdownChart class
- collaboration_matrix.py - CollaborationMatrixChart class

The ForceDirectedLayout algorithm has been moved to utils/graph_layout.py
Common utilities are in network_utils.py

For backward compatibility, NetworkCharts is still exported and delegates to the new classes.
"""

# Backward compatible facade
from .charts import NetworkCharts

# Individual chart classes for direct usage
from .mk_network import MKCollaborationNetwork
from .faction_network import FactionCollaborationNetwork
from .coalition_breakdown import CoalitionBreakdownChart
from .collaboration_matrix import CollaborationMatrixChart

# Utilities
from .network_utils import (
    COALITION_STATUS_COLORS,
    INDEPENDENT_COLOR,
    get_faction_color_map,
    get_node_size,
)

# Re-export ForceDirectedLayout from utils for convenience
from utils.graph_layout import ForceDirectedLayout, get_layout_explanation

__all__ = [
    # Main facade (backward compatible)
    'NetworkCharts',
    # Individual chart classes
    'MKCollaborationNetwork',
    'FactionCollaborationNetwork',
    'CoalitionBreakdownChart',
    'CollaborationMatrixChart',
    # Utilities
    'COALITION_STATUS_COLORS',
    'INDEPENDENT_COLOR',
    'get_faction_color_map',
    'get_node_size',
    # Layout algorithm
    'ForceDirectedLayout',
    'get_layout_explanation',
]
