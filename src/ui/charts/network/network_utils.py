"""Common utilities and constants for network charts.

This module provides shared functionality used across different network chart types.
"""

from typing import Dict, List

import plotly.express as px


# Coalition status color mapping
COALITION_STATUS_COLORS: Dict[str, str] = {
    'Coalition': '#1f77b4',
    'Opposition': '#ff7f0e',
    'Unknown': '#808080'
}

# Color for independent/unaffiliated MKs
INDEPENDENT_COLOR = '#FFD700'  # Gold


def get_faction_color_map(factions: List[str]) -> Dict[str, str]:
    """Generate a color map for factions.

    Args:
        factions: List of faction names

    Returns:
        Dictionary mapping faction names to colors
    """
    # Use multiple color palettes to ensure enough colors
    base_colors = (
        px.colors.qualitative.Set3 +
        px.colors.qualitative.Plotly +
        px.colors.qualitative.Set1
    )
    colors = base_colors[:len(factions)]
    color_map = dict(zip(factions, colors))

    # Ensure 'Independent' has a distinct color
    if 'Independent' in color_map:
        color_map['Independent'] = INDEPENDENT_COLOR

    return color_map


def get_node_size(value: float, max_value: float, min_size: int = 20, max_size: int = 80) -> float:
    """Calculate node size proportional to a value.

    Args:
        value: The value for this node
        max_value: Maximum value across all nodes
        min_size: Minimum node size in pixels
        max_size: Maximum node size in pixels

    Returns:
        Calculated node size
    """
    if max_value <= 0:
        return min_size
    return max(min_size, min(max_size, min_size + (value / max_value * (max_size - min_size))))


# SQL CTE for getting person's faction in a specific Knesset
PERSON_FACTION_CTE = """
MKFactionInKnesset AS (
    SELECT
        arp.PersonID,
        arp.KnessetNum,
        COALESCE(
            (SELECT f.Name
             FROM KNS_PersonToPosition ptp
             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
             WHERE ptp.PersonID = arp.PersonID
                 AND ptp.KnessetNum = arp.KnessetNum
                 AND ptp.FactionID IS NOT NULL
             ORDER BY ptp.StartDate DESC
             LIMIT 1),
            'Independent'
        ) as FactionName
    FROM AllRelevantPeople arp
)
"""

# SQL CTE for getting faction's coalition status
FACTION_COALITION_STATUS_CTE = """
FactionCoalitionStatus AS (
    SELECT DISTINCT
        f.FactionID,
        f.Name as FactionName,
        COALESCE(ufs.CoalitionStatus, 'Unknown') as CoalitionStatus
    FROM KNS_Faction f
    LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID
)
"""
