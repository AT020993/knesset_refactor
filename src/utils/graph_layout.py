"""Graph layout algorithms for network visualizations.

This module provides reusable force-directed layout algorithms that can be used
across different network visualization types (MK networks, faction networks, etc.).
"""

from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd


class ForceDirectedLayout:
    """Unified force-directed layout algorithm for network visualizations.

    Consolidates the common force-directed algorithm pattern used across
    MK networks, faction networks, and other network visualizations.

    The algorithm uses repulsive forces between all nodes and attractive
    forces between connected nodes, with optional weighting based on
    collaboration/connection counts.

    Args:
        k: Optimal distance between nodes (default: 80)
        iterations: Number of simulation iterations (default: 200)
        repulsion_multiplier: Multiplier for repulsive forces (default: 1.5)
        dt: Time step for position updates (default: 0.15)
        weighted: Whether to weight attractive forces by edge weights (default: True)
        position_range: Range for initial random positions (default: 50)

    Example:
        layout = ForceDirectedLayout(k=80, iterations=200, weighted=True)
        positions = layout.compute(
            nodes_df, edges_df,
            node_id_col='PersonID',
            source_col='MainInitiatorID',
            target_col='SupporterID',
            weight_col='CollaborationCount'
        )
    """

    def __init__(
        self,
        k: float = 80,
        iterations: int = 200,
        repulsion_multiplier: float = 1.5,
        dt: float = 0.15,
        weighted: bool = True,
        position_range: float = 50
    ):
        self.k = k
        self.iterations = iterations
        self.repulsion_multiplier = repulsion_multiplier
        self.dt = dt
        self.weighted = weighted
        self.position_range = position_range

    def compute(
        self,
        nodes_df: pd.DataFrame,
        edges_df: pd.DataFrame,
        node_id_col: str,
        source_col: str,
        target_col: str,
        weight_col: Optional[str] = None
    ) -> Dict[int, Tuple[float, float]]:
        """Compute force-directed layout positions for nodes.

        Args:
            nodes_df: DataFrame containing node information
            edges_df: DataFrame containing edge information
            node_id_col: Column name for node IDs in nodes_df
            source_col: Column name for source node IDs in edges_df
            target_col: Column name for target node IDs in edges_df
            weight_col: Optional column name for edge weights in edges_df

        Returns:
            Dictionary mapping node IDs to (x, y) positions
        """
        import random

        # Initialize random positions
        positions = {}
        for _, node in nodes_df.iterrows():
            node_id = node[node_id_col]
            positions[node_id] = [
                random.uniform(-self.position_range, self.position_range),
                random.uniform(-self.position_range, self.position_range)
            ]

        # Build edge weights dictionary
        edge_weights = self._build_edge_weights(
            edges_df, positions, source_col, target_col, weight_col
        )

        # Run force-directed simulation
        for iteration in range(self.iterations):
            forces = {node_id: [0.0, 0.0] for node_id in positions}

            # Repulsive forces (all nodes repel each other)
            self._apply_repulsive_forces(positions, forces)

            # Attractive forces (connected nodes attract)
            self._apply_attractive_forces(positions, forces, edge_weights)

            # Update positions with cooling
            self._update_positions(positions, forces, iteration)

        # Convert to tuple format
        return {node_id: tuple(pos) for node_id, pos in positions.items()}

    def _build_edge_weights(
        self,
        edges_df: pd.DataFrame,
        positions: dict,
        source_col: str,
        target_col: str,
        weight_col: Optional[str]
    ) -> Dict[Tuple[int, int], float]:
        """Build dictionary of edge weights from edges DataFrame."""
        edge_weights = {}
        for _, edge in edges_df.iterrows():
            source_id = edge[source_col]
            target_id = edge[target_col]
            weight = edge[weight_col] if weight_col and weight_col in edge.index else 1

            # Only include edges where both nodes exist
            if source_id in positions and target_id in positions:
                key = tuple(sorted([source_id, target_id]))
                if key not in edge_weights:
                    edge_weights[key] = 0
                edge_weights[key] += weight

        return edge_weights

    def _apply_repulsive_forces(self, positions: dict, forces: dict) -> None:
        """Apply repulsive forces between all pairs of nodes."""
        node_ids = list(positions.keys())
        for i, id1 in enumerate(node_ids):
            for id2 in node_ids[i + 1:]:
                dx = positions[id1][0] - positions[id2][0]
                dy = positions[id1][1] - positions[id2][1]
                distance = max(np.sqrt(dx * dx + dy * dy), 0.1)

                # Repulsive force magnitude
                force_mag = (self.k * self.k * self.repulsion_multiplier) / distance

                # Apply forces in opposite directions
                fx = force_mag * dx / distance
                fy = force_mag * dy / distance

                forces[id1][0] += fx
                forces[id1][1] += fy
                forces[id2][0] -= fx
                forces[id2][1] -= fy

    def _apply_attractive_forces(
        self,
        positions: dict,
        forces: dict,
        edge_weights: Dict[Tuple[int, int], float]
    ) -> None:
        """Apply attractive forces between connected nodes."""
        for (id1, id2), weight in edge_weights.items():
            dx = positions[id2][0] - positions[id1][0]
            dy = positions[id2][1] - positions[id1][1]
            distance = max(np.sqrt(dx * dx + dy * dy), 0.1)

            # Attractive force magnitude (weighted if enabled)
            if self.weighted:
                force_mag = (distance * distance / self.k) * (0.5 + np.log1p(weight) * 0.3)
            else:
                force_mag = distance * distance / self.k

            # Apply forces toward each other
            fx = force_mag * dx / distance
            fy = force_mag * dy / distance

            forces[id1][0] += fx
            forces[id1][1] += fy
            forces[id2][0] -= fx
            forces[id2][1] -= fy

    def _update_positions(self, positions: dict, forces: dict, iteration: int) -> None:
        """Update node positions based on computed forces with cooling."""
        cooling = 1.0 - (iteration / self.iterations) * 0.5
        max_displacement = 12  # Stability limit

        for node_id in positions:
            force_magnitude = np.sqrt(
                forces[node_id][0] ** 2 + forces[node_id][1] ** 2
            )
            if force_magnitude > 0:
                displacement = min(force_magnitude * self.dt * cooling, max_displacement)
                positions[node_id][0] += (forces[node_id][0] / force_magnitude) * displacement
                positions[node_id][1] += (forces[node_id][1] / force_magnitude) * displacement


def get_layout_explanation() -> str:
    """
    Get a verbal explanation of how distance is calculated in the network charts.

    Returns:
        Markdown-formatted explanation of the force-directed layout algorithm.
    """
    return """
### How to Read This Network Chart

**Distance = Collaboration Strength**

In this visualization, the distance between nodes (circles) represents how frequently they collaborate:
- **Closer nodes** = More collaborations between them
- **Farther nodes** = Fewer or no collaborations

---

### The Algorithm: Weighted Force-Directed Layout

This chart uses a physics-based simulation where nodes behave like charged particles connected by springs:

**1. Repulsive Force (All Nodes Push Apart)**
- Every node repels every other node, like magnets with the same pole
- Formula: `F = (80² × 1.5) / distance`
- This prevents all nodes from collapsing into a single point

**2. Attractive Force (Collaborators Pull Together)**
- Nodes that collaborate are connected by "springs" that pull them closer
- The more collaborations, the stronger the pull
- Formula: `F = (distance² / 80) × (0.5 + ln(1 + collaborations) × 0.3)`
- The logarithmic scaling (`ln(1 + collaborations)`) ensures that very high collaboration counts don't dominate

---

### Algorithm Parameters

| Parameter | Value | Meaning |
|-----------|-------|---------|
| **k** | 80 | Optimal distance between unconnected nodes |
| **Iterations** | 200 | Number of simulation cycles to reach equilibrium |
| **Repulsion multiplier** | 1.5× | Increased spacing for better visibility |
| **Cooling schedule** | 1.0 → 0.5 | Gradual slowdown for stability |

---

### Visual Cues

- **Node Size**: Proportional to total bills initiated (larger = more active legislator/faction)
- **Node Color**: Indicates faction or coalition status
- **Cluster Patterns**: Tight groups indicate frequent collaboration partners
- **Outliers**: Isolated nodes have few cross-party collaborations
"""
