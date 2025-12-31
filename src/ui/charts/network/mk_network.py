"""MK (Member of Knesset) Collaboration Network Chart.

This module provides functionality for visualizing collaboration networks
between individual Members of Knesset based on bill co-sponsorship.
"""

import logging
from typing import List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from utils.graph_layout import ForceDirectedLayout
from ..base import BaseChart
from .network_utils import get_faction_color_map, get_node_size, INDEPENDENT_COLOR


class MKCollaborationNetwork(BaseChart):
    """Generates MK collaboration network visualizations."""

    def plot(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 3,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate MK collaboration network chart showing individual member connections.

        Args:
            knesset_filter: Optional list of Knesset numbers to filter by
            faction_filter: Optional list of faction names to filter by
            min_collaborations: Minimum number of collaborations required for an edge
            **kwargs: Additional filter arguments

        Returns:
            Plotly Figure object or None if no data
        """
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(
                    con, ["KNS_Bill", "KNS_BillInitiator", "KNS_Person", "KNS_PersonToPosition", "KNS_Faction"]
                ):
                    return None

                query = self._build_query(filters, min_collaborations)
                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No MK collaboration data found for '{filters['knesset_title']}' "
                        f"with minimum {min_collaborations} collaborations."
                    )
                    return None

                return self._create_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating MK collaboration network: {e}", exc_info=True)
            st.error(f"Could not generate MK collaboration network: {e}")
            return None

    def _build_query(self, filters: dict, min_collaborations: int) -> str:
        """Build SQL query for MK collaboration network data."""
        return f"""
        WITH BillCollaborations AS (
            SELECT
                main.PersonID as MainInitiatorID,
                supp.PersonID as SupporterID,
                b.KnessetNum,
                COUNT(DISTINCT main.BillID) as CollaborationCount
            FROM KNS_BillInitiator main
            JOIN KNS_Bill b ON main.BillID = b.BillID
            JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID
            WHERE main.Ordinal = 1
                AND supp.Ordinal > 1
                AND b.KnessetNum IS NOT NULL
                AND {filters["knesset_condition"]}
            GROUP BY main.PersonID, supp.PersonID, b.KnessetNum
            HAVING COUNT(DISTINCT main.BillID) >= {min_collaborations}
        ),
        RelevantKnessets AS (
            SELECT DISTINCT KnessetNum FROM BillCollaborations
        ),
        AllRelevantPeople AS (
            SELECT DISTINCT PersonID, KnessetNum
            FROM (
                SELECT MainInitiatorID as PersonID, KnessetNum FROM BillCollaborations
                UNION
                SELECT SupporterID as PersonID, KnessetNum FROM BillCollaborations
            ) people
        ),
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
        ),
        MKDetails AS (
            SELECT
                mkf.PersonID,
                p.FirstName || ' ' || p.LastName as FullName,
                mkf.FactionName,
                COUNT(DISTINCT CASE WHEN bi.Ordinal = 1 AND b.KnessetNum = mkf.KnessetNum THEN bi.BillID END) as TotalBills
            FROM MKFactionInKnesset mkf
            JOIN KNS_Person p ON mkf.PersonID = p.PersonID
            LEFT JOIN KNS_BillInitiator bi ON mkf.PersonID = bi.PersonID
            LEFT JOIN KNS_Bill b ON bi.BillID = b.BillID
            GROUP BY mkf.PersonID, p.FirstName, p.LastName, mkf.FactionName
        )
        SELECT
            bc.MainInitiatorID,
            bc.SupporterID,
            SUM(bc.CollaborationCount) as CollaborationCount,
            main_mk.FullName as MainInitiatorName,
            main_mk.FactionName as MainInitiatorFaction,
            main_mk.TotalBills as MainInitiatorTotalBills,
            supp_mk.FullName as SupporterName,
            supp_mk.FactionName as SupporterFaction,
            supp_mk.TotalBills as SupporterTotalBills
        FROM BillCollaborations bc
        JOIN MKDetails main_mk ON bc.MainInitiatorID = main_mk.PersonID
        JOIN MKDetails supp_mk ON bc.SupporterID = supp_mk.PersonID
        GROUP BY bc.MainInitiatorID, bc.SupporterID,
            main_mk.FullName, main_mk.FactionName, main_mk.TotalBills,
            supp_mk.FullName, supp_mk.FactionName, supp_mk.TotalBills
        ORDER BY SUM(bc.CollaborationCount) DESC
        """

    def _create_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create MK collaboration network chart with force-directed layout."""
        try:
            # Extract unique nodes with safe string conversion
            main_nodes = df[['MainInitiatorID', 'MainInitiatorName', 'MainInitiatorFaction', 'MainInitiatorTotalBills']].copy()
            main_nodes.columns = ['PersonID', 'Name', 'Faction', 'TotalBills']
            main_nodes['Name'] = main_nodes['Name'].astype(str)
            main_nodes['Faction'] = main_nodes['Faction'].astype(str)

            supp_nodes = df[['SupporterID', 'SupporterName', 'SupporterFaction', 'SupporterTotalBills']].copy()
            supp_nodes.columns = ['PersonID', 'Name', 'Faction', 'TotalBills']
            supp_nodes['Name'] = supp_nodes['Name'].astype(str)
            supp_nodes['Faction'] = supp_nodes['Faction'].astype(str)

            all_nodes = pd.concat([main_nodes, supp_nodes]).drop_duplicates(subset=['PersonID'])

        except Exception as e:
            self.logger.error(f"Error processing node data: {e}")
            fig = go.Figure()
            fig.add_annotation(
                text="Error processing network data",
                xref="paper", yref="paper", x=0.5, y=0.5,
                showarrow=False, font=dict(size=16)
            )
            return fig

        # Generate weighted force-directed layout
        node_positions = self._create_weighted_layout(all_nodes, df)

        # Prepare faction colors
        unique_factions = all_nodes['Faction'].unique()
        color_map = get_faction_color_map(list(unique_factions))

        # Create the interactive network visualization
        fig = go.Figure()

        # Add ALL edges as a single trace
        edge_x = []
        edge_y = []

        for _, edge in df.iterrows():
            main_id = edge['MainInitiatorID']
            supp_id = edge['SupporterID']
            # Skip edges where either node is missing from positions
            if main_id not in node_positions or supp_id not in node_positions:
                continue
            source_pos = node_positions[main_id]
            target_pos = node_positions[supp_id]

            edge_x.extend([source_pos[0], target_pos[0], None])
            edge_y.extend([source_pos[1], target_pos[1], None])

        # Single edge trace
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=0.8, color='rgba(100,100,100,0.4)'),
            hoverinfo='none',
            showlegend=False,
            name='connections'
        ))

        # Add nodes grouped by faction for better legend
        max_bills = all_nodes['TotalBills'].max() if not all_nodes['TotalBills'].empty else 1

        for faction in unique_factions:
            try:
                faction_nodes = all_nodes[all_nodes['Faction'] == faction]

                node_x = []
                node_y = []
                node_sizes = []
                hover_texts = []
                node_names = []

                for _, node in faction_nodes.iterrows():
                    person_id = node['PersonID']
                    pos = node_positions[person_id]

                    # Node sizes for visibility
                    node_size = get_node_size(node['TotalBills'], max_bills, min_size=20, max_size=80)

                    # Get connections for this node
                    connections = df[(df['MainInitiatorID'] == person_id) | (df['SupporterID'] == person_id)]
                    collaboration_count = len(connections)

                    node_x.append(pos[0])
                    node_y.append(pos[1])
                    node_sizes.append(node_size)
                    node_names.append(node['Name'])

                    hover_text = (
                        f"<b>{node['Name']}</b><br>"
                        f"Faction: {faction}<br>"
                        f"Total Bills: {node['TotalBills']}<br>"
                        f"Collaborations: {collaboration_count}"
                    )
                    hover_texts.append(hover_text)

                # Create faction trace with enhanced visibility
                fig.add_trace(go.Scatter(
                    x=node_x,
                    y=node_y,
                    mode='markers+text',
                    marker=dict(
                        size=node_sizes,
                        color=color_map.get(faction, '#9467BD'),
                        line=dict(width=3, color='white'),
                        opacity=0.9
                    ),
                    text=node_names,
                    textposition="middle center",
                    textfont=dict(size=10, color='black', family="Arial Black"),
                    hovertext=hover_texts,
                    hoverinfo='text',
                    name=str(faction),
                    showlegend=True,
                    legendgroup=faction
                ))

            except Exception as e:
                self.logger.error(f"Error processing faction '{faction}': {e}")
                continue

        fig.update_layout(
            title=(
                f"<b>MK Collaboration Network<br>{title_suffix}</b><br>"
                f"<sub>Distance between MKs reflects collaboration strength (closer = more collaborations)</sub>"
            ),
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=40, l=40, r=40, t=120),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-150, 150]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-150, 150]),
            height=900,
            width=900,
            plot_bgcolor='rgba(240,240,240,0.1)',
            annotations=[],
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.05,
                font=dict(size=10)
            )
        )

        return fig

    def _create_weighted_layout(self, nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> dict:
        """Create weighted force-directed layout for MK network.

        More collaborations = stronger attractive force = closer distance between MKs.
        """
        layout = ForceDirectedLayout(
            k=80, iterations=200, repulsion_multiplier=1.5, dt=0.15, weighted=True
        )
        return layout.compute(
            nodes_df, edges_df,
            node_id_col='PersonID',
            source_col='MainInitiatorID',
            target_col='SupporterID',
            weight_col='CollaborationCount'
        )
