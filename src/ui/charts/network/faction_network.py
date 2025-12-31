"""Faction Collaboration Network Chart.

This module provides functionality for visualizing collaboration networks
between political factions based on cross-party bill co-sponsorship.
"""

import logging
from typing import List, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from utils.graph_layout import ForceDirectedLayout
from ..base import BaseChart
from .network_utils import COALITION_STATUS_COLORS, get_node_size


class FactionCollaborationNetwork(BaseChart):
    """Generates faction collaboration network visualizations."""

    def plot(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate faction collaboration network chart showing inter-faction connections.

        Distance between factions represents collaboration strength - more collaborations = closer together.

        Args:
            knesset_filter: Optional list of Knesset numbers to filter by
            faction_filter: Optional list of faction names to filter by
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
                    con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction"]
                ):
                    return None

                query = self._build_query(filters)
                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No faction collaboration data found for '{filters['knesset_title']}'.")
                    return None

                return self._create_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating faction collaboration network: {e}", exc_info=True)
            st.error(f"Could not generate faction collaboration network: {e}")
            return None

    def _build_query(self, filters: dict) -> str:
        """Build SQL query for faction collaboration network data."""
        return f"""
        WITH FactionCollaborations AS (
            SELECT
                main.PersonID as MainPersonID,
                supp.PersonID as SuppPersonID,
                main.BillID,
                b.KnessetNum
            FROM KNS_BillInitiator main
            JOIN KNS_Bill b ON main.BillID = b.BillID
            JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID
            WHERE main.Ordinal = 1
                AND supp.Ordinal > 1
                AND b.KnessetNum IS NOT NULL
                AND {filters["knesset_condition"]}
        ),
        RelevantKnessets AS (
            SELECT DISTINCT KnessetNum FROM FactionCollaborations
        ),
        PersonFactions AS (
            SELECT DISTINCT
                fc.MainPersonID as PersonID,
                fc.KnessetNum,
                (SELECT f.FactionID
                 FROM KNS_PersonToPosition ptp
                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                 WHERE ptp.PersonID = fc.MainPersonID
                     AND ptp.KnessetNum = fc.KnessetNum
                 ORDER BY ptp.StartDate DESC LIMIT 1) as FactionID
            FROM FactionCollaborations fc
            UNION
            SELECT DISTINCT
                fc.SuppPersonID as PersonID,
                fc.KnessetNum,
                (SELECT f.FactionID
                 FROM KNS_PersonToPosition ptp
                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                 WHERE ptp.PersonID = fc.SuppPersonID
                     AND ptp.KnessetNum = fc.KnessetNum
                 ORDER BY ptp.StartDate DESC LIMIT 1) as FactionID
            FROM FactionCollaborations fc
        ),
        FactionTotalBills AS (
            SELECT
                f.FactionID,
                f.Name as FactionName,
                COUNT(DISTINCT bi.BillID) as TotalBills
            FROM KNS_Faction f
            JOIN KNS_PersonToPosition ptp ON f.FactionID = ptp.FactionID
            JOIN KNS_BillInitiator bi ON ptp.PersonID = bi.PersonID AND bi.Ordinal = 1
            JOIN KNS_Bill b ON bi.BillID = b.BillID AND b.KnessetNum = ptp.KnessetNum
            WHERE b.KnessetNum IS NOT NULL
                AND {filters["knesset_condition"]}
            GROUP BY f.FactionID, f.Name
        )
        SELECT
            main_pf.FactionID as MainFactionID,
            supp_pf.FactionID as SupporterFactionID,
            COUNT(DISTINCT fc.BillID) as CollaborationCount,
            main_f.Name as MainFactionName,
            supp_f.Name as SupporterFactionName,
            COALESCE(main_ufs.CoalitionStatus, 'Unknown') as MainCoalitionStatus,
            COALESCE(supp_ufs.CoalitionStatus, 'Unknown') as SupporterCoalitionStatus,
            COALESCE(main_ftb.TotalBills, 0) as MainFactionTotalBills,
            COALESCE(supp_ftb.TotalBills, 0) as SupporterFactionTotalBills
        FROM FactionCollaborations fc
        JOIN PersonFactions main_pf ON fc.MainPersonID = main_pf.PersonID
        JOIN PersonFactions supp_pf ON fc.SuppPersonID = supp_pf.PersonID
        JOIN KNS_Faction main_f ON main_pf.FactionID = main_f.FactionID
        JOIN KNS_Faction supp_f ON supp_pf.FactionID = supp_f.FactionID
        LEFT JOIN UserFactionCoalitionStatus main_ufs ON main_f.FactionID = main_ufs.FactionID AND fc.KnessetNum = main_ufs.KnessetNum
        LEFT JOIN UserFactionCoalitionStatus supp_ufs ON supp_f.FactionID = supp_ufs.FactionID AND fc.KnessetNum = supp_ufs.KnessetNum
        LEFT JOIN FactionTotalBills main_ftb ON main_f.FactionID = main_ftb.FactionID
        LEFT JOIN FactionTotalBills supp_ftb ON supp_f.FactionID = supp_ftb.FactionID
        WHERE main_pf.FactionID IS NOT NULL
            AND supp_pf.FactionID IS NOT NULL
            AND main_pf.FactionID <> supp_pf.FactionID
        GROUP BY main_pf.FactionID, supp_pf.FactionID, main_f.Name, supp_f.Name,
                 main_ufs.CoalitionStatus, supp_ufs.CoalitionStatus, main_ftb.TotalBills, supp_ftb.TotalBills
        HAVING COUNT(DISTINCT fc.BillID) >= 1
        ORDER BY CollaborationCount DESC
        """

    def _create_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create faction collaboration network chart with weighted force-directed layout."""
        try:
            # Extract unique factions with safe string conversion
            main_factions = df[['MainFactionID', 'MainFactionName', 'MainCoalitionStatus']].copy()
            main_factions.columns = ['FactionID', 'Name', 'Status']
            main_factions['Name'] = main_factions['Name'].astype(str)
            main_factions['Status'] = main_factions['Status'].astype(str)

            supp_factions = df[['SupporterFactionID', 'SupporterFactionName', 'SupporterCoalitionStatus']].copy()
            supp_factions.columns = ['FactionID', 'Name', 'Status']
            supp_factions['Name'] = supp_factions['Name'].astype(str)
            supp_factions['Status'] = supp_factions['Status'].astype(str)

            all_factions = pd.concat([main_factions, supp_factions]).drop_duplicates(subset=['FactionID'])

            # Add total bills for proper node sizing
            faction_total_bills = {}
            for _, row in df.iterrows():
                main_faction_id = row['MainFactionID']
                if main_faction_id not in faction_total_bills:
                    faction_total_bills[main_faction_id] = row['MainFactionTotalBills']

                supp_faction_id = row['SupporterFactionID']
                if supp_faction_id not in faction_total_bills:
                    faction_total_bills[supp_faction_id] = row['SupporterFactionTotalBills']

            # Calculate collaboration count for hover info
            faction_collaboration_counts = {}
            for _, faction in all_factions.iterrows():
                faction_id = faction['FactionID']
                collaborations = df[(df['MainFactionID'] == faction_id) | (df['SupporterFactionID'] == faction_id)]
                faction_collaboration_counts[faction_id] = len(collaborations)

            all_factions['TotalBills'] = all_factions['FactionID'].map(faction_total_bills).fillna(0)
            all_factions['CollaborationCount'] = all_factions['FactionID'].map(faction_collaboration_counts)

        except Exception as e:
            self.logger.error(f"Error processing faction data: {e}")
            fig = go.Figure()
            fig.add_annotation(
                text="Error processing faction network data",
                xref="paper", yref="paper", x=0.5, y=0.5,
                showarrow=False, font=dict(size=16)
            )
            return fig

        # Generate weighted force-directed layout
        node_positions = self._create_weighted_layout(all_factions, df)

        fig = go.Figure()

        # Add edges with visible styling
        edge_x = []
        edge_y = []

        for _, edge in df.iterrows():
            main_id = edge['MainFactionID']
            supp_id = edge['SupporterFactionID']
            # Skip edges where either node is missing from positions
            if main_id not in node_positions or supp_id not in node_positions:
                continue
            source_pos = node_positions[main_id]
            target_pos = node_positions[supp_id]

            edge_x.extend([source_pos[0], target_pos[0], None])
            edge_y.extend([source_pos[1], target_pos[1], None])

        # Single edge trace with better visibility
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=2, color='rgba(50,50,50,0.6)'),
            hoverinfo='none',
            showlegend=False,
            name='collaborations'
        ))

        # Add faction nodes grouped by status
        max_bills = all_factions['TotalBills'].max() if not all_factions['TotalBills'].empty else 1

        for status in ['Coalition', 'Opposition', 'Unknown']:
            status_factions = all_factions[all_factions['Status'] == status]
            if status_factions.empty:
                continue

            node_x = []
            node_y = []
            node_sizes = []
            hover_texts = []
            node_names = []

            for _, faction in status_factions.iterrows():
                try:
                    faction_id = faction['FactionID']
                    pos = node_positions[faction_id]

                    # Node sizes based on total bills
                    node_size = get_node_size(faction['TotalBills'], max_bills, min_size=30, max_size=100)

                    # Get detailed collaboration info
                    collaborations = df[(df['MainFactionID'] == faction_id) | (df['SupporterFactionID'] == faction_id)]
                    partner_factions = set()
                    for _, collab in collaborations.iterrows():
                        if collab['MainFactionID'] == faction_id:
                            partner_factions.add(collab['SupporterFactionName'])
                        else:
                            partner_factions.add(collab['MainFactionName'])

                    node_x.append(pos[0])
                    node_y.append(pos[1])
                    node_sizes.append(node_size)
                    node_names.append(faction['Name'])

                    hover_text = (
                        f"<b>{faction['Name']}</b><br>"
                        f"Status: {status}<br>"
                        f"Total Bills: {int(faction['TotalBills'])}<br>"
                        f"Collaborations: {faction['CollaborationCount']}<br>"
                        f"Partner Factions: {len(partner_factions)}"
                    )
                    hover_texts.append(hover_text)

                except Exception as e:
                    self.logger.error(f"Error processing faction '{faction_id}': {e}")
                    continue

            if node_x:  # Only add trace if we have data
                fig.add_trace(go.Scatter(
                    x=node_x,
                    y=node_y,
                    mode='markers+text',
                    marker=dict(
                        size=node_sizes,
                        color=COALITION_STATUS_COLORS.get(status, '#808080'),
                        line=dict(width=4, color='white'),
                        opacity=0.9
                    ),
                    text=node_names,
                    textposition="middle center",
                    textfont=dict(size=12, color='black', family="Arial Black"),
                    hovertext=hover_texts,
                    hoverinfo='text',
                    name=status,
                    showlegend=True,
                    legendgroup=status
                ))

        fig.update_layout(
            title=(
                f"<b>Faction Collaboration Network<br>{title_suffix}</b><br>"
                f"<sub>Distance between factions reflects collaboration strength (closer = more collaborations)</sub>"
            ),
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=40, l=40, r=40, t=120),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-200, 200]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-180, 180]),
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
                font=dict(size=12)
            )
        )

        return fig

    def _create_weighted_layout(self, factions_df: pd.DataFrame, edges_df: pd.DataFrame) -> dict:
        """Create weighted force-directed layout for faction network.

        More collaborations = stronger attractive force = closer distance between factions.
        """
        layout = ForceDirectedLayout(
            k=80, iterations=200, repulsion_multiplier=1.5, dt=0.15, weighted=True
        )
        return layout.compute(
            factions_df, edges_df,
            node_id_col='FactionID',
            source_col='MainFactionID',
            target_col='SupporterFactionID',
            weight_col='CollaborationCount'
        )
