"""Faction Collaboration Matrix Chart.

This module provides functionality for visualizing faction collaboration patterns
as a heatmap matrix, showing both cross-party collaborations and solo bill activity.
"""

import logging
from typing import List, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from ..base import BaseChart


class CollaborationMatrixChart(BaseChart):
    """Generates faction collaboration matrix visualizations."""

    def plot(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 3,
        show_solo_bills: bool = True,
        min_total_bills: int = 1,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate enhanced faction collaboration matrix showing both collaborations and solo bill activity.

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
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(
                    con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction"]
                ):
                    return None

                query = self._build_query(filters, min_collaborations, show_solo_bills, min_total_bills)
                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No faction activity data found for '{filters['knesset_title']}'.")
                    return None

                return self._create_chart(df, filters['knesset_title'], min_collaborations, show_solo_bills)

        except Exception as e:
            self.logger.error(f"Error generating faction collaboration matrix: {e}", exc_info=True)
            st.error(f"Could not generate faction collaboration matrix: {e}")
            return None

    def _build_query(self, filters: dict, min_collaborations: int, show_solo_bills: bool, min_total_bills: int) -> str:
        """Build SQL query for collaboration matrix data."""
        return f"""
        WITH AllActiveFactions AS (
            -- Get all factions that initiated bills (main or supporting) in selected Knesset(s)
            SELECT DISTINCT
                f.FactionID,
                f.Name as FactionName,
                COALESCE(ufs.CoalitionStatus, 'Unknown') as CoalitionStatus
            FROM KNS_Faction f
            LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID
            WHERE f.FactionID IN (
                SELECT DISTINCT
                    (SELECT f2.FactionID
                     FROM KNS_PersonToPosition ptp
                     JOIN KNS_Faction f2 ON ptp.FactionID = f2.FactionID
                     WHERE ptp.PersonID = bi.PersonID
                         AND ptp.KnessetNum = b.KnessetNum
                     ORDER BY ptp.StartDate DESC LIMIT 1) as FactionID
                FROM KNS_BillInitiator bi
                JOIN KNS_Bill b ON bi.BillID = b.BillID
                WHERE b.KnessetNum IS NOT NULL
                  AND {filters["knesset_condition"]}
                  AND bi.PersonID IS NOT NULL
            )
            AND f.FactionID IS NOT NULL
        ),
        SoloBills AS (
            -- Count bills where each faction worked alone (only 1 initiator total)
            SELECT
                af.FactionID,
                af.FactionName,
                af.CoalitionStatus,
                COUNT(DISTINCT solo_bills.BillID) as SoloBillCount
            FROM AllActiveFactions af
            LEFT JOIN (
                SELECT DISTINCT
                    bi.BillID,
                    (SELECT f.FactionID
                     FROM KNS_PersonToPosition ptp
                     JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                     WHERE ptp.PersonID = bi.PersonID
                         AND ptp.KnessetNum = b.KnessetNum
                     ORDER BY ptp.StartDate DESC LIMIT 1) as FactionID
                FROM KNS_BillInitiator bi
                JOIN KNS_Bill b ON bi.BillID = b.BillID
                WHERE bi.BillID IN (
                    SELECT BillID
                    FROM KNS_BillInitiator
                    GROUP BY BillID
                    HAVING COUNT(*) = 1
                )
                AND b.KnessetNum IS NOT NULL
                AND {filters["knesset_condition"]}
            ) solo_bills ON af.FactionID = solo_bills.FactionID
            GROUP BY af.FactionID, af.FactionName, af.CoalitionStatus
        ),
        FactionCollaborations AS (
            -- Existing collaboration logic
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
        CollaborationPairs AS (
            SELECT
                main_faction.FactionID as MainFactionID,
                supp_faction.FactionID as SupporterFactionID,
                COUNT(DISTINCT fc.BillID) as CollaborationCount,
                main_faction.FactionName as MainFactionName,
                supp_faction.FactionName as SupporterFactionName,
                main_faction.CoalitionStatus as MainCoalitionStatus,
                supp_faction.CoalitionStatus as SupporterCoalitionStatus
            FROM FactionCollaborations fc
            JOIN (
                SELECT DISTINCT
                    fc2.MainPersonID as PersonID,
                    af.FactionID,
                    af.FactionName,
                    af.CoalitionStatus
                FROM FactionCollaborations fc2
                JOIN AllActiveFactions af ON af.FactionID = (
                    SELECT f.FactionID
                    FROM KNS_PersonToPosition ptp
                    JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    WHERE ptp.PersonID = fc2.MainPersonID
                        AND ptp.KnessetNum = fc2.KnessetNum
                    ORDER BY ptp.StartDate DESC LIMIT 1
                )
            ) main_faction ON fc.MainPersonID = main_faction.PersonID
            JOIN (
                SELECT DISTINCT
                    fc2.SuppPersonID as PersonID,
                    af.FactionID,
                    af.FactionName,
                    af.CoalitionStatus
                FROM FactionCollaborations fc2
                JOIN AllActiveFactions af ON af.FactionID = (
                    SELECT f.FactionID
                    FROM KNS_PersonToPosition ptp
                    JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    WHERE ptp.PersonID = fc2.SuppPersonID
                        AND ptp.KnessetNum = fc2.KnessetNum
                    ORDER BY ptp.StartDate DESC LIMIT 1
                )
            ) supp_faction ON fc.SuppPersonID = supp_faction.PersonID
            WHERE main_faction.FactionID IS NOT NULL
                AND supp_faction.FactionID IS NOT NULL
                AND main_faction.FactionID <> supp_faction.FactionID
            GROUP BY main_faction.FactionID, supp_faction.FactionID,
                     main_faction.FactionName, supp_faction.FactionName,
                     main_faction.CoalitionStatus, supp_faction.CoalitionStatus
            HAVING COUNT(DISTINCT fc.BillID) >= {min_collaborations}
        )
        -- Return results in format compatible with existing matrix creation
        SELECT
            'collaboration' as DataType,
            cp.MainFactionID as FactionID1,
            cp.SupporterFactionID as FactionID2,
            cp.MainFactionName as FactionName1,
            cp.SupporterFactionName as FactionName2,
            cp.MainCoalitionStatus as CoalitionStatus1,
            cp.SupporterCoalitionStatus as CoalitionStatus2,
            cp.CollaborationCount as Count
        FROM CollaborationPairs cp
        UNION ALL
        SELECT
            'solo' as DataType,
            sb.FactionID as FactionID1,
            sb.FactionID as FactionID2,
            sb.FactionName as FactionName1,
            sb.FactionName as FactionName2,
            sb.CoalitionStatus as CoalitionStatus1,
            sb.CoalitionStatus as CoalitionStatus2,
            sb.SoloBillCount as Count
        FROM SoloBills sb
        WHERE ({1 if show_solo_bills else 0} = 1 AND sb.SoloBillCount >= {min_total_bills})
           OR (sb.FactionID IN (SELECT DISTINCT MainFactionID FROM CollaborationPairs))
           OR (sb.FactionID IN (SELECT DISTINCT SupporterFactionID FROM CollaborationPairs))
        ORDER BY Count DESC
        """

    def _create_chart(
        self, df: pd.DataFrame, title_suffix: str, min_collaborations: int, show_solo_bills: bool
    ) -> go.Figure:
        """Create enhanced faction collaboration matrix with both collaborations and solo bills."""
        # Separate solo and collaboration data
        solo_data = df[df['DataType'] == 'solo'].copy()
        collab_data = df[df['DataType'] == 'collaboration'].copy()

        # Get all unique factions
        all_factions_set = set()
        for _, row in df.iterrows():
            all_factions_set.add(row['FactionName1'])
            if row['FactionName1'] != row['FactionName2']:
                all_factions_set.add(row['FactionName2'])

        all_factions = sorted(list(all_factions_set))

        # Get faction coalition status mapping
        faction_status = {}
        for _, row in df.iterrows():
            faction_status[row['FactionName1']] = row['CoalitionStatus1']
            if row['FactionName1'] != row['FactionName2']:
                faction_status[row['FactionName2']] = row['CoalitionStatus2']

        # Sort factions by coalition status and activity level
        def sort_key(faction):
            status = faction_status.get(faction, 'Unknown')
            total_activity = 0

            # Add solo bills
            solo_count = solo_data[solo_data['FactionName1'] == faction]['Count'].sum()
            total_activity += solo_count

            # Add collaboration activity
            collab_as_main = collab_data[collab_data['FactionName1'] == faction]['Count'].sum()
            collab_as_supporter = collab_data[collab_data['FactionName2'] == faction]['Count'].sum()
            total_activity += collab_as_main + collab_as_supporter

            status_order = {'Coalition': 0, 'Opposition': 1, 'Unknown': 2}
            return (status_order.get(status, 3), -total_activity)

        sorted_factions = sorted(all_factions, key=sort_key)
        n_factions = len(sorted_factions)

        # Create full matrix
        matrix_data = np.zeros((n_factions, n_factions))
        matrix_type = np.full((n_factions, n_factions), 'none', dtype=object)

        # Fill diagonal with solo bills
        if show_solo_bills:
            for _, row in solo_data.iterrows():
                if row['FactionName1'] in sorted_factions:
                    idx = sorted_factions.index(row['FactionName1'])
                    matrix_data[idx, idx] = row['Count']
                    matrix_type[idx, idx] = 'solo'

        # Fill off-diagonal with collaborations
        for _, row in collab_data.iterrows():
            if row['FactionName1'] in sorted_factions and row['FactionName2'] in sorted_factions:
                idx1 = sorted_factions.index(row['FactionName1'])
                idx2 = sorted_factions.index(row['FactionName2'])
                matrix_data[idx1, idx2] = row['Count']
                matrix_type[idx1, idx2] = 'collaboration'

        # Create custom hover text
        hover_text = self._create_hover_text(
            n_factions, sorted_factions, matrix_data, matrix_type,
            faction_status, show_solo_bills, min_collaborations
        )

        # Create visualization
        collab_matrix = matrix_data.copy()
        np.fill_diagonal(collab_matrix, 0)

        solo_matrix = np.zeros_like(matrix_data)
        np.fill_diagonal(solo_matrix, np.diag(matrix_data))

        fig = go.Figure()

        # Add collaboration heatmap (off-diagonal)
        if float(collab_matrix.max()) > 0:
            fig.add_trace(go.Heatmap(
                z=collab_matrix,
                x=sorted_factions,
                y=sorted_factions,
                colorscale=[
                    [0, 'rgba(255,255,255,0)'],
                    [0.001, 'white'],
                    [0.1, '#e6f3ff'],
                    [0.3, '#b3d9ff'],
                    [0.6, '#66c2ff'],
                    [0.8, '#1a8cff'],
                    [1.0, '#0066cc']
                ],
                name="Collaborations",
                hovertemplate='%{customdata}<extra></extra>',
                customdata=hover_text,
                colorbar=dict(
                    title=dict(text="Collaboration Count", side="right"),
                    thickness=15,
                    len=0.8,
                    x=1.02,
                    xanchor="left"
                ),
                zmin=0,
                zmax=collab_matrix.max() if collab_matrix.max() > 0 else 1,
                showscale=True
            ))

        # Add solo bills heatmap (diagonal only)
        if show_solo_bills and float(solo_matrix.max()) > 0:
            fig.add_trace(go.Heatmap(
                z=solo_matrix,
                x=sorted_factions,
                y=sorted_factions,
                colorscale=[
                    [0, 'rgba(255,255,255,0)'],
                    [0.001, 'white'],
                    [0.1, '#e6ffe6'],
                    [0.3, '#b3ffb3'],
                    [0.6, '#66ff66'],
                    [0.8, '#1aff1a'],
                    [1.0, '#00cc00']
                ],
                name="Solo Bills",
                hovertemplate='%{customdata}<extra></extra>',
                customdata=hover_text,
                colorbar=dict(
                    title=dict(text="Solo Bills Count", side="right"),
                    thickness=15,
                    len=0.8,
                    x=1.12,
                    xanchor="left"
                ),
                zmin=0,
                zmax=solo_matrix.max() if solo_matrix.max() > 0 else 1,
                showscale=True
            ))

        # Update layout
        title_text = f"<b>Cross-Party Collaboration Matrix<br>{title_suffix}</b>"
        if show_solo_bills:
            title_text += "<br><sub>Blue: Inter-faction collaborations | Green: Solo bills (diagonal)</sub>"

        fig.update_layout(
            title=title_text,
            title_x=0.5,
            xaxis=dict(
                title="Sponsored Factions",
                side='bottom',
                tickangle=45,
                tickfont=dict(size=10)
            ),
            yaxis=dict(
                title="First Initiator Faction",
                tickmode='linear',
                tickfont=dict(size=10),
                autorange='reversed'
            ),
            height=max(700, n_factions * 35),
            width=max(900, n_factions * 35),
            margin=dict(l=250, r=280, t=120, b=180),
            plot_bgcolor='white'
        )

        return fig

    def _create_hover_text(
        self,
        n_factions: int,
        sorted_factions: list,
        matrix_data: np.ndarray,
        matrix_type: np.ndarray,
        faction_status: dict,
        show_solo_bills: bool,
        min_collaborations: int
    ) -> list:
        """Create hover text for each cell in the matrix."""
        hover_text = []
        for i in range(n_factions):
            hover_row = []
            for j in range(n_factions):
                faction1 = sorted_factions[i]
                faction2 = sorted_factions[j]
                value = int(matrix_data[i, j])
                data_type = matrix_type[i, j]

                if i == j:  # Diagonal - solo bills
                    if show_solo_bills and value > 0:
                        hover_row.append(
                            f"<b>{faction1}</b><br>"
                            f"Solo Bills: {int(value)}<br>"
                            f"Status: {faction_status.get(faction1, 'Unknown')}<br>"
                            f"<i>Bills with only 1 initiator</i>"
                        )
                    else:
                        hover_row.append(
                            f"<b>{faction1}</b><br>"
                            f"Solo Bills: 0<br>"
                            f"Status: {faction_status.get(faction1, 'Unknown')}"
                        )
                else:  # Off-diagonal - collaborations
                    if value > 0:
                        hover_row.append(
                            f"<b>{faction1}</b> → <b>{faction2}</b><br>"
                            f"Collaborations: {int(value)}<br>"
                            f"Primary: {faction_status.get(faction1, 'Unknown')}<br>"
                            f"Supporter: {faction_status.get(faction2, 'Unknown')}<br>"
                            f"<i>Bills with cross-party support</i>"
                        )
                    else:
                        hover_row.append(
                            f"<b>{faction1}</b> → <b>{faction2}</b><br>"
                            f"No collaboration (< {min_collaborations})<br>"
                            f"Primary: {faction_status.get(faction1, 'Unknown')}<br>"
                            f"Supporter: {faction_status.get(faction2, 'Unknown')}"
                        )
            hover_text.append(hover_row)
        return hover_text

    def generate(self, **kwargs) -> Optional[go.Figure]:
        """Generate the collaboration matrix chart.

        Required implementation of BaseChart abstract method.
        Delegates to plot() with the provided kwargs.
        """
        return self.plot(**kwargs)
