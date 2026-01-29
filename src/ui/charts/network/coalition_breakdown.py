"""Coalition Breakdown Chart.

This module provides functionality for visualizing faction collaboration breakdown
by Coalition vs Opposition status.
"""

import logging
from typing import List, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from ..base import BaseChart
from .network_utils import COALITION_STATUS_COLORS


class CoalitionBreakdownChart(BaseChart):
    """Generates faction coalition breakdown visualizations."""

    def plot(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 5,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate faction collaboration breakdown chart showing Coalition vs Opposition collaboration percentages.

        Args:
            knesset_filter: Optional list of Knesset numbers to filter by
            faction_filter: Optional list of faction names to filter by
            min_collaborations: Minimum number of collaborations required for a faction
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
                    con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction", "UserFactionCoalitionStatus"]
                ):
                    return None

                query = self._build_query(filters, min_collaborations)
                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No faction collaboration breakdown data found for '{filters['knesset_title']}' "
                        f"with minimum {min_collaborations} collaborations."
                    )
                    return None

                return self._create_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating faction collaboration breakdown: {e}", exc_info=True)
            st.error(f"Could not generate faction collaboration breakdown: {e}")
            return None

    def _build_query(self, filters: dict, min_collaborations: int) -> str:
        """Build SQL query for coalition breakdown data."""
        return f"""
        WITH BillCollaborations AS (
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
        PersonFactions AS (
            SELECT DISTINCT
                bc.MainPersonID as PersonID,
                bc.KnessetNum,
                (SELECT f.FactionID
                 FROM KNS_PersonToPosition ptp
                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                 WHERE ptp.PersonID = bc.MainPersonID
                     AND ptp.KnessetNum = bc.KnessetNum
                 ORDER BY ptp.StartDate DESC LIMIT 1) as FactionID
            FROM BillCollaborations bc
            UNION
            SELECT DISTINCT
                bc.SuppPersonID as PersonID,
                bc.KnessetNum,
                (SELECT f.FactionID
                 FROM KNS_PersonToPosition ptp
                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                 WHERE ptp.PersonID = bc.SuppPersonID
                     AND ptp.KnessetNum = bc.KnessetNum
                 ORDER BY ptp.StartDate DESC LIMIT 1) as FactionID
            FROM BillCollaborations bc
        ),
        FactionCollaborations AS (
            SELECT
                main_f.FactionID as MainFactionID,
                main_f.Name as MainFactionName,
                COALESCE(main_ufs.CoalitionStatus, 'Unknown') as MainCoalitionStatus,
                COALESCE(supp_ufs.CoalitionStatus, 'Unknown') as SupporterCoalitionStatus,
                COUNT(DISTINCT bc.BillID) as CollaborationCount
            FROM BillCollaborations bc
            JOIN PersonFactions main_pf ON bc.MainPersonID = main_pf.PersonID AND bc.KnessetNum = main_pf.KnessetNum
            JOIN PersonFactions supp_pf ON bc.SuppPersonID = supp_pf.PersonID AND bc.KnessetNum = supp_pf.KnessetNum
            JOIN KNS_Faction main_f ON main_pf.FactionID = main_f.FactionID
            JOIN KNS_Faction supp_f ON supp_pf.FactionID = supp_f.FactionID
            LEFT JOIN UserFactionCoalitionStatus main_ufs ON main_f.FactionID = main_ufs.FactionID AND bc.KnessetNum = main_ufs.KnessetNum
            LEFT JOIN UserFactionCoalitionStatus supp_ufs ON supp_f.FactionID = supp_ufs.FactionID AND bc.KnessetNum = supp_ufs.KnessetNum
            WHERE main_pf.FactionID IS NOT NULL
                AND supp_pf.FactionID IS NOT NULL
                AND main_pf.FactionID <> supp_pf.FactionID
                AND supp_ufs.CoalitionStatus IS NOT NULL
                AND supp_ufs.CoalitionStatus IN ('Coalition', 'Opposition')
            GROUP BY main_f.FactionID, main_f.Name, main_ufs.CoalitionStatus, supp_ufs.CoalitionStatus
        )
        SELECT
            MainFactionID,
            MainFactionName,
            MainCoalitionStatus,
            SUM(CollaborationCount) as TotalCollaborations,
            SUM(CASE WHEN SupporterCoalitionStatus = 'Coalition' THEN CollaborationCount ELSE 0 END) as CoalitionCollaborations,
            SUM(CASE WHEN SupporterCoalitionStatus = 'Opposition' THEN CollaborationCount ELSE 0 END) as OppositionCollaborations,
            ROUND(
                100.0 * SUM(CASE WHEN SupporterCoalitionStatus = 'Coalition' THEN CollaborationCount ELSE 0 END) /
                SUM(CollaborationCount), 1
            ) as CoalitionPercentage,
            ROUND(
                100.0 * SUM(CASE WHEN SupporterCoalitionStatus = 'Opposition' THEN CollaborationCount ELSE 0 END) /
                SUM(CollaborationCount), 1
            ) as OppositionPercentage
        FROM FactionCollaborations
        GROUP BY MainFactionID, MainFactionName, MainCoalitionStatus
        HAVING SUM(CollaborationCount) >= {min_collaborations}
        ORDER BY TotalCollaborations DESC
        """

    def _create_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create stacked bar chart showing faction collaboration breakdown by Coalition vs Opposition."""
        # Sort by total collaborations descending
        df_sorted = df.sort_values('TotalCollaborations', ascending=True)  # Ascending for horizontal bar

        # Prepare data
        faction_names = df_sorted['MainFactionName'].tolist()
        coalition_pct = df_sorted['CoalitionPercentage'].tolist()
        opposition_pct = df_sorted['OppositionPercentage'].tolist()
        total_collaborations = df_sorted['TotalCollaborations'].tolist()
        faction_status = df_sorted['MainCoalitionStatus'].tolist()

        # Create horizontal stacked bar chart
        fig = go.Figure()

        # Add Coalition bars
        fig.add_trace(go.Bar(
            name='Coalition Collaborations',
            y=faction_names,
            x=coalition_pct,
            orientation='h',
            marker=dict(color=COALITION_STATUS_COLORS['Coalition'], opacity=0.8),
            hovertemplate='<b>%{y}</b><br>Coalition: %{x}%<br>Count: %{customdata}<extra></extra>',
            customdata=df_sorted['CoalitionCollaborations'].tolist()
        ))

        # Add Opposition bars
        fig.add_trace(go.Bar(
            name='Opposition Collaborations',
            y=faction_names,
            x=opposition_pct,
            orientation='h',
            marker=dict(color=COALITION_STATUS_COLORS['Opposition'], opacity=0.8),
            hovertemplate='<b>%{y}</b><br>Opposition: %{x}%<br>Count: %{customdata}<extra></extra>',
            customdata=df_sorted['OppositionCollaborations'].tolist()
        ))

        # Add total collaboration annotations
        for i, (faction, total, coal_pct, opp_pct, status) in enumerate(
            zip(faction_names, total_collaborations, coalition_pct, opposition_pct, faction_status)
        ):
            fig.add_annotation(
                x=102,  # Just outside the 100% mark
                y=i,
                text=f"Total: {int(total)}",
                showarrow=False,
                font=dict(size=10, color='black'),
                xanchor='left'
            )

        fig.update_layout(
            title=(
                f"<b>Faction Collaboration Breakdown - Coalition vs Opposition<br>{title_suffix}</b>"
            ),
            title_x=0.5,
            title_y=0.98,
            xaxis=dict(
                title="Percentage of Collaborations (%)",
                range=[0, 120],
                showgrid=True,
                gridcolor='lightgray'
            ),
            yaxis=dict(
                title="Political Factions",
                showgrid=False
            ),
            barmode='stack',
            height=max(400, len(faction_names) * 40),
            margin=dict(l=150, r=100, t=120, b=50),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.05,
                xanchor="center",
                x=0.5,
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="gray",
                borderwidth=1
            ),
            plot_bgcolor='rgba(240,240,240,0.1)'
        )

        return fig

    def generate(self, **kwargs) -> Optional[go.Figure]:
        """Generate the coalition breakdown chart.

        Required implementation of BaseChart abstract method.
        Delegates to plot() with the provided kwargs.
        """
        return self.plot(**kwargs)
