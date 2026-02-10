"""Bill Comparison Charts.

This module provides functionality for visualizing bill analytics:
- Bills per faction
- Bills by coalition status
- Top bill initiators
"""

import logging
from typing import Any, Callable, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from ui.queries.sql_templates import SQLTemplates
from ..base import BaseChart


class BillComparisonCharts(BaseChart):
    """Bill-related comparison charts."""

    def plot_bills_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate bills per initiating faction chart."""
        if not self.check_database_exists():
            return None

        if not knesset_filter or len(knesset_filter) != 1:
            st.info(
                "Please select a single Knesset to view the 'Bills per Faction' plot."
            )
            self.logger.info(
                "plot_bills_per_faction requires a single Knesset filter."
            )
            return None

        single_knesset_num = knesset_filter[0]

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                required_tables = [
                    "KNS_Bill",
                    "KNS_BillInitiator",
                    "KNS_PersonToPosition",
                    "KNS_Faction",
                ]
                if not self.check_tables_exist(con, required_tables):
                    return None

                query = f"""
                WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                SELECT
                    COALESCE(p2p.FactionName, f_fallback.Name) AS FactionName,
                    {SQLTemplates.BILL_STATUS_CASE_HE} AS Stage,
                    COUNT(DISTINCT b.BillID) AS BillCount
                FROM KNS_Bill b
                LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                LEFT JOIN KNS_PersonToPosition p2p ON bi.PersonID = p2p.PersonID
                    AND b.KnessetNum = p2p.KnessetNum
                    AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                        BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                        AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID
                    AND b.KnessetNum = f_fallback.KnessetNum
                WHERE b.KnessetNum = ?
                    AND bi.Ordinal = 1  -- Count only main/primary initiators (not supporting members)
                    AND COALESCE(p2p.FactionName, f_fallback.Name) IS NOT NULL
                """

                params: List[Any] = [single_knesset_num]

                # Build filters using the base class method
                filters = self.build_filters([single_knesset_num], faction_filter, table_prefix="b", **kwargs)
                query += f" AND {filters['bill_origin_condition']}"

                if faction_filter:
                    valid_ids = [
                        str(fid) for fid in faction_filter if str(fid).isdigit()
                    ]
                    if valid_ids:
                        placeholders = ", ".join("?" for _ in valid_ids)
                        query += f" AND p2p.FactionID IN ({placeholders})"
                        params.extend(valid_ids)

                query += """
                GROUP BY COALESCE(p2p.FactionName, f_fallback.Name), Stage
                HAVING BillCount > 0
                ORDER BY FactionName, Stage;
                """

                self.logger.debug(
                    "Executing SQL for plot_bills_per_faction (Knesset %s): %s",
                    single_knesset_num,
                    query,
                )
                df = safe_execute_query(con, query, self.logger, params=params)

                if df.empty:
                    st.info(
                        f"No bill data found for Knesset {single_knesset_num} with the current filters."
                    )
                    return None

                df["BillCount"] = pd.to_numeric(
                    df["BillCount"], errors="coerce"
                ).fillna(0)

                # Use centralized stage order and colors from SQLTemplates
                stage_order = SQLTemplates.BILL_STAGE_ORDER
                stage_colors = SQLTemplates.BILL_STAGE_COLORS

                # Sort factions by total bill count
                faction_totals = df.groupby('FactionName')['BillCount'].sum().sort_values(ascending=False)
                faction_order = faction_totals.index.tolist()

                # Create figure with manual traces for proper stacking
                fig = go.Figure()

                # Add a trace for each stage
                for stage in stage_order:
                    stage_data = df[df['Stage'] == stage].set_index('FactionName')
                    counts = [stage_data.loc[faction, 'BillCount'] if faction in stage_data.index else 0
                             for faction in faction_order]

                    fig.add_trace(go.Bar(
                        name=stage,
                        x=faction_order,
                        y=counts,
                        marker_color=stage_colors[stage],
                        text=counts,
                        textposition='inside',
                        textfont=dict(color='white', size=12),
                        hovertemplate='<b>%{x}</b><br>' + f'{stage}: %{{y}}<br>' + '<extra></extra>'
                    ))

                fig.update_layout(
                    barmode='stack',
                    title=f"<b>Bills per Initiating Faction by Status (Knesset {single_knesset_num})</b><br><sub>Main initiators only</sub>",
                    title_x=0.5,
                    xaxis_title="Initiating Faction",
                    yaxis_title="Number of Bills",
                    xaxis_tickangle=-45,
                    showlegend=True,
                    legend_title_text='Bill Status',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    height=800,
                    margin=dict(t=180),
                    font_size=12,
                    xaxis=dict(automargin=True),
                    yaxis=dict(gridcolor="lightgray"),
                    plot_bgcolor="white"
                )

                return fig

        except Exception as e:
            self.logger.error(
                "Error generating 'plot_bills_per_faction' for Knesset %s: %s",
                single_knesset_num,
                e,
                exc_info=True,
            )
            st.error(f"Could not generate 'Bills per Faction' plot: {e}")
            return None

    def plot_bills_by_coalition_status(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate bill distribution by coalition/opposition status."""
        if not self.check_database_exists():
            return None

        if not knesset_filter or len(knesset_filter) != 1:
            st.info(
                "Please select a single Knesset to view the 'Bills by Coalition Status' plot."
            )
            self.logger.info(
                "plot_bills_by_coalition_status requires a single Knesset filter."
            )
            return None

        single_knesset_num = knesset_filter[0]

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                required_tables = [
                    "KNS_Bill",
                    "KNS_BillInitiator",
                    "KNS_PersonToPosition",
                    "UserFactionCoalitionStatus",
                ]
                if not self.check_tables_exist(con, required_tables):
                    return None

                query = f"""
                WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                SELECT
                    ufs.CoalitionStatus AS CoalitionStatus,
                    {SQLTemplates.BILL_STATUS_CASE_HE} AS Stage,
                    COUNT(DISTINCT b.BillID) AS BillCount
                FROM KNS_Bill b
                LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                LEFT JOIN KNS_PersonToPosition p2p ON bi.PersonID = p2p.PersonID
                    AND b.KnessetNum = p2p.KnessetNum
                    AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                        BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                        AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID
                    AND b.KnessetNum = ufs.KnessetNum
                WHERE b.KnessetNum = ?
                    AND ufs.CoalitionStatus IS NOT NULL
                """

                params: List[Any] = [single_knesset_num]

                # Build filters using the base class method
                filters = self.build_filters([single_knesset_num], faction_filter, table_prefix="b", **kwargs)
                query += f" AND {filters['bill_origin_condition']}"

                if faction_filter:
                    valid_ids = [
                        str(fid) for fid in faction_filter if str(fid).isdigit()
                    ]
                    if valid_ids:
                        placeholders = ", ".join("?" for _ in valid_ids)
                        query += f" AND p2p.FactionID IN ({placeholders})"
                        params.extend(valid_ids)

                query += """
                GROUP BY CoalitionStatus, Stage
                HAVING BillCount > 0
                ORDER BY CoalitionStatus, Stage;
                """

                self.logger.debug(
                    "Executing SQL for plot_bills_by_coalition_status (Knesset %s): %s",
                    single_knesset_num,
                    query,
                )
                df = safe_execute_query(con, query, self.logger, params=params)

                if df.empty:
                    st.info(
                        f"No bill data for Knesset {single_knesset_num} to visualize 'Bills by Coalition Status'."
                    )
                    return None

                df["BillCount"] = pd.to_numeric(
                    df["BillCount"], errors="coerce"
                ).fillna(0)

                # Use centralized stage order and colors from SQLTemplates
                stage_order = SQLTemplates.BILL_STAGE_ORDER
                stage_colors = SQLTemplates.BILL_STAGE_COLORS

                # Sort coalition statuses by total bill count
                coalition_totals = df.groupby('CoalitionStatus')['BillCount'].sum().sort_values(ascending=False)
                coalition_order = coalition_totals.index.tolist()

                # Create figure with manual traces for proper stacking
                fig = go.Figure()

                # Add a trace for each stage
                for stage in stage_order:
                    stage_data = df[df['Stage'] == stage].set_index('CoalitionStatus')
                    counts = [stage_data.loc[coalition, 'BillCount'] if coalition in stage_data.index else 0
                             for coalition in coalition_order]

                    fig.add_trace(go.Bar(
                        name=stage,
                        x=coalition_order,
                        y=counts,
                        marker_color=stage_colors[stage],
                        text=counts,
                        textposition='inside',
                        textfont=dict(color='white', size=12),
                        hovertemplate='<b>%{x}</b><br>' + f'{stage}: %{{y}}<br>' + '<extra></extra>'
                    ))

                fig.update_layout(
                    barmode='stack',
                    title=f"<b>Bills by Coalition Status and Bill Stage (Knesset {single_knesset_num})</b>",
                    title_x=0.5,
                    xaxis_title="Coalition Status",
                    yaxis_title="Number of Bills",
                    showlegend=True,
                    legend_title_text='Bill Status',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    height=800,
                    margin=dict(t=180),
                    font_size=12,
                    xaxis=dict(automargin=True),
                    yaxis=dict(gridcolor="lightgray"),
                    plot_bgcolor="white"
                )

                return fig

        except Exception as e:
            self.logger.error(
                "Error generating 'plot_bills_by_coalition_status' for Knesset %s: %s",
                single_knesset_num,
                e,
                exc_info=True,
            )
            st.error(f"Could not generate 'Bills by Coalition Status' plot: {e}")
            return None

    def plot_top_bill_initiators(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate top 10 Knesset members who were main initiators of bills."""
        if not self.check_database_exists():
            return None

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                required_tables = [
                    "KNS_Bill",
                    "KNS_BillInitiator",
                    "KNS_Person",
                    "KNS_PersonToPosition",
                    "KNS_Faction",
                ]
                if not self.check_tables_exist(con, required_tables):
                    return None

                params: List[Any] = []

                # Build base query - structure depends on Knesset selection
                if knesset_filter and len(knesset_filter) == 1:
                    # Single Knesset - simpler query without KnessetNum in SELECT
                    query = f"""
                    WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                    SELECT
                        p.FirstName || ' ' || p.LastName AS MKName,
                        p.PersonID,
                        {SQLTemplates.BILL_STATUS_CASE_HE} AS Stage,
                        COUNT(DISTINCT b.BillID) AS BillCount
                    FROM KNS_Bill b
                    LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                    JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                    JOIN KNS_Person p ON bi.PersonID = p.PersonID
                    LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
                        AND b.KnessetNum = ptp.KnessetNum
                        AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                            BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
                        AND ptp.FactionID IS NOT NULL
                    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    WHERE bi.Ordinal = 1  -- Main initiators only (not supporting members)
                        AND bi.PersonID IS NOT NULL
                        AND b.KnessetNum = ?
                    """
                    params.append(knesset_filter[0])
                    knesset_title = f"Knesset {knesset_filter[0]}"

                    # Add bill origin filter using build_filters method
                    temp_filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)
                    query += f" AND {temp_filters['bill_origin_condition']}"

                    # Add faction filter for single Knesset
                    if faction_filter:
                        valid_ids = [
                            str(fid) for fid in faction_filter if str(fid).isdigit()
                        ]
                        if valid_ids:
                            placeholders = ", ".join("?" for _ in valid_ids)
                            query += f" AND ptp.FactionID IN ({placeholders})"
                            params.extend(valid_ids)

                    query += """
                    GROUP BY p.PersonID, p.FirstName, p.LastName, Stage
                    ORDER BY p.PersonID, Stage;
                    """

                else:
                    # Multiple Knessets - include KnessetNum in SELECT and GROUP BY
                    query = f"""
                    WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                    SELECT
                        p.FirstName || ' ' || p.LastName AS MKName,
                        p.PersonID,
                        {SQLTemplates.BILL_STATUS_CASE_HE} AS Stage,
                        COUNT(DISTINCT b.BillID) AS BillCount,
                        b.KnessetNum
                    FROM KNS_Bill b
                    LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                    JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                    JOIN KNS_Person p ON bi.PersonID = p.PersonID
                    LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
                        AND b.KnessetNum = ptp.KnessetNum
                        AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                            BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
                        AND ptp.FactionID IS NOT NULL
                    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    WHERE bi.Ordinal = 1  -- Main initiators only (not supporting members)
                        AND bi.PersonID IS NOT NULL
                    """

                    # Add Knesset filter for multiple Knessets
                    if knesset_filter:
                        knesset_placeholders = ", ".join("?" for _ in knesset_filter)
                        query += f" AND b.KnessetNum IN ({knesset_placeholders})"
                        params.extend(knesset_filter)
                        knesset_title = f"Knessets: {', '.join(map(str, knesset_filter))}"
                    else:
                        knesset_title = "All Knessets"

                    # Add bill origin filter using build_filters method
                    temp_filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)
                    query += f" AND {temp_filters['bill_origin_condition']}"

                    # Add faction filter for multiple Knessets
                    if faction_filter:
                        valid_ids = [
                            str(fid) for fid in faction_filter if str(fid).isdigit()
                        ]
                        if valid_ids:
                            placeholders = ", ".join("?" for _ in valid_ids)
                            query += f" AND ptp.FactionID IN ({placeholders})"
                            params.extend(valid_ids)

                    query += """
                    GROUP BY p.PersonID, p.FirstName, p.LastName, Stage, b.KnessetNum
                    ORDER BY p.PersonID, Stage;
                    """

                self.logger.debug(
                    "Executing SQL for plot_top_bill_initiators: %s",
                    query,
                )
                df = safe_execute_query(con, query, self.logger, params=params)

                if df.empty:
                    st.info(
                        f"No bill initiator data found for {knesset_title} with the current filters."
                    )
                    return None

                df["BillCount"] = pd.to_numeric(
                    df["BillCount"], errors="coerce"
                ).fillna(0)

                # Aggregate by person and stage to get top 10 MKs by total bills
                person_totals = df.groupby(["MKName", "PersonID"])["BillCount"].sum().sort_values(ascending=False).head(10)
                top_10_persons = person_totals.index.get_level_values("PersonID").tolist()

                # Filter data to include only top 10 MKs
                df = df[df["PersonID"].isin(top_10_persons)]

                # Sort MKs by total bill count
                mk_order = person_totals.index.get_level_values("MKName").tolist()

                # Use centralized stage order and colors from SQLTemplates
                stage_order = SQLTemplates.BILL_STAGE_ORDER
                stage_colors = SQLTemplates.BILL_STAGE_COLORS

                # Create figure with manual traces for proper stacking
                fig = go.Figure()

                # Add a trace for each stage
                for stage in stage_order:
                    stage_data = df[df['Stage'] == stage].set_index('MKName')
                    counts = [stage_data.loc[mk, 'BillCount'] if mk in stage_data.index else 0
                             for mk in mk_order]

                    fig.add_trace(go.Bar(
                        name=stage,
                        x=mk_order,
                        y=counts,
                        marker_color=stage_colors[stage],
                        text=counts,
                        textposition='inside',
                        textfont=dict(color='white', size=12),
                        hovertemplate='<b>%{x}</b><br>' + f'{stage}: %{{y}}<br>' + '<extra></extra>'
                    ))

                fig.update_layout(
                    barmode='stack',
                    title=f"<b>Top 10 Bill Initiators by Status ({knesset_title})</b>",
                    title_x=0.5,
                    xaxis_title="Knesset Member",
                    yaxis_title="Number of Bills",
                    xaxis_tickangle=-45,
                    showlegend=True,
                    legend_title_text='Bill Status',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    height=800,
                    margin=dict(t=180),
                    font_size=12,
                    xaxis=dict(automargin=True),
                    yaxis=dict(gridcolor="lightgray"),
                    plot_bgcolor="white"
                )

                return fig

        except Exception as e:
            self.logger.error(
                "Error generating 'plot_top_bill_initiators': %s",
                e,
                exc_info=True,
            )
            st.error(f"Could not generate 'Top Bill Initiators' plot: {e}")
            return None

    def generate(self, chart_type: str = "", **kwargs: Any) -> Optional[go.Figure]:
        """Generate the requested bill comparison chart.

        Args:
            chart_type: Type of chart to generate
            **kwargs: Chart-specific arguments

        Returns:
            Plotly Figure object or None if unknown chart type
        """
        chart_methods: dict[str, Callable[..., Optional[go.Figure]]] = {
            "bills_per_faction": self.plot_bills_per_faction,
            "bills_by_coalition_status": self.plot_bills_by_coalition_status,
            "top_bill_initiators": self.plot_top_bill_initiators,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown bill chart type: {chart_type}")
            return None
