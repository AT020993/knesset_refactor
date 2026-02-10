"""Agenda Comparison Charts.

This module provides functionality for visualizing agenda analytics:
- Agendas per faction
- Agendas by coalition status
"""

import logging
from typing import Any, Callable, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from ..base import BaseChart


class AgendaComparisonCharts(BaseChart):
    """Agenda-related comparison charts."""

    def plot_agendas_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda items per initiating faction chart."""
        if not self.check_database_exists():
            return None

        if not knesset_filter or len(knesset_filter) != 1:
            st.info(
                "Please select a single Knesset to view the 'Agendas per Faction' plot."
            )
            self.logger.info(
                "plot_agendas_per_faction requires a single Knesset filter."
            )
            return None

        single_knesset_num = knesset_filter[0]

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                required_tables = [
                    "KNS_Agenda",
                    "KNS_Person",
                    "KNS_PersonToPosition",
                    "KNS_Faction",
                ]
                if not self.check_tables_exist(con, required_tables):
                    return None

                # First, get total agenda counts by classification to show inclusive proposals note
                classification_query = """
                SELECT
                    ClassificationDesc,
                    COUNT(*) as count
                FROM KNS_Agenda
                WHERE KnessetNum = ?
                GROUP BY ClassificationDesc
                """
                classification_df = safe_execute_query(
                    con, classification_query, self.logger, params=[single_knesset_num]
                )

                inclusive_count = 0
                independent_count = 0
                if not classification_df.empty:
                    for _, row in classification_df.iterrows():
                        if row["ClassificationDesc"] == "כוללת":
                            inclusive_count = int(row["count"])
                        elif row["ClassificationDesc"] == "עצמאית":
                            independent_count = int(row["count"])

                date_column = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"

                query = f"""
                SELECT
                    COALESCE(p2p.FactionName, f_fallback.Name) AS FactionName,
                    p2p.FactionID,
                    COUNT(DISTINCT a.AgendaID) AS AgendaCount
                FROM KNS_Agenda a
                JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
                LEFT JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
                    AND a.KnessetNum = p2p.KnessetNum
                    AND CAST({date_column} AS TIMESTAMP)
                        BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                        AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID
                    AND a.KnessetNum = f_fallback.KnessetNum
                WHERE a.KnessetNum = ? AND a.InitiatorPersonID IS NOT NULL
                    AND COALESCE(p2p.FactionName, f_fallback.Name) IS NOT NULL
                """

                params: List[Any] = [single_knesset_num]
                if faction_filter:
                    valid_ids = [
                        str(fid) for fid in faction_filter if str(fid).isdigit()
                    ]
                    if valid_ids:
                        placeholders = ", ".join("?" for _ in valid_ids)
                        query += f" AND p2p.FactionID IN ({placeholders})"
                        params.extend(valid_ids)

                query += """
                GROUP BY COALESCE(p2p.FactionName, f_fallback.Name), p2p.FactionID
                HAVING AgendaCount > 0
                ORDER BY AgendaCount DESC;
                """

                self.logger.debug(
                    "Executing SQL for plot_agendas_per_faction (Knesset %s): %s",
                    single_knesset_num,
                    query,
                )
                df = safe_execute_query(con, query, self.logger, params=params)

                if df.empty:
                    st.info(
                        f"No agenda data found for Knesset {single_knesset_num} with the current filters."
                    )
                    return None

                df["AgendaCount"] = pd.to_numeric(
                    df["AgendaCount"], errors="coerce"
                ).fillna(0)

                # Build title with inclusive proposals note
                total_agendas = inclusive_count + independent_count
                if inclusive_count > 0:
                    inclusive_pct = round(inclusive_count * 100.0 / total_agendas, 1) if total_agendas > 0 else 0
                    title = (
                        f"<b>Agendas per Initiating Faction (Knesset {single_knesset_num})</b><br>"
                        f"<sub>Showing {independent_count} independent proposals only. "
                        f"{inclusive_count} inclusive/unified proposals ({inclusive_pct}%) have no single initiator.</sub>"
                    )
                else:
                    title = f"<b>Agendas per Initiating Faction (Knesset {single_knesset_num})</b>"

                fig = px.bar(
                    df,
                    x="FactionName",
                    y="AgendaCount",
                    color="FactionName",
                    title=title,
                    labels={
                        "FactionName": "Faction",
                        "AgendaCount": "Number of Agenda Items",
                    },
                    hover_name="FactionName",
                    custom_data=["AgendaCount"],
                    color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE,
                )

                fig.update_traces(
                    hovertemplate="<b>Faction:</b> %{x}<br><b>Agenda Items:</b> %{customdata[0]}<extra></extra>"
                )

                fig.update_layout(
                    xaxis_title="Initiating Faction",
                    yaxis_title="Number of Agenda Items",
                    title_x=0.5,
                    xaxis_tickangle=-45,
                    showlegend=False,
                    height=800,
                    margin=dict(t=180),
                )

                return fig

        except Exception as e:
            self.logger.error(
                "Error generating 'plot_agendas_per_faction' for Knesset %s: %s",
                single_knesset_num,
                e,
                exc_info=True,
            )
            st.error(f"Could not generate 'Agendas per Faction' plot: {e}")
            return None

    def plot_agendas_by_coalition_status(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda distribution by coalition/opposition status."""
        if not self.check_database_exists():
            return None

        if not knesset_filter or len(knesset_filter) != 1:
            st.info(
                "Please select a single Knesset to view the 'Agendas by Coalition Status' plot."
            )
            self.logger.info(
                "plot_agendas_by_coalition_status requires a single Knesset filter."
            )
            return None

        single_knesset_num = knesset_filter[0]

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                required_tables = [
                    "KNS_Agenda",
                    "KNS_Person",
                    "KNS_PersonToPosition",
                    "UserFactionCoalitionStatus",
                ]
                if not self.check_tables_exist(con, required_tables):
                    return None

                # First, get total agenda counts by classification to show inclusive proposals note
                classification_query = """
                SELECT
                    ClassificationDesc,
                    COUNT(*) as count
                FROM KNS_Agenda
                WHERE KnessetNum = ?
                GROUP BY ClassificationDesc
                """
                classification_df = safe_execute_query(
                    con, classification_query, self.logger, params=[single_knesset_num]
                )

                inclusive_count = 0
                independent_count = 0
                if not classification_df.empty:
                    for _, row in classification_df.iterrows():
                        if row["ClassificationDesc"] == "כוללת":
                            inclusive_count = int(row["count"])
                        elif row["ClassificationDesc"] == "עצמאית":
                            independent_count = int(row["count"])

                date_column = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"

                # Build faction filter condition
                faction_filter_sql = ""
                params: List[Any] = [single_knesset_num]
                if faction_filter:
                    valid_ids = [
                        str(fid) for fid in faction_filter if str(fid).isdigit()
                    ]
                    if valid_ids:
                        placeholders = ", ".join("?" for _ in valid_ids)
                        faction_filter_sql = f" AND p2p.FactionID IN ({placeholders})"
                        params.extend(valid_ids)

                # Use CTE to deduplicate: each agenda gets ONE faction (prefer non-NULL)
                # This fixes the double-counting issue where a person may have multiple positions
                query = f"""
                WITH AgendaWithFaction AS (
                    SELECT DISTINCT ON (a.AgendaID)
                        a.AgendaID,
                        p2p.FactionID
                    FROM KNS_Agenda a
                    JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
                    LEFT JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
                        AND a.KnessetNum = p2p.KnessetNum
                        AND CAST({date_column} AS TIMESTAMP)
                            BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    WHERE a.KnessetNum = ? AND a.InitiatorPersonID IS NOT NULL
                        {faction_filter_sql}
                    ORDER BY a.AgendaID, p2p.FactionID NULLS LAST
                )
                SELECT
                    COALESCE(ufs.CoalitionStatus, 'Unmapped') AS CoalitionStatus,
                    COUNT(*) AS AgendaCount
                FROM AgendaWithFaction awf
                LEFT JOIN UserFactionCoalitionStatus ufs ON awf.FactionID = ufs.FactionID
                    AND ufs.KnessetNum = ?
                GROUP BY CoalitionStatus
                HAVING AgendaCount > 0
                ORDER BY AgendaCount DESC
                """
                params.append(single_knesset_num)

                self.logger.debug(
                    "Executing SQL for plot_agendas_by_coalition_status (Knesset %s): %s",
                    single_knesset_num,
                    query,
                )
                df = safe_execute_query(con, query, self.logger, params=params)

                if df.empty:
                    st.info(
                        f"No agenda data for Knesset {single_knesset_num} to visualize 'Agendas by Coalition Status'."
                    )
                    return None

                df["AgendaCount"] = pd.to_numeric(
                    df["AgendaCount"], errors="coerce"
                ).fillna(0)

                # Build title with inclusive proposals note
                total_agendas = inclusive_count + independent_count
                if inclusive_count > 0:
                    inclusive_pct = round(inclusive_count * 100.0 / total_agendas, 1) if total_agendas > 0 else 0
                    title = (
                        f"<b>Agendas by Initiator Coalition Status (Knesset {single_knesset_num})</b><br>"
                        f"<sub>Showing {independent_count} independent proposals only. "
                        f"{inclusive_count} inclusive/unified proposals ({inclusive_pct}%) have no single initiator.</sub>"
                    )
                else:
                    title = f"<b>Agendas by Initiator Coalition Status (Knesset {single_knesset_num})</b>"

                # Add Unmapped to color map
                coalition_colors = {**self.config.COALITION_OPPOSITION_COLORS, "Unmapped": "#808080"}

                fig = px.pie(
                    df,
                    values="AgendaCount",
                    names="CoalitionStatus",
                    title=title,
                    color="CoalitionStatus",
                    color_discrete_map=coalition_colors,
                )

                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(title_x=0.5, height=600, margin=dict(t=120))

                return fig

        except Exception as e:
            self.logger.error(
                "Error generating 'plot_agendas_by_coalition_status' for Knesset %s: %s",
                single_knesset_num,
                e,
                exc_info=True,
            )
            st.error(f"Could not generate 'Agendas by Coalition Status' plot: {e}")
            return None

    def generate(self, chart_type: str = "", **kwargs: Any) -> Optional[go.Figure]:
        """Generate the requested agenda comparison chart.

        Args:
            chart_type: Type of chart to generate
            **kwargs: Chart-specific arguments

        Returns:
            Plotly Figure object or None if unknown chart type
        """
        chart_methods: dict[str, Callable[..., Optional[go.Figure]]] = {
            "agendas_per_faction": self.plot_agendas_per_faction,
            "agendas_by_coalition_status": self.plot_agendas_by_coalition_status,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown agenda chart type: {chart_type}")
            return None
