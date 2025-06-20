"""Comparison and faction analysis chart generators."""

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query

from .base import BaseChart


class ComparisonCharts(BaseChart):
    """Comparison charts for factions, ministries, etc."""

    def plot_queries_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate queries per faction chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q")

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(
                    con, ["KNS_Query", "KNS_PersonToPosition"]
                ):
                    return None

                query = f"""
                    SELECT
                        COALESCE(p2p.FactionName, 'Unknown') AS FactionName,
                        COUNT(q.QueryID) AS QueryCount
                    FROM KNS_Query q
                    LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID 
                        AND q.KnessetNum = p2p.KnessetNum
                    WHERE q.KnessetNum IS NOT NULL
                        AND {filters['knesset_condition']}
                    GROUP BY p2p.FactionName
                    ORDER BY QueryCount DESC
                    LIMIT 20
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No faction query data found for '{filters['knesset_title']}'."
                    )
                    return None

                fig = px.bar(
                    df,
                    x="FactionName",
                    y="QueryCount",
                    title=f"<b>Queries per Faction for {filters['knesset_title']}</b>",
                    labels={
                        "FactionName": "Faction",
                        "QueryCount": "Number of Queries",
                    },
                    color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE,
                )

                fig.update_layout(
                    xaxis_title="Faction",
                    yaxis_title="Number of Queries",
                    title_x=0.5,
                    xaxis_tickangle=-45,
                )

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating queries per faction chart: {e}", exc_info=True
            )
            st.error(f"Could not generate queries per faction chart: {e}")
            return None

    def plot_queries_by_coalition_status(self, **kwargs) -> Optional[go.Figure]:
        """Generate queries by coalition/opposition status chart."""
        # TODO: Implement from original plot_generators.py
        pass

    def plot_queries_by_ministry(self, **kwargs) -> Optional[go.Figure]:
        """Generate queries by ministry chart."""
        # TODO: Implement from original plot_generators.py
        pass

    def plot_agendas_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agendas per faction chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a")

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(
                    con, ["KNS_Agenda", "KNS_PersonToPosition"]
                ):
                    return None

                date_col = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"
                query = f"""
                    SELECT
                        COALESCE(p2p.FactionName, 'Unknown') AS FactionName,
                        COUNT(DISTINCT a.AgendaID) AS AgendaCount
                    FROM KNS_Agenda a
                    LEFT JOIN KNS_PersonToPosition p2p ON a.InitiatorPersonID = p2p.PersonID
                        AND a.KnessetNum = p2p.KnessetNum
                        AND CAST({date_col} AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    WHERE a.KnessetNum IS NOT NULL
                        AND a.InitiatorPersonID IS NOT NULL
                        AND {filters['knesset_condition']}
                        AND {filters['faction_condition']}
                    GROUP BY FactionName
                    ORDER BY AgendaCount DESC
                    LIMIT 20
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No agenda data found for '{filters['knesset_title']}'.")
                    return None

                fig = px.bar(
                    df,
                    x="FactionName",
                    y="AgendaCount",
                    title=f"<b>Agendas per Faction for {filters['knesset_title']}</b>",
                    labels={
                        "FactionName": "Faction",
                        "AgendaCount": "Number of Agendas",
                    },
                    color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE,
                )

                fig.update_layout(
                    xaxis_title="Faction",
                    yaxis_title="Number of Agendas",
                    title_x=0.5,
                    xaxis_tickangle=-45,
                )

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating agendas per faction chart: {e}", exc_info=True
            )
            st.error(f"Could not generate agendas per faction chart: {e}")
            return None

    def plot_agendas_by_coalition_status(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agendas by coalition/opposition status chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a")

        if not filters.get("is_single_knesset"):
            st.info(
                "Please select a single Knesset to view the 'Agendas by Coalition Status' plot."
            )
            return None

        single_knesset = knesset_filter[0] if knesset_filter else None

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(
                    con,
                    [
                        "KNS_Agenda",
                        "KNS_PersonToPosition",
                        "UserFactionCoalitionStatus",
                    ],
                ):
                    return None

                date_col = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"
                query = f"""
                    SELECT
                        COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
                        COUNT(DISTINCT a.AgendaID) AS AgendaCount
                    FROM KNS_Agenda a
                    LEFT JOIN KNS_PersonToPosition p2p ON a.InitiatorPersonID = p2p.PersonID
                        AND a.KnessetNum = p2p.KnessetNum
                        AND CAST({date_col} AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID
                        AND a.KnessetNum = ufs.KnessetNum
                    WHERE a.KnessetNum = {single_knesset}
                        AND a.InitiatorPersonID IS NOT NULL
                        AND p2p.FactionID IS NOT NULL
                        AND {filters['faction_condition']}
                    GROUP BY CoalitionStatus
                    ORDER BY AgendaCount DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No agenda data found for Knesset {single_knesset} to visualize 'Agendas by Coalition Status'."
                    )
                    return None

                fig = px.bar(
                    df,
                    x="CoalitionStatus",
                    y="AgendaCount",
                    title=f"<b>Agendas by Coalition Status (Knesset {single_knesset})</b>",
                    labels={
                        "CoalitionStatus": "Initiator Coalition Status",
                        "AgendaCount": "Number of Agendas",
                    },
                    color="CoalitionStatus",
                    color_discrete_map=self.config.COALITION_OPPOSITION_COLORS,
                )

                fig.update_layout(
                    xaxis_title="Coalition Status",
                    yaxis_title="Number of Agendas",
                    title_x=0.5,
                    showlegend=False,
                )

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating agendas by coalition status chart: {e}",
                exc_info=True,
            )
            st.error(f"Could not generate agendas by coalition status chart: {e}")
            return None

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested comparison chart."""
        chart_methods = {
            "queries_per_faction": self.plot_queries_per_faction,
            "queries_by_coalition_status": self.plot_queries_by_coalition_status,
            "queries_by_ministry": self.plot_queries_by_ministry,
            "agendas_per_faction": self.plot_agendas_per_faction,
            "agendas_by_coalition_status": self.plot_agendas_by_coalition_status,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown comparison chart type: {chart_type}")
            return None
