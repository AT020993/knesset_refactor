"""Comparison and faction analysis chart generators."""

import logging
from pathlib import Path
from typing import List, Optional, Any

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
                        AND {filters["knesset_condition"]}
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

    def plot_query_status_by_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate query status description with faction breakdown chart."""
        if not self.check_database_exists():
            return None

        if not knesset_filter or len(knesset_filter) != 1:
            st.info(
                "Please select a single Knesset to view the 'Query Status Description with Faction Breakdown' plot."
            )
            self.logger.info(
                "plot_query_status_by_faction requires a single Knesset filter."
            )
            return None

        if start_date and end_date and start_date > end_date:
            st.error("Start date must be before or equal to end date.")
            self.logger.error(
                "Invalid date range: start_date=%s, end_date=%s",
                start_date,
                end_date,
            )
            return None

        single_knesset_num = knesset_filter[0]

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                required_tables = [
                    "KNS_Query",
                    "KNS_Person",
                    "KNS_PersonToPosition",
                    "KNS_Status",
                    "KNS_Faction",
                ]
                if not self.check_tables_exist(con, required_tables):
                    return None

                params: List[Any] = [single_knesset_num]
                conditions = [
                    "q.KnessetNum = ?",
                    "q.SubmitDate IS NOT NULL",
                    "p2p.FactionID IS NOT NULL",
                ]

                if start_date:
                    conditions.append("CAST(q.SubmitDate AS DATE) >= ?")
                    params.append(start_date)
                if end_date:
                    conditions.append("CAST(q.SubmitDate AS DATE) <= ?")
                    params.append(end_date)

                if faction_filter:
                    valid_ids = [
                        str(fid) for fid in faction_filter if str(fid).isdigit()
                    ]
                    if valid_ids:
                        placeholders = ", ".join("?" for _ in valid_ids)
                        conditions.append(f"p2p.FactionID IN ({placeholders})")
                        params.extend(valid_ids)

                where_clause = " AND ".join(conditions)

                sql_query = f"""
                WITH QueryStatusFactionInfo AS (
                    SELECT
                        q.QueryID,
                        COALESCE(s.Desc, 'Unknown Status') AS StatusDescription,
                        COALESCE(p2p.FactionName, f_fallback.Name, 'Unknown Faction') AS FactionName,
                        p2p.FactionID
                    FROM KNS_Query q
                    JOIN KNS_Person p ON q.PersonID = p.PersonID
                    LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID
                    LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                        AND q.KnessetNum = p2p.KnessetNum
                        AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID AND q.KnessetNum = p2p.KnessetNum
                    WHERE {where_clause}
                )
                SELECT
                    qsfi.StatusDescription,
                    qsfi.FactionName,
                    COUNT(DISTINCT qsfi.QueryID) AS QueryCount
                FROM QueryStatusFactionInfo qsfi
                GROUP BY
                    qsfi.StatusDescription,
                    qsfi.FactionName
                HAVING QueryCount > 0
                ORDER BY
                    qsfi.StatusDescription,
                    QueryCount DESC;
                """

                date_filter_info = ""
                if start_date or end_date:
                    date_filter_info = f" with date filter: {start_date or 'None'} to {end_date or 'None'}"
                self.logger.debug(
                    "Executing SQL for plot_query_status_by_faction (Knesset %s)%s: %s",
                    single_knesset_num,
                    date_filter_info,
                    sql_query,
                )

                df = safe_execute_query(con, sql_query, self.logger, params=params)

                if df.empty:
                    st.info(
                        f"No query data for Knesset {single_knesset_num} to visualize 'Query Status by Faction' with the current filters."
                    )
                    self.logger.info(
                        "No data for 'Query Status by Faction' plot (Knesset %s).",
                        single_knesset_num,
                    )
                    return None

                df["QueryCount"] = pd.to_numeric(
                    df["QueryCount"], errors="coerce"
                ).fillna(0)
                df["StatusDescription"] = df["StatusDescription"].fillna(
                    "Unknown Status"
                )
                df["FactionName"] = df["FactionName"].fillna("Unknown Faction")

                ids = []
                labels = []
                parents = []
                values = []

                status_totals = (
                    df.groupby("StatusDescription")["QueryCount"].sum().reset_index()
                )
                for _, row in status_totals.iterrows():
                    status = row["StatusDescription"]
                    ids.append(status)
                    labels.append(f"{status}<br>({row['QueryCount']} queries)")
                    parents.append("")
                    values.append(row["QueryCount"])

                for _, row in df.iterrows():
                    status = row["StatusDescription"]
                    faction = row["FactionName"]
                    count = row["QueryCount"]
                    faction_id = f"{status} - {faction}"
                    ids.append(faction_id)
                    labels.append(f"{faction}<br>({count} queries)")
                    parents.append(status)
                    values.append(count)

                title = f"<b>Query Status with Faction Breakdown for Knesset {single_knesset_num}</b>"
                if start_date or end_date:
                    if start_date and end_date:
                        date_range_text = f" ({start_date} to {end_date})"
                    elif start_date:
                        date_range_text = f" (from {start_date})"
                    else:
                        date_range_text = f" (until {end_date})"
                    title = f"<b>Query Status with Faction Breakdown for Knesset {single_knesset_num}{date_range_text}</b>"

                fig = go.Figure(
                    go.Sunburst(
                        ids=ids,
                        labels=labels,
                        parents=parents,
                        values=values,
                        branchvalues="total",
                        hovertemplate="<b>%{label}</b><br>Queries: %{value}<br>Percentage: %{percentParent}<extra></extra>",
                        maxdepth=2,
                    )
                )

                fig.update_layout(
                    title=title,
                    title_x=0.5,
                    font_size=12,
                    margin=dict(t=50, l=0, r=0, b=0),
                )

                return fig

        except Exception as e:
            self.logger.error(
                "Error generating 'plot_query_status_by_faction' for Knesset %s: %s",
                single_knesset_num,
                e,
                exc_info=True,
            )
            st.error(f"Could not generate 'Query Status by Faction' plot: {e}")
            return None

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested comparison chart."""
        chart_methods = {
            "queries_per_faction": self.plot_queries_per_faction,
            "queries_by_coalition_status": self.plot_queries_by_coalition_status,
            "queries_by_ministry": self.plot_queries_by_ministry,
            "query_status_by_faction": self.plot_query_status_by_faction,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown comparison chart type: {chart_type}")
            return None
