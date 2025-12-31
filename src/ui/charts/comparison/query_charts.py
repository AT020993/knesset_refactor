"""Query Comparison Charts.

This module provides functionality for visualizing query analytics:
- Queries per faction
- Queries by ministry
- Query status by faction
"""

import logging
from typing import Any, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from ..base import BaseChart


class QueryComparisonCharts(BaseChart):
    """Query-related comparison charts."""

    def plot_queries_per_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate queries per faction chart with date-based faction attribution."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(
                    con, ["KNS_Query", "KNS_PersonToPosition", "KNS_Faction"]
                ):
                    return None

                # Use date-based faction attribution - queries attributed to faction MK belonged to at submission time
                query = f"""
                    SELECT
                        COALESCE(f.Name, 'Unknown') AS FactionName,
                        COUNT(DISTINCT q.QueryID) AS QueryCount
                    FROM KNS_Query q
                    LEFT JOIN KNS_PersonToPosition ptp ON q.PersonID = ptp.PersonID
                        AND q.KnessetNum = ptp.KnessetNum
                        AND CAST(q.SubmitDate AS TIMESTAMP)
                            BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
                    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    WHERE q.KnessetNum IS NOT NULL
                        AND q.SubmitDate IS NOT NULL
                        AND f.Name IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY f.Name
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

    def plot_queries_by_ministry(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """
        Generate queries by ministry and status chart.

        Shows query distribution and reply percentage by ministry for a specific Knesset.
        Requires a single Knesset to be selected.
        """
        if not self.check_database_exists():
            return None

        if not knesset_filter or len(knesset_filter) != 1:
            st.info(
                "Please select a single Knesset to view the 'Query Performance by Ministry' plot."
            )
            self.logger.info(
                "plot_queries_by_ministry requires a single Knesset filter."
            )
            return None

        single_knesset_num = knesset_filter[0]

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                required_tables = ["KNS_Query", "KNS_GovMinistry", "KNS_Status"]
                if not self.check_tables_exist(con, required_tables):
                    return None

                # Build answer status categorization
                answer_status_case_sql = """
                    CASE
                        WHEN s.Desc LIKE '%נענתה%' AND s.Desc NOT LIKE '%לא נענתה%' THEN 'Answered'
                        WHEN s.Desc LIKE '%לא נענתה%' THEN 'Not Answered'
                        WHEN s.Desc LIKE '%הועברה%' THEN 'Other/In Progress'
                        WHEN s.Desc LIKE '%בטיפול%' THEN 'Other/In Progress'
                        WHEN s.Desc LIKE '%נדחתה%' THEN 'Not Answered'
                        WHEN s.Desc LIKE '%הוסרה%' THEN 'Other/In Progress'
                        WHEN s.Desc LIKE '%נקבע תאריך%' THEN 'Other/In Progress'
                        ELSE 'Unknown'
                    END AS AnswerStatus
                """

                sql_query = f"""
                WITH MinistryQueryStats AS (
                    SELECT
                        q.GovMinistryID,
                        m.Name AS MinistryName,
                        {answer_status_case_sql},
                        COUNT(q.QueryID) AS QueryCount
                    FROM KNS_Query q
                    JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID
                    JOIN KNS_Status s ON q.StatusID = s.StatusID
                    WHERE q.KnessetNum = {single_knesset_num} AND q.GovMinistryID IS NOT NULL
                    GROUP BY q.GovMinistryID, m.Name, AnswerStatus
                )
                SELECT
                    MinistryName,
                    AnswerStatus,
                    QueryCount,
                    SUM(QueryCount) OVER (PARTITION BY MinistryName) AS TotalQueriesForMinistry,
                    SUM(CASE WHEN AnswerStatus = 'Answered' THEN QueryCount ELSE 0 END) OVER (PARTITION BY MinistryName) AS AnsweredQueriesForMinistry
                FROM MinistryQueryStats
                ORDER BY TotalQueriesForMinistry DESC, MinistryName,
                    CASE AnswerStatus
                        WHEN 'Answered' THEN 1
                        WHEN 'Not Answered' THEN 2
                        WHEN 'Other/In Progress' THEN 3
                        ELSE 4
                    END
                """

                self.logger.debug(
                    f"Executing SQL for plot_queries_by_ministry (Knesset {single_knesset_num}): {sql_query}"
                )
                result = safe_execute_query(con, sql_query, self.logger)

                if result is None or result.empty:
                    st.info(
                        f"No query data found for ministries in Knesset {single_knesset_num}."
                    )
                    return None

                df = result.copy()

                # Convert to numeric and calculate percentages
                df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors="coerce").fillna(0)
                df["TotalQueriesForMinistry"] = pd.to_numeric(
                    df["TotalQueriesForMinistry"], errors="coerce"
                ).fillna(0)
                df["AnsweredQueriesForMinistry"] = pd.to_numeric(
                    df["AnsweredQueriesForMinistry"], errors="coerce"
                ).fillna(0)

                df["ReplyPercentage"] = (
                    (df["AnsweredQueriesForMinistry"] / df["TotalQueriesForMinistry"].replace(0, pd.NA)) * 100
                ).round(1)
                df["ReplyPercentageText"] = df["ReplyPercentage"].apply(
                    lambda x: f"{x}% replied" if pd.notna(x) else "N/A replied"
                )

                # Get ministry order for consistent sorting
                df_annotations = df.drop_duplicates(subset=["MinistryName"]).sort_values(
                    by="TotalQueriesForMinistry", ascending=False
                )

                # Import color config
                from config.charts import ChartConfig

                fig = px.bar(
                    df,
                    x="MinistryName",
                    y="QueryCount",
                    color="AnswerStatus",
                    title=f"<b>Query Distribution and Reply Rate by Ministry (Knesset {single_knesset_num})</b>",
                    labels={
                        "MinistryName": "Ministry",
                        "QueryCount": "Number of Queries",
                        "AnswerStatus": "Query Outcome",
                    },
                    color_discrete_map=ChartConfig.ANSWER_STATUS_COLORS,
                    category_orders={
                        "AnswerStatus": ["Answered", "Not Answered", "Other/In Progress", "Unknown"],
                        "MinistryName": df_annotations["MinistryName"].tolist(),
                    },
                )

                # Update hover for each trace separately to show reply rate only for Answered
                for trace in fig.data:
                    trace_df = df[df["AnswerStatus"] == trace.name]
                    if trace.name == "Answered":
                        trace.customdata = trace_df[["TotalQueriesForMinistry", "ReplyPercentage"]].values
                        trace.hovertemplate = (
                            "<b>Ministry:</b> %{x}<br>"
                            + "<b>Status:</b> %{fullData.name}<br>"
                            + "<b>Answered:</b> %{y}<br>"
                            + "<b>Total Queries:</b> %{customdata[0]}<br>"
                            + "<b>Reply Rate:</b> %{customdata[1]:.1f}%<extra></extra>"
                        )
                    else:
                        trace.customdata = trace_df[["TotalQueriesForMinistry"]].values
                        trace.hovertemplate = (
                            "<b>Ministry:</b> %{x}<br>"
                            + "<b>Status:</b> %{fullData.name}<br>"
                            + "<b>Count:</b> %{y}<br>"
                            + "<b>Total Queries:</b> %{customdata[0]}<extra></extra>"
                        )

                fig.update_layout(
                    xaxis_title="Ministry",
                    yaxis_title="Number of Queries",
                    legend_title_text="Query Outcome",
                    title_x=0.5,
                    xaxis_tickangle=-45,
                    height=800,
                    margin=dict(t=180),
                )

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating 'plot_queries_by_ministry': {e}", exc_info=True
            )
            st.error(f"Could not generate 'Query Performance by Ministry' plot: {e}")
            return None

    def plot_query_status_by_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate query status by faction as a stacked bar chart (similar to ministry chart)."""
        if not self.check_database_exists():
            return None

        if not knesset_filter or len(knesset_filter) != 1:
            st.info(
                "Please select a single Knesset to view the 'Query Status by Faction' plot."
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

                # Use same status categorization as the ministry chart
                answer_status_case_sql = """
                    CASE
                        WHEN s.Desc LIKE '%נענתה%' AND s.Desc NOT LIKE '%לא נענתה%' THEN 'Answered'
                        WHEN s.Desc LIKE '%לא נענתה%' THEN 'Not Answered'
                        WHEN s.Desc LIKE '%הועברה%' THEN 'Other/In Progress'
                        WHEN s.Desc LIKE '%בטיפול%' THEN 'Other/In Progress'
                        WHEN s.Desc LIKE '%נדחתה%' THEN 'Not Answered'
                        WHEN s.Desc LIKE '%הוסרה%' THEN 'Other/In Progress'
                        WHEN s.Desc LIKE '%נקבע תאריך%' THEN 'Other/In Progress'
                        ELSE 'Unknown'
                    END AS AnswerStatus
                """

                sql_query = f"""
                WITH FactionQueryStats AS (
                    SELECT
                        COALESCE(f.Name, 'Unknown Faction') AS FactionName,
                        p2p.FactionID,
                        {answer_status_case_sql},
                        COUNT(DISTINCT q.QueryID) AS QueryCount
                    FROM KNS_Query q
                    JOIN KNS_Person p ON q.PersonID = p.PersonID
                    LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID
                    LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                        AND q.KnessetNum = p2p.KnessetNum
                        AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    LEFT JOIN KNS_Faction f ON p2p.FactionID = f.FactionID
                    WHERE {where_clause}
                        AND f.Name IS NOT NULL
                    GROUP BY f.Name, p2p.FactionID, AnswerStatus
                )
                SELECT
                    FactionName,
                    FactionID,
                    AnswerStatus,
                    QueryCount,
                    SUM(QueryCount) OVER (PARTITION BY FactionName) AS TotalQueriesForFaction,
                    SUM(CASE WHEN AnswerStatus = 'Answered' THEN QueryCount ELSE 0 END) OVER (PARTITION BY FactionName) AS AnsweredQueriesForFaction
                FROM FactionQueryStats
                ORDER BY TotalQueriesForFaction DESC, FactionName,
                    CASE AnswerStatus
                        WHEN 'Answered' THEN 1
                        WHEN 'Not Answered' THEN 2
                        WHEN 'Other/In Progress' THEN 3
                        ELSE 4
                    END
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

                result = safe_execute_query(con, sql_query, self.logger, params=params)

                if result is None or result.empty:
                    st.info(
                        f"No query data for Knesset {single_knesset_num} to visualize 'Query Status by Faction' with the current filters."
                    )
                    self.logger.info(
                        "No data for 'Query Status by Faction' plot (Knesset %s).",
                        single_knesset_num,
                    )
                    return None

                df = result.copy()

                # Convert to numeric and calculate percentages
                df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors="coerce").fillna(0)
                df["TotalQueriesForFaction"] = pd.to_numeric(
                    df["TotalQueriesForFaction"], errors="coerce"
                ).fillna(0)
                df["AnsweredQueriesForFaction"] = pd.to_numeric(
                    df["AnsweredQueriesForFaction"], errors="coerce"
                ).fillna(0)

                df["ReplyPercentage"] = (
                    (df["AnsweredQueriesForFaction"] / df["TotalQueriesForFaction"].replace(0, pd.NA)) * 100
                ).round(1)

                # Get faction order for consistent sorting (by total queries)
                df_annotations = df.drop_duplicates(subset=["FactionName"]).sort_values(
                    by="TotalQueriesForFaction", ascending=False
                )

                # Import color config
                from config.charts import ChartConfig

                # Build title with optional date range
                title = f"<b>Query Status by Faction (Knesset {single_knesset_num})</b>"
                if start_date or end_date:
                    if start_date and end_date:
                        date_range_text = f" ({start_date} to {end_date})"
                    elif start_date:
                        date_range_text = f" (from {start_date})"
                    else:
                        date_range_text = f" (until {end_date})"
                    title = f"<b>Query Status by Faction (Knesset {single_knesset_num}){date_range_text}</b>"

                fig = px.bar(
                    df,
                    x="FactionName",
                    y="QueryCount",
                    color="AnswerStatus",
                    title=title,
                    labels={
                        "FactionName": "Faction",
                        "QueryCount": "Number of Queries",
                        "AnswerStatus": "Query Status",
                    },
                    color_discrete_map=ChartConfig.ANSWER_STATUS_COLORS,
                    category_orders={
                        "AnswerStatus": ["Answered", "Not Answered", "Other/In Progress", "Unknown"],
                        "FactionName": df_annotations["FactionName"].tolist(),
                    },
                )

                # Update hover for each trace separately to show reply rate only for Answered
                for trace in fig.data:
                    trace_df = df[df["AnswerStatus"] == trace.name]
                    if trace.name == "Answered":
                        trace.customdata = trace_df[["TotalQueriesForFaction", "ReplyPercentage"]].values
                        trace.hovertemplate = (
                            "<b>Faction:</b> %{x}<br>"
                            + "<b>Status:</b> %{fullData.name}<br>"
                            + "<b>Answered:</b> %{y}<br>"
                            + "<b>Total Queries:</b> %{customdata[0]}<br>"
                            + "<b>Reply Rate:</b> %{customdata[1]:.1f}%<extra></extra>"
                        )
                    else:
                        trace.customdata = trace_df[["TotalQueriesForFaction"]].values
                        trace.hovertemplate = (
                            "<b>Faction:</b> %{x}<br>"
                            + "<b>Status:</b> %{fullData.name}<br>"
                            + "<b>Count:</b> %{y}<br>"
                            + "<b>Total Queries:</b> %{customdata[0]}<extra></extra>"
                        )

                fig.update_layout(
                    xaxis_title="Faction",
                    yaxis_title="Number of Queries",
                    legend_title_text="Query Status",
                    title_x=0.5,
                    xaxis_tickangle=-45,
                    height=800,
                    margin=dict(t=180),
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
        """Generate the requested query comparison chart.

        Args:
            chart_type: Type of chart to generate
            **kwargs: Chart-specific arguments

        Returns:
            Plotly Figure object or None if unknown chart type
        """
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
            self.logger.warning(f"Unknown query chart type: {chart_type}")
            return None
