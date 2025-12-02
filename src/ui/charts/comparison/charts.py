"""Comparison and faction analysis chart generators."""

import logging
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from utils.faction_resolver import FactionResolver, get_faction_name_field, get_coalition_status_field

from ..base import BaseChart


class ComparisonCharts(BaseChart):
    """Comparison charts for factions, ministries, etc."""

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
                WITH BillFirstSubmission AS (
                    -- Get the earliest activity date for each bill (true submission date)
                    SELECT
                        B.BillID,
                        MIN(earliest_date) as FirstSubmissionDate
                    FROM KNS_Bill B
                    LEFT JOIN (
                        -- Initiator assignment dates
                        SELECT BI.BillID, MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_BillInitiator BI WHERE BI.LastUpdatedDate IS NOT NULL GROUP BY BI.BillID
                        UNION ALL
                        -- Committee session dates
                        SELECT csi.ItemID as BillID, MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_CmtSessionItem csi JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                        WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL GROUP BY csi.ItemID
                        UNION ALL
                        -- Plenum session dates
                        SELECT psi.ItemID as BillID, MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_PlmSessionItem psi JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                        WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL GROUP BY psi.ItemID
                        UNION ALL
                        -- Publication dates
                        SELECT B.BillID, CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
                        FROM KNS_Bill B WHERE B.PublicationDate IS NOT NULL
                    ) all_dates ON B.BillID = all_dates.BillID
                    WHERE all_dates.earliest_date IS NOT NULL
                    GROUP BY B.BillID
                )
                SELECT
                    COALESCE(p2p.FactionName, f_fallback.Name) AS FactionName,
                    CASE
                        WHEN b.StatusID = 118 THEN 'התקבלה בקריאה שלישית'
                        WHEN b.StatusID IN (104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114) THEN 'קריאה ראשונה'
                        ELSE 'הופסק/לא פעיל'
                    END AS Stage,
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

                # Define stage order and colors
                stage_order = ['הופסק/לא פעיל', 'קריאה ראשונה', 'התקבלה בקריאה שלישית']
                stage_colors = {
                    'הופסק/לא פעיל': '#EF553B',  # Red
                    'קריאה ראשונה': '#636EFA',    # Blue
                    'התקבלה בקריאה שלישית': '#00CC96'  # Green
                }

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
                WITH BillFirstSubmission AS (
                    -- Get the earliest activity date for each bill (true submission date)
                    SELECT
                        B.BillID,
                        MIN(earliest_date) as FirstSubmissionDate
                    FROM KNS_Bill B
                    LEFT JOIN (
                        SELECT BI.BillID, MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_BillInitiator BI WHERE BI.LastUpdatedDate IS NOT NULL GROUP BY BI.BillID
                        UNION ALL
                        SELECT csi.ItemID as BillID, MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_CmtSessionItem csi JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                        WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL GROUP BY csi.ItemID
                        UNION ALL
                        SELECT psi.ItemID as BillID, MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_PlmSessionItem psi JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                        WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL GROUP BY psi.ItemID
                        UNION ALL
                        SELECT B.BillID, CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
                        FROM KNS_Bill B WHERE B.PublicationDate IS NOT NULL
                    ) all_dates ON B.BillID = all_dates.BillID
                    WHERE all_dates.earliest_date IS NOT NULL
                    GROUP BY B.BillID
                )
                SELECT
                    ufs.CoalitionStatus AS CoalitionStatus,
                    CASE
                        WHEN b.StatusID = 118 THEN 'התקבלה בקריאה שלישית'
                        WHEN b.StatusID IN (104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114) THEN 'קריאה ראשונה'
                        ELSE 'הופסק/לא פעיל'
                    END AS Stage,
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

                # Define stage order and colors
                stage_order = ['הופסק/לא פעיל', 'קריאה ראשונה', 'התקבלה בקריאה שלישית']
                stage_colors = {
                    'הופסק/לא פעיל': '#EF553B',  # Red
                    'קריאה ראשונה': '#636EFA',    # Blue
                    'התקבלה בקריאה שלישית': '#00CC96'  # Green
                }

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
                    query = """
                    WITH BillFirstSubmission AS (
                        -- Get the earliest activity date for each bill (true submission date)
                        SELECT
                            B.BillID,
                            MIN(earliest_date) as FirstSubmissionDate
                        FROM KNS_Bill B
                        LEFT JOIN (
                            -- Initiator assignment dates
                            SELECT BI.BillID, MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
                            FROM KNS_BillInitiator BI WHERE BI.LastUpdatedDate IS NOT NULL GROUP BY BI.BillID
                            UNION ALL
                            -- Committee session dates
                            SELECT csi.ItemID as BillID, MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
                            FROM KNS_CmtSessionItem csi JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                            WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL GROUP BY csi.ItemID
                            UNION ALL
                            -- Plenum session dates
                            SELECT psi.ItemID as BillID, MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
                            FROM KNS_PlmSessionItem psi JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                            WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL GROUP BY psi.ItemID
                            UNION ALL
                            -- Publication dates
                            SELECT B.BillID, CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
                            FROM KNS_Bill B WHERE B.PublicationDate IS NOT NULL
                        ) all_dates ON B.BillID = all_dates.BillID
                        WHERE all_dates.earliest_date IS NOT NULL
                        GROUP BY B.BillID
                    )
                    SELECT
                        p.FirstName || ' ' || p.LastName AS MKName,
                        p.PersonID,
                        CASE
                            WHEN b.StatusID = 118 THEN 'התקבלה בקריאה שלישית'
                            WHEN b.StatusID IN (104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114) THEN 'קריאה ראשונה'
                            ELSE 'הופסק/לא פעיל'
                        END AS Stage,
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
                    query = """
                    WITH BillFirstSubmission AS (
                        -- Get the earliest activity date for each bill (true submission date)
                        SELECT
                            B.BillID,
                            MIN(earliest_date) as FirstSubmissionDate
                        FROM KNS_Bill B
                        LEFT JOIN (
                            -- Initiator assignment dates
                            SELECT BI.BillID, MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
                            FROM KNS_BillInitiator BI WHERE BI.LastUpdatedDate IS NOT NULL GROUP BY BI.BillID
                            UNION ALL
                            -- Committee session dates
                            SELECT csi.ItemID as BillID, MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
                            FROM KNS_CmtSessionItem csi JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                            WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL GROUP BY csi.ItemID
                            UNION ALL
                            -- Plenum session dates
                            SELECT psi.ItemID as BillID, MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
                            FROM KNS_PlmSessionItem psi JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                            WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL GROUP BY psi.ItemID
                            UNION ALL
                            -- Publication dates
                            SELECT B.BillID, CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
                            FROM KNS_Bill B WHERE B.PublicationDate IS NOT NULL
                        ) all_dates ON B.BillID = all_dates.BillID
                        WHERE all_dates.earliest_date IS NOT NULL
                        GROUP BY B.BillID
                    )
                    SELECT
                        p.FirstName || ' ' || p.LastName AS MKName,
                        p.PersonID,
                        CASE
                            WHEN b.StatusID = 118 THEN 'התקבלה בקריאה שלישית'
                            WHEN b.StatusID IN (104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114) THEN 'קריאה ראשונה'
                            ELSE 'הופסק/לא פעיל'
                        END AS Stage,
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

                # Define stage order and colors
                stage_order = ['הופסק/לא פעיל', 'קריאה ראשונה', 'התקבלה בקריאה שלישית']
                stage_colors = {
                    'הופסק/לא פעיל': '#EF553B',  # Red
                    'קריאה ראשונה': '#636EFA',    # Blue
                    'התקבלה בקריאה שלישית': '#00CC96'  # Green
                }

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

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested comparison chart."""
        chart_methods = {
            "queries_per_faction": self.plot_queries_per_faction,
            "queries_by_coalition_status": self.plot_queries_by_coalition_status,
            "queries_by_ministry": self.plot_queries_by_ministry,
            "query_status_by_faction": self.plot_query_status_by_faction,
            "agendas_per_faction": self.plot_agendas_per_faction,
            "agendas_by_coalition_status": self.plot_agendas_by_coalition_status,
            "bills_per_faction": self.plot_bills_per_faction,
            "bills_by_coalition_status": self.plot_bills_by_coalition_status,
            "top_bill_initiators": self.plot_top_bill_initiators,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown comparison chart type: {chart_type}")
            return None
