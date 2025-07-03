"""Distribution and categorical chart generators."""

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query

from .base import BaseChart


class DistributionCharts(BaseChart):
    """Distribution analysis charts (pie, histogram, etc.)."""

    def plot_query_types_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate query types distribution chart with optional date range."""
        if not self.check_database_exists():
            return None

        if start_date and end_date and start_date > end_date:
            st.error("Start date must be before or equal to end date.")
            self.logger.error(
                "Invalid date range: start_date=%s, end_date=%s",
                start_date,
                end_date,
            )
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q", 
                                     start_date=start_date, end_date=end_date, **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Query"]):
                    return None

                # Add JOIN with KNS_Status table if status filters are used
                status_join = ""
                if filters['query_status_condition'] != "1=1":
                    status_join = "LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID"
                
                query = f"""
                    SELECT
                        COALESCE(q.TypeDesc, 'Unknown') AS QueryType,
                        COUNT(q.QueryID) AS Count
                    FROM KNS_Query q
                    {status_join}
                    WHERE q.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                        AND {filters["query_type_condition"]}
                        AND {filters["query_status_condition"]}
                        AND {filters["start_date_condition"]}
                        AND {filters["end_date_condition"]}
                    GROUP BY q.TypeDesc
                    ORDER BY Count DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No query type data found for '{filters['knesset_title']}'."
                    )
                    return None

                date_range_text = ""
                if start_date or end_date:
                    if start_date and end_date:
                        date_range_text = f" ({start_date} to {end_date})"
                    elif start_date:
                        date_range_text = f" (from {start_date})"
                    elif end_date:
                        date_range_text = f" (until {end_date})"

                fig = px.pie(
                    df,
                    values="Count",
                    names="QueryType",
                    title=(
                        f"<b>Query Types Distribution for {filters['knesset_title']}{date_range_text}</b>"
                    ),
                )

                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(title_x=0.5)

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating query types distribution chart: {e}", exc_info=True
            )
            st.error(f"Could not generate query types chart: {e}")
            return None

    def plot_agenda_classifications_pie(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda classifications pie chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a")

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Agenda"]):
                    return None

                query = f"""
                    SELECT
                        COALESCE(a.ClassificationDesc, 'Unknown') AS Classification,
                        COUNT(a.AgendaID) AS Count
                    FROM KNS_Agenda a
                    WHERE a.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY a.ClassificationDesc
                    ORDER BY Count DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No agenda classification data found for '{filters['knesset_title']}'."
                    )
                    return None

                fig = px.pie(
                    df,
                    values="Count",
                    names="Classification",
                    title=f"<b>Agenda Classifications Distribution for {filters['knesset_title']}</b>",
                )

                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(title_x=0.5)

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating agenda classifications pie chart: {e}", exc_info=True
            )
            st.error(f"Could not generate agenda classifications chart: {e}")
            return None

    def plot_query_status_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Generate query status distribution chart."""
        # TODO: Implement from original plot_generators.py
        pass

    def plot_agenda_status_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda status distribution chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a")

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Agenda", "KNS_Status"]):
                    return None

                query = f"""
                    SELECT
                        COALESCE(s."Desc", 'Unknown') AS Status,
                        COUNT(a.AgendaID) AS Count
                    FROM KNS_Agenda a
                    LEFT JOIN KNS_Status s ON a.StatusID = s.StatusID
                    WHERE a.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY s."Desc"
                    ORDER BY Count DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No agenda status data found for '{filters['knesset_title']}'."
                    )
                    return None

                fig = px.pie(
                    df,
                    values="Count",
                    names="Status",
                    title=f"<b>Agenda Status Distribution for {filters['knesset_title']}</b>",
                )

                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(title_x=0.5)

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating agenda status distribution chart: {e}", exc_info=True
            )
            st.error(f"Could not generate agenda status chart: {e}")
            return None

    def plot_bill_status_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate bill status distribution chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b")

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_Status"]):
                    return None

                query = f"""
                    SELECT
                        COALESCE(s."Desc", 'Unknown') AS Status,
                        COUNT(b.BillID) AS Count
                    FROM KNS_Bill b
                    LEFT JOIN KNS_Status s ON b.StatusID = s.StatusID
                    WHERE b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY s."Desc"
                    ORDER BY Count DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No bill status data found for '{filters['knesset_title']}'."
                    )
                    return None

                fig = px.pie(
                    df,
                    values="Count",
                    names="Status",
                    title=f"<b>Bill Status Distribution for {filters['knesset_title']}</b>",
                )

                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(title_x=0.5)

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating bill status distribution chart: {e}", exc_info=True
            )
            st.error(f"Could not generate bill status chart: {e}")
            return None

    def plot_query_status_by_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate a sunburst chart of query status with faction breakdown."""

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

                date_conditions = []
                if start_date:
                    date_conditions.append(
                        f"CAST(q.SubmitDate AS DATE) >= '{start_date}'"
                    )
                if end_date:
                    date_conditions.append(
                        f"CAST(q.SubmitDate AS DATE) <= '{end_date}'"
                    )

                date_filter_sql = " AND ".join(date_conditions)
                if date_filter_sql:
                    date_filter_sql = f" AND {date_filter_sql}"

                params = []
                faction_filter_sql = ""
                if faction_filter:
                    placeholders = ", ".join(["?"] * len(faction_filter))
                    faction_filter_sql = f" AND COALESCE(p2p.FactionName, f_fallback.Name) IN ({placeholders})"
                    params.extend(faction_filter)

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
                        AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID
                        AND q.KnessetNum = f_fallback.KnessetNum
                    WHERE q.KnessetNum = {single_knesset_num} AND q.SubmitDate IS NOT NULL
                        AND p2p.FactionID IS NOT NULL
                        {faction_filter_sql}{date_filter_sql}
                )
                SELECT
                    qsfi.StatusDescription,
                    qsfi.FactionName,
                    COUNT(DISTINCT qsfi.QueryID) AS QueryCount
                FROM QueryStatusFactionInfo qsfi
                GROUP BY qsfi.StatusDescription, qsfi.FactionName
                HAVING QueryCount > 0
                ORDER BY qsfi.StatusDescription, QueryCount DESC;
                """

                self.logger.debug(
                    "Executing SQL for plot_query_status_by_faction: %s",
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
                    date_range_text = ""
                    if start_date and end_date:
                        date_range_text = f" ({start_date} to {end_date})"
                    elif start_date:
                        date_range_text = f" (from {start_date})"
                    elif end_date:
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

    def plot_bill_subtype_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate bill subtype distribution chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b")

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill"]):
                    return None

                query = f"""
                    SELECT
                        COALESCE(b.SubTypeDesc, 'Unknown') AS SubType,
                        COUNT(b.BillID) AS Count
                    FROM KNS_Bill b
                    WHERE b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY b.SubTypeDesc
                    ORDER BY Count DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No bill subtype data found for '{filters['knesset_title']}'."
                    )
                    return None

                fig = px.pie(
                    df,
                    values="Count",
                    names="SubType",
                    title=f"<b>Bill SubType Distribution for {filters['knesset_title']}</b>",
                )

                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(title_x=0.5)

                return fig

        except Exception as e:
            self.logger.error(
                f"Error generating bill subtype distribution chart: {e}", exc_info=True
            )
            st.error(f"Could not generate bill subtype chart: {e}")
            return None

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested distribution chart."""
        chart_methods = {
            "query_types_distribution": self.plot_query_types_distribution,
            "agenda_classifications_pie": self.plot_agenda_classifications_pie,
            "query_status_distribution": self.plot_query_status_distribution,
            "query_status_by_faction": self.plot_query_status_by_faction,
            "agenda_status_distribution": self.plot_agenda_status_distribution,
            "bill_status_distribution": self.plot_bill_status_distribution,
            "bill_subtype_distribution": self.plot_bill_subtype_distribution,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown distribution chart type: {chart_type}")
            return None
