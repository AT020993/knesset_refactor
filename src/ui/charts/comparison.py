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

from .base import BaseChart


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

                fig = px.bar(
                    df,
                    x="FactionName",
                    y="AgendaCount",
                    color="FactionName",
                    title=f"<b>Agendas per Initiating Faction (Knesset {single_knesset_num})</b>",
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

                date_column = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"

                query = f"""
                SELECT
                    ufs.CoalitionStatus AS CoalitionStatus,
                    COUNT(DISTINCT a.AgendaID) AS AgendaCount
                FROM KNS_Agenda a
                JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
                LEFT JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
                    AND a.KnessetNum = p2p.KnessetNum
                    AND CAST({date_column} AS TIMESTAMP)
                        BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                        AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID
                    AND a.KnessetNum = ufs.KnessetNum
                WHERE a.KnessetNum = ? AND a.InitiatorPersonID IS NOT NULL
                    AND ufs.CoalitionStatus IS NOT NULL
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
                GROUP BY CoalitionStatus
                HAVING AgendaCount > 0
                ORDER BY AgendaCount DESC;
                """

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

                fig = px.pie(
                    df,
                    values="AgendaCount",
                    names="CoalitionStatus",
                    title=f"<b>Agendas by Initiator Coalition Status (Knesset {single_knesset_num})</b>",
                    color="CoalitionStatus",
                    color_discrete_map=self.config.COALITION_OPPOSITION_COLORS,
                )

                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(title_x=0.5)

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
                        COALESCE(p2p.FactionName, f_fallback.Name) AS FactionName,
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
                WHERE qsfi.FactionName IS NOT NULL
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

                fig = px.bar(
                    df,
                    x="FactionName",
                    y="BillCount",
                    color="Stage",
                    title=f"<b>Bills per Initiating Faction by Status (Knesset {single_knesset_num})</b>",
                    labels={
                        "FactionName": "Faction",
                        "BillCount": "Number of Bills",
                        "Stage": "Bill Status"
                    },
                    category_orders={
                        "FactionName": faction_order,
                        "Stage": stage_order
                    },
                    color_discrete_map=stage_colors,
                    barmode='stack'
                )

                fig.update_traces(
                    hovertemplate="<b>Faction:</b> %{x}<br><b>Status:</b> %{fullData.name}<br><b>Bills:</b> %{y}<extra></extra>"
                )

                fig.update_layout(
                    xaxis_title="Initiating Faction",
                    yaxis_title="Number of Bills",
                    title_x=0.5,
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
                    margin=dict(t=180)
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

                fig = px.bar(
                    df,
                    x="CoalitionStatus",
                    y="BillCount",
                    color="Stage",
                    title=f"<b>Bills by Coalition Status and Bill Stage (Knesset {single_knesset_num})</b>",
                    labels={
                        "CoalitionStatus": "Coalition Status",
                        "BillCount": "Number of Bills",
                        "Stage": "Bill Status"
                    },
                    category_orders={
                        "Stage": stage_order
                    },
                    color_discrete_map=stage_colors,
                    barmode='stack'
                )

                fig.update_traces(
                    hovertemplate="<b>Coalition:</b> %{x}<br><b>Status:</b> %{fullData.name}<br><b>Bills:</b> %{y}<extra></extra>"
                )

                fig.update_layout(
                    xaxis_title="Coalition Status",
                    yaxis_title="Number of Bills",
                    title_x=0.5,
                    showlegend=True,
                    legend_title_text='Bill Status',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
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
                            query += f" AND pf.FactionID IN ({placeholders})"
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
                            query += f" AND pf.FactionID IN ({placeholders})"
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

                fig = px.bar(
                    df,
                    x="MKName",
                    y="BillCount",
                    color="Stage",
                    title=f"<b>Top 10 Bill Initiators by Status ({knesset_title})</b>",
                    labels={
                        "MKName": "Knesset Member",
                        "BillCount": "Number of Bills",
                        "Stage": "Bill Status"
                    },
                    category_orders={
                        "MKName": mk_order,
                        "Stage": stage_order
                    },
                    color_discrete_map=stage_colors,
                    barmode='stack'
                )

                fig.update_traces(
                    hovertemplate="<b>MK:</b> %{x}<br><b>Status:</b> %{fullData.name}<br><b>Bills:</b> %{y}<extra></extra>"
                )

                fig.update_layout(
                    xaxis_title="Knesset Member",
                    yaxis_title="Number of Bills",
                    title_x=0.5,
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
                    margin=dict(t=180)
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

    def plot_bill_initiators_by_faction(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate chart showing count of MKs who were main bill initiators by faction."""
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
                    ),
                    PersonFactions AS (
                        -- Match each bill to the best faction based on date, prioritizing non-NULL FactionIDs
                        SELECT
                            bi.PersonID,
                            b.BillID,
                            ptp.FactionID,
                            ROW_NUMBER() OVER (
                                PARTITION BY b.BillID, bi.PersonID
                                ORDER BY CASE WHEN ptp.FactionID IS NULL THEN 1 ELSE 0 END,
                                         ptp.StartDate DESC
                            ) as rn
                        FROM KNS_Bill b
                        LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                        JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                        LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
                            AND b.KnessetNum = ptp.KnessetNum
                            AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                                BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
                                AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
                        WHERE b.KnessetNum = ?
                            AND bi.Ordinal = 1
                            AND bi.PersonID IS NOT NULL
                    )
                    SELECT
                        COALESCE(f.Name, 'Data Unavailable') AS FactionName,
                        COUNT(DISTINCT p.PersonID) AS MKCount
                    FROM KNS_Bill b
                    JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                    JOIN KNS_Person p ON bi.PersonID = p.PersonID
                    LEFT JOIN PersonFactions pf ON b.BillID = pf.BillID
                        AND bi.PersonID = pf.PersonID
                        AND pf.rn = 1
                    LEFT JOIN KNS_Faction f ON pf.FactionID = f.FactionID
                    WHERE bi.Ordinal = 1
                        AND bi.PersonID IS NOT NULL
                        AND b.KnessetNum = ?
                    """
                    params.append(knesset_filter[0])  # First ? in PersonFactions CTE
                    params.append(knesset_filter[0])  # Second ? in main SELECT
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
                            query += f" AND pf.FactionID IN ({placeholders})"
                            params.extend(valid_ids)

                    query += """
                    GROUP BY f.Name
                    ORDER BY MKCount DESC;
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
                    ),
                    PersonFactions AS (
                        -- Match each bill to the best faction based on date, prioritizing non-NULL FactionIDs
                        SELECT
                            bi.PersonID,
                            b.BillID,
                            b.KnessetNum,
                            ptp.FactionID,
                            ROW_NUMBER() OVER (
                                PARTITION BY b.BillID, bi.PersonID
                                ORDER BY CASE WHEN ptp.FactionID IS NULL THEN 1 ELSE 0 END,
                                         ptp.StartDate DESC
                            ) as rn
                        FROM KNS_Bill b
                        LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                        JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                        LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
                            AND b.KnessetNum = ptp.KnessetNum
                            AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                                BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
                                AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
                        WHERE bi.Ordinal = 1
                            AND bi.PersonID IS NOT NULL
                    )
                    SELECT
                        COALESCE(f.Name, 'Data Unavailable') AS FactionName,
                        COUNT(DISTINCT CONCAT(p.PersonID, '-', b.KnessetNum)) AS MKCount,
                        b.KnessetNum
                    FROM KNS_Bill b
                    JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                    JOIN KNS_Person p ON bi.PersonID = p.PersonID
                    LEFT JOIN PersonFactions pf ON b.BillID = pf.BillID
                        AND bi.PersonID = pf.PersonID
                        AND pf.rn = 1
                    LEFT JOIN KNS_Faction f ON pf.FactionID = f.FactionID
                    WHERE bi.Ordinal = 1
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
                            query += f" AND pf.FactionID IN ({placeholders})"
                            params.extend(valid_ids)

                    query += """
                    GROUP BY f.Name, b.KnessetNum
                    ORDER BY MKCount DESC;
                    """

                self.logger.debug(
                    "Executing SQL for plot_bill_initiators_by_faction: %s",
                    query,
                )
                df = safe_execute_query(con, query, self.logger, params=params)

                if df.empty:
                    st.info(
                        f"No bill initiator data found for {knesset_title} with the current filters."
                    )
                    return None

                df["MKCount"] = pd.to_numeric(
                    df["MKCount"], errors="coerce"
                ).fillna(0)

                # For multiple Knessets, aggregate by faction
                if not (knesset_filter and len(knesset_filter) == 1):
                    df = df.groupby(["FactionName"]).agg({
                        "MKCount": "sum"
                    }).reset_index().sort_values("MKCount", ascending=False)
                else:
                    # For single Knesset, ensure proper sorting
                    df = df.sort_values("MKCount", ascending=False)

                # Create bar chart
                fig = go.Figure()
                
                fig.add_trace(
                    go.Bar(
                        x=df["FactionName"],
                        y=df["MKCount"],
                        name="MK Count",
                        marker=dict(
                            color=self.config.KNESSET_COLOR_SEQUENCE[0], 
                            line=dict(color="black", width=1)
                        ),
                        text=df["MKCount"],
                        textposition="outside",
                        hovertemplate="<b>%{x}</b><br>MKs with Bills: %{y}<extra></extra>",
                    )
                )

                # Update layout
                fig.update_layout(
                    title=f"<b>Bill Main Initiators by Faction ({knesset_title})</b>",
                    xaxis_title="<b>Faction</b>",
                    yaxis_title="<b>Number of MKs Who Initiated Bills</b>",
                    showlegend=False,
                    height=800,
                    margin=dict(t=180, b=150, l=80, r=50),
                    font=dict(size=12),
                    title_font=dict(size=16),
                    xaxis=dict(
                        tickangle=-45,
                        categoryorder="array",
                        categoryarray=df["FactionName"].tolist(),
                    ),
                    yaxis=dict(gridcolor="lightgray"),
                    plot_bgcolor="white",
                )

                return fig

        except Exception as e:
            self.logger.error(f"Error in plot_bill_initiators_by_faction: {e}", exc_info=True)
            st.error(f"Could not generate 'Bill Initiators by Faction' plot: {e}")
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
            "bill_initiators_by_faction": self.plot_bill_initiators_by_faction,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown comparison chart type: {chart_type}")
            return None
