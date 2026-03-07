"""Distribution and categorical chart generators."""

from typing import Any, Callable, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from backend.connection_manager import get_db_connection, safe_execute_query
from ui.queries.sql_templates import SQLTemplates

from .base import BaseChart, chart_error_handler


class DistributionCharts(BaseChart):
    """Distribution analysis charts (pie, histogram, etc.)."""

    @chart_error_handler("query types distribution")
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
            self.show_error("Start date must be before or equal to end date.")
            self.logger.error("Invalid date range: start_date=%s, end_date=%s", start_date, end_date)
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q",
                                     start_date=start_date, end_date=end_date, **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            if not self.check_tables_exist(con, ["KNS_Query"]):
                return None

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

            if self.handle_empty_result(df, "query type", filters):
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
                df, values="Count", names="QueryType",
                title=f"<b>Query Types Distribution for {filters['knesset_title']}{date_range_text}</b>",
            )

            return self.apply_pie_chart_defaults(fig)

    @chart_error_handler("agenda classifications")
    def plot_agenda_classifications_pie(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda classifications pie chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a", **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
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

            if self.handle_empty_result(df, "agenda classification", filters):
                return None

            fig = px.pie(
                df, values="Count", names="Classification",
                title=f"<b>Agenda Classifications Distribution for {filters['knesset_title']}</b>",
            )

            return self.apply_pie_chart_defaults(fig)

    def plot_query_status_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Generate query status distribution chart."""
        if not self.check_database_exists():
            return None

        knesset_filter = kwargs.get("knesset_filter")
        faction_filter = kwargs.get("faction_filter")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        query_type_filter = kwargs.get("query_type_filter", [])
        query_status_filter = kwargs.get("query_status_filter", [])

        if start_date and end_date and start_date > end_date:
            self.show_error("Start date must be before or equal to end date.")
            self.logger.error("Invalid date range: start_date=%s, end_date=%s", start_date, end_date)
            return None

        if knesset_filter:
            knesset_title = f"Knesset {knesset_filter[0]}" if len(knesset_filter) == 1 else f"Knessets: {', '.join(map(str, knesset_filter))}"
        else:
            knesset_title = "All Knessets"

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                required_tables = [
                    "KNS_Query",
                    "KNS_Status",
                    "KNS_PersonToPosition",
                ]
                if not self.check_tables_exist(con, required_tables):
                    return None

                params: list[Any] = []
                conditions: list[str] = [
                    "q.KnessetNum IS NOT NULL",
                    "q.SubmitDate IS NOT NULL",
                ]

                if knesset_filter:
                    placeholders = ", ".join("?" for _ in knesset_filter)
                    conditions.append(f"q.KnessetNum IN ({placeholders})")
                    params.extend(int(k) for k in knesset_filter)

                valid_faction_ids = [
                    int(fid)
                    for fid in faction_filter or []
                    if str(fid).isdigit()
                ]
                if valid_faction_ids:
                    placeholders = ", ".join("?" for _ in valid_faction_ids)
                    conditions.append(f"p2p.FactionID IN ({placeholders})")
                    params.extend(valid_faction_ids)

                if start_date:
                    conditions.append("CAST(q.SubmitDate AS DATE) >= ?")
                    params.append(start_date)
                if end_date:
                    conditions.append("CAST(q.SubmitDate AS DATE) <= ?")
                    params.append(end_date)

                if query_type_filter:
                    placeholders = ", ".join("?" for _ in query_type_filter)
                    conditions.append(f"q.TypeDesc IN ({placeholders})")
                    params.extend(query_type_filter)

                if query_status_filter:
                    placeholders = ", ".join("?" for _ in query_status_filter)
                    conditions.append(f's."Desc" IN ({placeholders})')
                    params.extend(query_status_filter)

                where_clause = " AND ".join(conditions)

                query = f"""
                SELECT
                    COALESCE(s."Desc", 'Unknown') AS Status,
                    COUNT(DISTINCT q.QueryID) AS Count
                FROM KNS_Query q
                LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID
                LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                    AND q.KnessetNum = p2p.KnessetNum
                    AND p2p.FactionID IS NOT NULL
                    AND CAST(q.SubmitDate AS TIMESTAMP)
                        BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                        AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                WHERE {where_clause}
                GROUP BY s."Desc"
                ORDER BY Count DESC
                """

                df = safe_execute_query(con, query, self.logger, params=params)
                if self.handle_empty_result(df, "query status", {"knesset_title": knesset_title}):
                    return None

                date_range_text = ""
                if start_date or end_date:
                    if start_date and end_date:
                        date_range_text = f" ({start_date} to {end_date})"
                    elif start_date:
                        date_range_text = f" (from {start_date})"
                    else:
                        date_range_text = f" (until {end_date})"

                fig = px.pie(
                    df,
                    values="Count",
                    names="Status",
                    title=f"<b>Query Status Distribution for {knesset_title}{date_range_text}</b>",
                    color="Status",
                    color_discrete_map=self.config.GENERAL_STATUS_COLORS,
                )
                return self.apply_pie_chart_defaults(fig)
        except Exception as e:
            self.logger.error("Error generating query status distribution: %s", e, exc_info=True)
            self.show_error(f"Could not generate query status distribution: {e}")
            return None

    @chart_error_handler("agenda status distribution")
    def plot_agenda_status_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate agenda status distribution chart."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a", **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
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

            if self.handle_empty_result(df, "agenda status", filters):
                return None

            fig = px.pie(
                df, values="Count", names="Status",
                title=f"<b>Agenda Status Distribution for {filters['knesset_title']}</b>",
            )

            return self.apply_pie_chart_defaults(fig)

    @chart_error_handler("bill subtype distribution")
    def plot_bill_subtype_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate bill subtype distribution chart with status breakdown."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            if not self.check_tables_exist(con, ["KNS_Bill"]):
                return None

            query = f"""
                SELECT
                    COALESCE(b.SubTypeDesc, 'Unknown') AS SubType,
                    {SQLTemplates.BILL_STATUS_CASE_HE} AS Stage,
                    COUNT(b.BillID) AS Count
                FROM KNS_Bill b
                WHERE b.KnessetNum IS NOT NULL
                    AND {filters["knesset_condition"]}
                    AND {filters["bill_origin_condition"]}
                GROUP BY b.SubTypeDesc, Stage
                ORDER BY SubType, Stage
            """

            df = safe_execute_query(con, query, self.logger)

            if self.handle_empty_result(df, "bill subtype", filters):
                return None

            # Sort subtypes by total count
            subtype_totals = df.groupby('SubType')['Count'].sum().sort_values(ascending=False)
            subtypes = subtype_totals.index.tolist()

            # Use centralized stage order and colors from SQLTemplates
            stage_order = SQLTemplates.BILL_STAGE_ORDER
            stage_colors = SQLTemplates.BILL_STAGE_COLORS

            fig = go.Figure()

            for stage in stage_order:
                stage_data = df[df['Stage'] == stage].set_index('SubType')
                counts = [stage_data.loc[subtype, 'Count'] if subtype in stage_data.index else 0
                         for subtype in subtypes]

                fig.add_trace(go.Bar(
                    name=stage, x=subtypes, y=counts,
                    marker_color=stage_colors[stage],
                    text=counts, textposition='inside',
                    textfont=dict(color='white', size=12),
                    hovertemplate='<b>%{x}</b><br>' + f'{stage}: %{{y}}<br>' + '<extra></extra>'
                ))

            fig.update_layout(
                barmode='stack',
                title=f"<b>Bill SubType Distribution by Status for {filters['knesset_title']}</b>",
                title_x=0.5,
                xaxis_title="Bill SubType",
                yaxis_title="Number of Bills",
                xaxis_tickangle=-45,
                showlegend=True,
                legend_title_text='Bill Status',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=800, margin=dict(t=180), font_size=12,
                xaxis=dict(automargin=True),
                yaxis=dict(gridcolor="lightgray"),
                plot_bgcolor="white"
            )

            return fig

    @chart_error_handler("major topic distribution")
    def plot_majoril_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate distribution of bills by MajorIL topic with coalition breakdown."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            required = ["KNS_Bill", "UserBillCoding"]
            if not self.check_tables_exist(con, required):
                return None

            query = f"""
                WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                SELECT
                    ubc.MajorIL AS TopicCode,
                    COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
                    COUNT(DISTINCT b.BillID) AS BillCount
                FROM KNS_Bill b
                JOIN UserBillCoding ubc ON b.BillID = ubc.BillID
                LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                LEFT JOIN KNS_PersonToPosition p2p ON bi.PersonID = p2p.PersonID
                    AND b.KnessetNum = p2p.KnessetNum
                    AND p2p.FactionID IS NOT NULL
                    AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                        BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                        AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID
                    AND b.KnessetNum = ufs.KnessetNum
                WHERE ubc.MajorIL IS NOT NULL
                    AND {filters["knesset_condition"]}
                    AND {filters["bill_origin_condition"]}
                GROUP BY ubc.MajorIL, ufs.CoalitionStatus
                ORDER BY TopicCode, CoalitionStatus
            """

            df = safe_execute_query(con, query, self.logger)

            if self.handle_empty_result(df, "major topic", filters):
                return None

            df["BillCount"] = pd.to_numeric(df["BillCount"], errors="coerce").fillna(0)
            df["TopicCode"] = df["TopicCode"].astype(int)

            # Apply MAJORIL text labels
            from utils.majoril_labels import apply_majoril_labels

            df = apply_majoril_labels(df)

            show_percentage = kwargs.get("show_percentage", False)
            return self._build_topic_chart(
                df, filters, show_percentage,
                topic_label="Major Topic (MajorIL)",
                title_prefix="Bills by Major Topic (MajorIL)",
                bar_height=45, min_height=500,
                use_topic_labels=True,
            )

    @chart_error_handler("minor topic distribution")
    def plot_minoril_distribution(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate distribution of bills by MinorIL topic with coalition breakdown."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            required = ["KNS_Bill", "UserBillCoding"]
            if not self.check_tables_exist(con, required):
                return None

            query = f"""
                WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                SELECT
                    ubc.MinorIL AS TopicCode,
                    COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
                    COUNT(DISTINCT b.BillID) AS BillCount
                FROM KNS_Bill b
                JOIN UserBillCoding ubc ON b.BillID = ubc.BillID
                LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                LEFT JOIN KNS_PersonToPosition p2p ON bi.PersonID = p2p.PersonID
                    AND b.KnessetNum = p2p.KnessetNum
                    AND p2p.FactionID IS NOT NULL
                    AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                        BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                        AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID
                    AND b.KnessetNum = ufs.KnessetNum
                WHERE ubc.MinorIL IS NOT NULL
                    AND {filters["knesset_condition"]}
                    AND {filters["bill_origin_condition"]}
                GROUP BY ubc.MinorIL, ufs.CoalitionStatus
                ORDER BY TopicCode, CoalitionStatus
            """

            df = safe_execute_query(con, query, self.logger)

            if self.handle_empty_result(df, "minor topic", filters):
                return None

            df["BillCount"] = pd.to_numeric(df["BillCount"], errors="coerce").fillna(0)
            df["TopicCode"] = df["TopicCode"].astype(int)

            show_percentage = kwargs.get("show_percentage", False)
            return self._build_topic_chart(
                df, filters, show_percentage,
                topic_label="Minor Topic Code (MinorIL)",
                title_prefix="Bills by Minor Topic (MinorIL)",
                bar_height=30, min_height=600,
            )

    def _build_topic_chart(
        self,
        df: pd.DataFrame,
        filters: dict,
        show_percentage: bool,
        topic_label: str,
        title_prefix: str,
        bar_height: int = 45,
        min_height: int = 500,
        use_topic_labels: bool = False,
    ) -> go.Figure:
        """Build a horizontal stacked bar chart for topic distribution.

        Supports count mode (default) and percentage mode (100% stacked bars).
        When use_topic_labels=True, uses TopicLabel column for display axis
        while preserving numeric sort order.
        """
        # Sort by topic code numerically
        topic_order = sorted(df["TopicCode"].unique())

        if use_topic_labels and "TopicLabel" in df.columns:
            # Build ordered label list matching numeric sort
            label_map = dict(zip(df["TopicCode"], df["TopicLabel"]))
            topic_order_display = [label_map.get(t, str(t)) for t in topic_order]
            df["TopicDisplay"] = df["TopicLabel"]
            y_column = "TopicDisplay"
        else:
            df["TopicCode"] = df["TopicCode"].astype(str)
            topic_order_display = [str(t) for t in topic_order]
            y_column = "TopicCode"

        coalition_colors = {
            **self.config.COALITION_OPPOSITION_COLORS,
            "Unknown": "#808080",
        }
        status_order = ["Coalition", "Opposition", "Unknown"]
        n_topics = len(topic_order_display)

        pct_suffix = " (%)" if show_percentage else ""
        fig = px.bar(
            df,
            y=y_column,
            x="BillCount",
            color="CoalitionStatus",
            orientation="h",
            title=f"<b>{title_prefix} - {filters['knesset_title']}{pct_suffix}</b>",
            labels={
                y_column: topic_label,
                "BillCount": "Share (%)" if show_percentage else "Number of Bills",
                "CoalitionStatus": "Coalition Status",
            },
            color_discrete_map=coalition_colors,
            category_orders={
                y_column: topic_order_display,
                "CoalitionStatus": status_order,
            },
        )

        left_margin = 250 if use_topic_labels else 80
        layout_kwargs: dict = {
            "xaxis_title": "Share (%)" if show_percentage else "Number of Bills",
            "yaxis_title": topic_label,
            "yaxis": dict(type="category", dtick=1),
            "title_x": 0.5,
            "height": max(min_height, n_topics * bar_height + 200),
            "margin": dict(t=100, l=left_margin, r=40),
            "legend_title_text": "Coalition Status",
            "barmode": "stack",
            "bargap": 0.2,
        }

        if show_percentage:
            layout_kwargs["barnorm"] = "percent"
            layout_kwargs["xaxis"] = dict(
                title="Share (%)", ticksuffix="%", range=[0, 100]
            )

        fig.update_layout(**layout_kwargs)

        return fig

    @chart_error_handler("government bills topic distribution")
    def plot_majoril_distribution_gov(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate topic distribution for government bills (no coalition split).

        Government bills have no initiating MK, so coalition/opposition status
        is not applicable. Shows simple horizontal bar chart by topic.
        """
        if not self.check_database_exists():
            return None

        filters = self.build_filters(
            knesset_filter, faction_filter, table_prefix="b", **kwargs
        )

        with get_db_connection(
            self.db_path, read_only=True, logger_obj=self.logger
        ) as con:
            required = ["KNS_Bill", "UserBillCoding"]
            if not self.check_tables_exist(con, required):
                return None

            query = f"""
                SELECT
                    ubc.MajorIL AS TopicCode,
                    COUNT(DISTINCT b.BillID) AS BillCount
                FROM KNS_Bill b
                JOIN UserBillCoding ubc ON b.BillID = ubc.BillID
                WHERE ubc.MajorIL IS NOT NULL
                    AND b.PrivateNumber IS NULL
                    AND {filters["knesset_condition"]}
                GROUP BY ubc.MajorIL
                ORDER BY TopicCode
            """

            df = safe_execute_query(con, query, self.logger)
            if self.handle_empty_result(df, "government bill topic", filters):
                return None

            df["BillCount"] = pd.to_numeric(
                df["BillCount"], errors="coerce"
            ).fillna(0)
            df["TopicCode"] = df["TopicCode"].astype(int)

            # Apply labels
            from utils.majoril_labels import apply_majoril_labels

            df = apply_majoril_labels(df)

            topic_order = sorted(df["TopicCode"].unique())
            label_map = dict(zip(df["TopicCode"], df["TopicLabel"]))
            topic_order_display = [
                label_map.get(t, str(t)) for t in topic_order
            ]
            df["TopicDisplay"] = df["TopicLabel"]

            fig = px.bar(
                df.sort_values("TopicCode"),
                y="TopicDisplay",
                x="BillCount",
                orientation="h",
                title=(
                    f"<b>Government Bills by Major Topic"
                    f" - {filters['knesset_title']}</b>"
                ),
                labels={
                    "TopicDisplay": "Major Topic",
                    "BillCount": "Number of Bills",
                },
                category_orders={"TopicDisplay": topic_order_display},
            )
            fig.update_layout(
                yaxis=dict(type="category", dtick=1),
                height=max(500, len(topic_order) * 45 + 200),
                title_x=0.5,
                margin=dict(t=100, l=250, r=40),
            )
            return fig

    @chart_error_handler("policy topic time trend")
    def plot_majoril_time_trend(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate heatmap of major policy topics across Knesset terms.

        X-axis: Knesset terms
        Y-axis: MAJORIL topic labels
        Color: Share (%) of bills per topic within each Knesset term
        Optional: split by coalition/opposition via split_by_coalition kwarg
        """
        if not self.check_database_exists():
            return None

        bill_origin = kwargs.get("bill_origin_filter", "All Bills")
        split_by_coalition = kwargs.get("split_by_coalition", False)

        # Build origin condition
        origin_cond = "1=1"
        if bill_origin == "Private Bills Only":
            origin_cond = "b.PrivateNumber IS NOT NULL"
        elif bill_origin == "Governmental Bills Only":
            origin_cond = "b.PrivateNumber IS NULL"

        with get_db_connection(
            self.db_path, read_only=True, logger_obj=self.logger
        ) as con:
            required = ["KNS_Bill", "UserBillCoding"]
            if not self.check_tables_exist(con, required):
                return None

            if split_by_coalition:
                query = f"""
                    WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                    SELECT
                        b.KnessetNum,
                        ubc.MajorIL AS TopicCode,
                        COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
                        COUNT(DISTINCT b.BillID) AS BillCount
                    FROM KNS_Bill b
                    JOIN UserBillCoding ubc ON b.BillID = ubc.BillID
                    LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                    LEFT JOIN KNS_BillInitiator bi
                        ON b.BillID = bi.BillID AND bi.Ordinal = 1
                    LEFT JOIN KNS_PersonToPosition p2p
                        ON bi.PersonID = p2p.PersonID
                        AND b.KnessetNum = p2p.KnessetNum
                        AND p2p.FactionID IS NOT NULL
                        AND COALESCE(
                            bfs.FirstSubmissionDate,
                            CAST(b.LastUpdatedDate AS TIMESTAMP)
                        )
                            BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                            AND CAST(
                                COALESCE(p2p.FinishDate, '9999-12-31')
                                AS TIMESTAMP
                            )
                    LEFT JOIN UserFactionCoalitionStatus ufs
                        ON p2p.FactionID = ufs.FactionID
                        AND b.KnessetNum = ufs.KnessetNum
                    WHERE ubc.MajorIL IS NOT NULL
                        AND {origin_cond}
                    GROUP BY b.KnessetNum, ubc.MajorIL, ufs.CoalitionStatus
                    ORDER BY b.KnessetNum, TopicCode
                """
            else:
                query = f"""
                    SELECT
                        b.KnessetNum,
                        ubc.MajorIL AS TopicCode,
                        COUNT(DISTINCT b.BillID) AS BillCount
                    FROM KNS_Bill b
                    JOIN UserBillCoding ubc ON b.BillID = ubc.BillID
                    WHERE ubc.MajorIL IS NOT NULL
                        AND {origin_cond}
                    GROUP BY b.KnessetNum, ubc.MajorIL
                    ORDER BY b.KnessetNum, TopicCode
                """

            df = safe_execute_query(con, query, self.logger)
            if self.handle_empty_result(df, "policy topic trend", {}):
                return None

        df["TopicCode"] = df["TopicCode"].astype(int)
        df["BillCount"] = pd.to_numeric(
            df["BillCount"], errors="coerce"
        ).fillna(0)

        # Apply labels
        from utils.majoril_labels import load_majoril_labels

        labels = load_majoril_labels()
        df["TopicLabel"] = df["TopicCode"].apply(
            lambda c: f"{c} - {labels.get(c, {}).get('he', '?')}"
        )

        if split_by_coalition:
            return self._build_coalition_split_heatmap(df, labels)
        return self._build_single_heatmap(df, labels)

    def _build_single_heatmap(
        self, df: pd.DataFrame, labels: dict
    ) -> go.Figure:
        """Build a single heatmap of topic salience across Knesset terms."""
        # Compute share within each Knesset term
        knesset_totals = df.groupby("KnessetNum")["BillCount"].sum()
        df = df.copy()
        df["Share"] = df.apply(
            lambda r: (
                round(
                    100.0 * r["BillCount"] / knesset_totals[r["KnessetNum"]],
                    1,
                )
                if knesset_totals.get(r["KnessetNum"], 0) > 0
                else 0
            ),
            axis=1,
        )

        topic_order = sorted(df["TopicCode"].unique())
        topic_labels_ordered = [
            f"{c} - {labels.get(c, {}).get('he', '?')}" for c in topic_order
        ]
        knesset_order = sorted(df["KnessetNum"].unique())

        pivot = df.pivot_table(
            values="Share",
            index="TopicLabel",
            columns="KnessetNum",
            fill_value=0,
        )
        pivot = pivot.reindex(topic_labels_ordered).fillna(0)
        pivot = pivot[sorted(pivot.columns)]

        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values,
                x=[f"K{k}" for k in pivot.columns],
                y=pivot.index.tolist(),
                colorscale="YlOrRd",
                text=pivot.values,
                texttemplate="%{text:.1f}%",
                textfont={"size": 9},
                hovertemplate=(
                    "Knesset: %{x}<br>"
                    "Topic: %{y}<br>"
                    "Share: %{z:.1f}%<extra></extra>"
                ),
                colorbar=dict(title="Share (%)", ticksuffix="%"),
            )
        )

        fig.update_layout(
            title="<b>Major Policy Topic Salience Across Knesset Terms</b>",
            title_x=0.5,
            xaxis_title="Knesset Term",
            yaxis_title="Major Topic (MajorIL)",
            yaxis=dict(type="category", dtick=1, autorange="reversed"),
            height=max(600, len(topic_order) * 35 + 200),
            margin=dict(t=100, l=250, r=80),
        )
        return fig

    def _build_coalition_split_heatmap(
        self, df: pd.DataFrame, labels: dict
    ) -> go.Figure:
        """Build side-by-side heatmaps for Coalition and Opposition."""
        from plotly.subplots import make_subplots

        topic_order = sorted(df["TopicCode"].unique())
        topic_labels_ordered = [
            f"{c} - {labels.get(c, {}).get('he', '?')}" for c in topic_order
        ]

        fig = make_subplots(
            rows=1,
            cols=2,
            subplot_titles=("Coalition", "Opposition"),
            shared_yaxes=True,
            horizontal_spacing=0.05,
        )

        for col_idx, status in enumerate(["Coalition", "Opposition"], 1):
            sub_df = df[df["CoalitionStatus"] == status].copy()
            if sub_df.empty:
                continue

            knesset_totals = sub_df.groupby("KnessetNum")["BillCount"].sum()
            sub_df["Share"] = sub_df.apply(
                lambda r: (
                    round(
                        100.0
                        * r["BillCount"]
                        / knesset_totals[r["KnessetNum"]],
                        1,
                    )
                    if knesset_totals.get(r["KnessetNum"], 0) > 0
                    else 0
                ),
                axis=1,
            )

            pivot = sub_df.pivot_table(
                values="Share",
                index="TopicLabel",
                columns="KnessetNum",
                fill_value=0,
            )
            pivot = pivot.reindex(topic_labels_ordered).fillna(0)
            pivot = pivot[sorted(pivot.columns)]

            fig.add_trace(
                go.Heatmap(
                    z=pivot.values,
                    x=[f"K{k}" for k in pivot.columns],
                    y=pivot.index.tolist(),
                    colorscale="YlOrRd",
                    text=pivot.values,
                    texttemplate="%{text:.1f}%",
                    textfont={"size": 8},
                    showscale=(col_idx == 2),
                    colorbar=(
                        dict(title="Share (%)") if col_idx == 2 else None
                    ),
                    zmin=0,
                    zmax=40,
                ),
                row=1,
                col=col_idx,
            )

        fig.update_layout(
            title=(
                "<b>Major Topic Salience: Coalition vs Opposition"
                " Across Knesset Terms</b>"
            ),
            title_x=0.5,
            height=max(600, len(topic_order) * 35 + 250),
            margin=dict(t=120, l=250, r=80),
        )
        fig.update_yaxes(type="category", dtick=1, autorange="reversed")
        return fig

    def generate(self, chart_type: str = "", **kwargs: Any) -> Optional[go.Figure]:
        """Generate the requested distribution chart."""
        chart_methods: dict[str, Callable[..., Optional[go.Figure]]] = {
            "query_types_distribution": self.plot_query_types_distribution,
            "agenda_classifications_pie": self.plot_agenda_classifications_pie,
            "query_status_distribution": self.plot_query_status_distribution,
            "agenda_status_distribution": self.plot_agenda_status_distribution,
            "bill_subtype_distribution": self.plot_bill_subtype_distribution,
            "majoril_distribution": self.plot_majoril_distribution,
            "minoril_distribution": self.plot_minoril_distribution,
            "majoril_distribution_gov": self.plot_majoril_distribution_gov,
            "majoril_time_trend": self.plot_majoril_time_trend,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown distribution chart type: {chart_type}")
            return None
