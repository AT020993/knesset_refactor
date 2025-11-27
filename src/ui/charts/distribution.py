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

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a", **kwargs)

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

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a", **kwargs)

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

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill"]):
                    return None

                query = f"""
                    SELECT
                        COALESCE(b.SubTypeDesc, 'Unknown') AS SubType,
                        CASE
                            WHEN b.StatusID = 118 THEN 'התקבלה בקריאה שלישית'
                            WHEN b.StatusID IN (104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114) THEN 'קריאה ראשונה'
                            ELSE 'הופסק/לא פעיל'
                        END AS Stage,
                        COUNT(b.BillID) AS Count
                    FROM KNS_Bill b
                    WHERE b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                        AND {filters["bill_origin_condition"]}
                    GROUP BY b.SubTypeDesc, Stage
                    ORDER BY SubType, Stage
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(
                        f"No bill subtype data found for '{filters['knesset_title']}'."
                    )
                    return None

                # Sort subtypes by total count
                subtype_totals = df.groupby('SubType')['Count'].sum().sort_values(ascending=False)
                subtypes = subtype_totals.index.tolist()

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
                    stage_data = df[df['Stage'] == stage].set_index('SubType')
                    counts = [stage_data.loc[subtype, 'Count'] if subtype in stage_data.index else 0
                             for subtype in subtypes]

                    fig.add_trace(go.Bar(
                        name=stage,
                        x=subtypes,
                        y=counts,
                        marker_color=stage_colors[stage],
                        text=counts,
                        textposition='inside',
                        textfont=dict(color='white', size=12),
                        hovertemplate='<b>%{x}</b><br>' +
                                      f'{stage}: %{{y}}<br>' +
                                      '<extra></extra>'
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
            "agenda_status_distribution": self.plot_agenda_status_distribution,
            "bill_subtype_distribution": self.plot_bill_subtype_distribution,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown distribution chart type: {chart_type}")
            return None
