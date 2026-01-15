"""Time series chart generators."""

from datetime import datetime
from pathlib import Path
from typing import Optional, List
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from .base import BaseChart, chart_error_handler
from backend.connection_manager import get_db_connection, safe_execute_query
from ui.queries.sql_templates import SQLTemplates


class TimeSeriesCharts(BaseChart):
    """Time series and temporal analysis charts."""

    @chart_error_handler("queries by time period")
    def plot_queries_by_time_period(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        aggregation_level: str = "Yearly",
        show_average_line: bool = False,
        **kwargs
    ) -> Optional[go.Figure]:
        """Generate a bar chart of Knesset queries per time period."""

        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q", **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            if not self.check_tables_exist(con, ["KNS_Query"]):
                return None

            current_year = datetime.now().year
            date_column = "q.SubmitDate"

            # Use consolidated time period config
            time_configs = self.get_time_period_config(date_column)
            config = time_configs.get(aggregation_level, time_configs["Yearly"])
            time_period_sql = config["sql"]
            x_axis_label = config["label"]

            knesset_select = "" if filters['is_single_knesset'] else "q.KnessetNum,"

            status_join = ""
            if filters['query_status_condition'] != "1=1":
                status_join = "LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID"

            query = f"""
                SELECT
                    {time_period_sql} AS TimePeriod,
                    {knesset_select}
                    COUNT(q.QueryID) AS QueryCount
                FROM KNS_Query q
                {status_join}
                WHERE {date_column} IS NOT NULL
                    AND q.KnessetNum IS NOT NULL
                    AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
                    AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) > 1940
                    AND {filters['knesset_condition']}
                    AND {filters['query_type_condition']}
                    AND {filters['query_status_condition']}
                    AND {filters['start_date_condition']}
                    AND {filters['end_date_condition']}
            """

            group_by_terms = ["TimePeriod"]
            if not filters['is_single_knesset']:
                group_by_terms.append("q.KnessetNum")

            query += f" GROUP BY {', '.join(group_by_terms)}"
            query += f" ORDER BY {', '.join(group_by_terms)}, QueryCount DESC"

            self.logger.debug(f"Executing time series query: {query}")
            df = safe_execute_query(con, query, self.logger)

            if self.handle_empty_result(df, "query", filters, f"Queries by {x_axis_label}"):
                return None

            # Optimize large datasets
            max_time_periods = 100
            if len(df['TimePeriod'].unique()) > max_time_periods:
                self.logger.info(f"Large dataset detected ({len(df)} rows), optimizing aggregation")
                if aggregation_level == "Monthly":
                    st.info("Dataset too large for monthly view. Automatically switching to yearly aggregation.")
                    df['Year'] = df['TimePeriod'].str[:4]
                    group_cols = ['Year', 'KnessetNum'] if "KnessetNum" in df.columns else ['Year']
                    df = df.groupby(group_cols, as_index=False)['QueryCount'].sum()
                    df.rename(columns={'Year': 'TimePeriod'}, inplace=True)

            # Normalize DataFrame types
            df = self.normalize_time_series_df(df)

            # Create chart
            plot_title = f"<b>Queries per {aggregation_level.replace('ly','')} for {filters['knesset_title']}</b>"
            color_param = "KnessetNum" if "KnessetNum" in df.columns and len(df["KnessetNum"].unique()) > 1 else None

            custom_data_cols = ["TimePeriod", "QueryCount"]
            if "KnessetNum" in df.columns:
                custom_data_cols.append("KnessetNum")

            fig = px.bar(
                df,
                x="TimePeriod",
                y="QueryCount",
                color=color_param,
                title=plot_title,
                labels={"TimePeriod": x_axis_label, "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                category_orders={"TimePeriod": sorted(df["TimePeriod"].unique())},
                custom_data=custom_data_cols,
                color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE
            )

            # Configure hover
            if "KnessetNum" in df.columns and len(custom_data_cols) > 2:
                hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Knesset:</b> %{customdata[2]}<br><b>Queries:</b> %{y}<extra></extra>"
            else:
                hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Queries:</b> %{y}<extra></extra>"
            fig.update_traces(hovertemplate=hovertemplate)

            fig.update_layout(
                xaxis_title=x_axis_label,
                yaxis_title="Number of Queries",
                legend_title_text='Knesset' if color_param else None,
                showlegend=bool(color_param),
                title_x=0.5,
                xaxis_type='category'
            )

            # Add average line if requested
            if show_average_line and not df.empty:
                avg_queries = df.groupby("TimePeriod")["QueryCount"].sum().mean()
                if pd.notna(avg_queries):
                    fig.add_hline(
                        y=avg_queries, line_dash="dash", line_color="red",
                        annotation_text=f"Avg Queries/Period: {avg_queries:.1f}",
                        annotation_position="bottom right", annotation_font_size=10, annotation_font_color="red"
                    )

            return fig
    
    @chart_error_handler("agendas by time period")
    def plot_agendas_by_time_period(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        aggregation_level: str = "Yearly",
        show_average_line: bool = False,
        **kwargs
    ) -> Optional[go.Figure]:
        """Generate a bar chart of Knesset agendas per time period."""

        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a", **kwargs)

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            if not self.check_tables_exist(con, ["KNS_Agenda"]):
                return None

            current_year = datetime.now().year
            date_column = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"

            # Use consolidated time period config
            time_configs = self.get_time_period_config(date_column)
            config = time_configs.get(aggregation_level, time_configs["Yearly"])
            time_period_sql = config["sql"]
            x_axis_label = config["label"]

            knesset_select = "" if filters['is_single_knesset'] else "a.KnessetNum,"

            status_join = ""
            if filters['agenda_status_condition'] != "1=1":
                status_join = "LEFT JOIN KNS_Status s ON a.StatusID = s.StatusID"

            query = f"""
                SELECT
                    {time_period_sql} AS TimePeriod,
                    {knesset_select}
                    COUNT(a.AgendaID) AS AgendaCount
                FROM KNS_Agenda a
                {status_join}
                WHERE {date_column} IS NOT NULL
                    AND a.KnessetNum IS NOT NULL
                    AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
                    AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) > 1940
                    AND {filters['knesset_condition']}
                    AND {filters['session_type_condition']}
                    AND {filters['agenda_status_condition']}
                    AND {filters['start_date_condition']}
                    AND {filters['end_date_condition']}
            """

            group_by_terms = ["TimePeriod"]
            if not filters['is_single_knesset']:
                group_by_terms.append("a.KnessetNum")

            query += f" GROUP BY {', '.join(group_by_terms)}"
            query += f" ORDER BY {', '.join(group_by_terms)}, AgendaCount DESC"

            self.logger.debug(f"Executing agendas time series query: {query}")
            df = safe_execute_query(con, query, self.logger)

            if self.handle_empty_result(df, "agenda", filters, f"Agendas by {x_axis_label}"):
                return None

            # Normalize DataFrame types
            df = self.normalize_time_series_df(df)

            # Create chart
            plot_title = f"<b>Agenda Items per {aggregation_level.replace('ly','')} for {filters['knesset_title']}</b>"
            color_param = "KnessetNum" if "KnessetNum" in df.columns and len(df["KnessetNum"].unique()) > 1 else None

            custom_data_cols = ["TimePeriod", "AgendaCount"]
            if "KnessetNum" in df.columns:
                custom_data_cols.append("KnessetNum")

            fig = px.bar(
                df,
                x="TimePeriod",
                y="AgendaCount",
                color=color_param,
                title=plot_title,
                labels={"TimePeriod": x_axis_label, "AgendaCount": "Number of Agenda Items", "KnessetNum": "Knesset Number"},
                category_orders={"TimePeriod": sorted(df["TimePeriod"].unique())},
                custom_data=custom_data_cols,
                color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE
            )

            # Configure hover
            if "KnessetNum" in df.columns and len(custom_data_cols) > 2:
                hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Knesset:</b> %{customdata[2]}<br><b>Agendas:</b> %{y}<extra></extra>"
            else:
                hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Agendas:</b> %{y}<extra></extra>"
            fig.update_traces(hovertemplate=hovertemplate)

            fig.update_layout(
                xaxis_title=x_axis_label,
                yaxis_title="Number of Agenda Items",
                legend_title_text='Knesset' if color_param else None,
                showlegend=bool(color_param),
                title_x=0.5,
                xaxis_type='category'
            )

            # Add average line if requested
            if show_average_line and not df.empty:
                avg_agendas = df.groupby("TimePeriod")["AgendaCount"].sum().mean()
                if pd.notna(avg_agendas):
                    fig.add_hline(
                        y=avg_agendas, line_dash="dash", line_color="red",
                        annotation_text=f"Avg Agendas/Period: {avg_agendas:.1f}",
                        annotation_position="bottom right", annotation_font_size=10, annotation_font_color="red"
                    )

            return fig
    
    @chart_error_handler("bills by time period")
    def plot_bills_by_time_period(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        aggregation_level: str = "Yearly",
        show_average_line: bool = False,
        **kwargs
    ) -> Optional[go.Figure]:
        """Generate a stacked bar chart of Knesset bills per time period categorized by status."""

        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)
        filters['bill_status_condition'] = "1=1"  # This chart does its own status categorization

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            if not self.check_tables_exist(con, ["KNS_Bill"]):
                return None

            current_year = datetime.now().year
            date_column = "COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))"

            # Use consolidated time period config
            time_configs = self.get_time_period_config(date_column)
            config = time_configs.get(aggregation_level, time_configs["Yearly"])
            time_period_sql = config["sql"]
            x_axis_label = config["label"]

            knesset_select = "" if filters['is_single_knesset'] else "b.KnessetNum,"

            query = f"""
                WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
                SELECT
                    {time_period_sql} AS TimePeriod,
                    {knesset_select}
                    {SQLTemplates.BILL_STATUS_CASE_HE} AS Stage,
                    COUNT(b.BillID) AS BillCount
                FROM KNS_Bill b
                LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                WHERE {date_column} IS NOT NULL
                    AND b.KnessetNum IS NOT NULL
                    AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
                    AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) > 1940
                    AND {filters['knesset_condition']}
                    AND {filters['bill_type_condition']}
                    AND {filters['bill_origin_condition']}
                    AND {filters['start_date_condition']}
                    AND {filters['end_date_condition']}
            """

            group_by_terms = ["TimePeriod", "Stage"]
            if not filters['is_single_knesset']:
                group_by_terms.insert(1, "b.KnessetNum")

            query += f" GROUP BY {', '.join(group_by_terms)}"
            query += f" ORDER BY TimePeriod"

            self.logger.debug(f"Executing bills time series query: {query}")
            df = safe_execute_query(con, query, self.logger)

            if self.handle_empty_result(df, "bill", filters, f"Bills by {x_axis_label}"):
                return None

            # Normalize DataFrame types
            df = self.normalize_time_series_df(df)

            # Use centralized stage order and colors from SQLTemplates
            stage_order = SQLTemplates.BILL_STAGE_ORDER
            stage_colors = SQLTemplates.BILL_STAGE_COLORS

            # Create stacked bar chart
            plot_title = f"<b>Bills per {aggregation_level.replace('ly','')} by Status for {filters['knesset_title']}</b>"

            fig = px.bar(
                df,
                x="TimePeriod",
                y="BillCount",
                color="Stage",
                title=plot_title,
                labels={"TimePeriod": x_axis_label, "BillCount": "Number of Bills", "Stage": "Bill Status"},
                category_orders={"TimePeriod": sorted(df["TimePeriod"].unique()), "Stage": stage_order},
                color_discrete_map=stage_colors,
                barmode='stack'
            )

            fig.update_traces(
                hovertemplate="<b>Period:</b> %{x}<br><b>Status:</b> %{fullData.name}<br><b>Bills:</b> %{y}<extra></extra>"
            )

            fig.update_layout(
                xaxis_title=x_axis_label,
                yaxis_title="Number of Bills",
                legend_title_text='Bill Status',
                showlegend=True,
                title_x=0.5,
                xaxis_type='category',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            # Add average line if requested
            if show_average_line and not df.empty:
                period_totals = df.groupby("TimePeriod")["BillCount"].sum()
                avg_bills = period_totals.mean()
                if pd.notna(avg_bills):
                    fig.add_hline(
                        y=avg_bills, line_dash="dash", line_color="black",
                        annotation_text=f"Avg Bills/Period: {avg_bills:.1f}",
                        annotation_position="bottom right", annotation_font_size=10, annotation_font_color="black"
                    )

            return fig

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested time series chart."""
        chart_methods = {
            "queries_by_time": self.plot_queries_by_time_period,
            "agendas_by_time": self.plot_agendas_by_time_period,
            "bills_by_time": self.plot_bills_by_time_period,
        }
        
        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown time series chart type: {chart_type}")
            return None