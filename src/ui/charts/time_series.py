"""Time series chart generators."""

from datetime import datetime
from pathlib import Path
from typing import Optional, List
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from .base import BaseChart
from backend.connection_manager import get_db_connection, safe_execute_query


class TimeSeriesCharts(BaseChart):
    """Time series and temporal analysis charts."""
    
    def plot_queries_by_time_period(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        aggregation_level: str = "Yearly",
        show_average_line: bool = False
    ) -> Optional[go.Figure]:
        """Generate a bar chart of Knesset queries per time period."""
        
        if not self.check_database_exists():
            return None
        
        # Build filter conditions
        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q")
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if not self.check_tables_exist(con, ["KNS_Query"]):
                    return None
                
                current_year = datetime.now().year
                date_column = "q.SubmitDate"
                
                # Configure time period aggregation
                time_configs = {
                    "Monthly": {
                        "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y-%m')",
                        "label": "Year-Month"
                    },
                    "Quarterly": {
                        "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y') || '-Q' || CAST((CAST(strftime(CAST({date_column} AS TIMESTAMP), '%m') AS INTEGER) - 1) / 3 + 1 AS VARCHAR)",
                        "label": "Year-Quarter"
                    },
                    "Yearly": {
                        "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y')",
                        "label": "Year"
                    }
                }
                
                config = time_configs.get(aggregation_level, time_configs["Yearly"])
                time_period_sql = config["sql"]
                x_axis_label = config["label"]
                
                # Build SQL query
                knesset_select = "" if filters['is_single_knesset'] else "q.KnessetNum,"
                
                query = f"""
                    SELECT
                        {time_period_sql} AS TimePeriod,
                        {knesset_select}
                        COUNT(q.QueryID) AS QueryCount
                    FROM KNS_Query q
                    WHERE {date_column} IS NOT NULL
                        AND q.KnessetNum IS NOT NULL
                        AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
                        AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) > 1940
                        AND {filters['knesset_condition']}
                """
                
                group_by_terms = ["TimePeriod"]
                if not filters['is_single_knesset']:
                    group_by_terms.append("q.KnessetNum")
                
                query += f" GROUP BY {', '.join(group_by_terms)}"
                query += f" ORDER BY {', '.join(group_by_terms)}, QueryCount DESC"
                
                self.logger.debug(f"Executing time series query: {query}")
                df = safe_execute_query(con, query, self.logger)
                
                if df.empty:
                    st.info(f"No query data found for '{filters['knesset_title']}' to visualize 'Queries by {x_axis_label}' with the current filters.")
                    return None
                
                # Prepare data for plotting
                if "KnessetNum" in df.columns:
                    df["KnessetNum"] = df["KnessetNum"].astype(str)
                df["TimePeriod"] = df["TimePeriod"].astype(str)
                
                # Create the chart
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
                    labels={
                        "TimePeriod": x_axis_label,
                        "QueryCount": "Number of Queries",
                        "KnessetNum": "Knesset Number"
                    },
                    category_orders={"TimePeriod": sorted(df["TimePeriod"].unique())},
                    custom_data=custom_data_cols,
                    color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE
                )
                
                # Configure hover template
                if "KnessetNum" in df.columns and len(custom_data_cols) > 2:
                    hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Knesset:</b> %{customdata[2]}<br><b>Queries:</b> %{y}<extra></extra>"
                else:
                    hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Queries:</b> %{y}<extra></extra>"
                
                fig.update_traces(hovertemplate=hovertemplate)
                
                # Update layout
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
                            y=avg_queries,
                            line_dash="dash",
                            line_color="red",
                            annotation_text=f"Avg Queries/Period: {avg_queries:.1f}",
                            annotation_position="bottom right",
                            annotation_font_size=10,
                            annotation_font_color="red"
                        )
                
                return fig
                
        except Exception as e:
            self.logger.error(f"Error generating time series chart: {e}", exc_info=True)
            st.error(f"Could not generate 'Queries by {x_axis_label}' plot: {e}")
            return None
    
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
        
        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a")
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if not self.check_tables_exist(con, ["KNS_Agenda"]):
                    return None
                
                current_year = datetime.now().year
                date_column = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"
                
                # Configure time period aggregation
                time_configs = {
                    "Monthly": {
                        "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y-%m')",
                        "label": "Year-Month"
                    },
                    "Quarterly": {
                        "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y') || '-Q' || CAST((CAST(strftime(CAST({date_column} AS TIMESTAMP), '%m') AS INTEGER) - 1) / 3 + 1 AS VARCHAR)",
                        "label": "Year-Quarter"
                    },
                    "Yearly": {
                        "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y')",
                        "label": "Year"
                    }
                }
                
                config = time_configs.get(aggregation_level, time_configs["Yearly"])
                time_period_sql = config["sql"]
                x_axis_label = config["label"]
                
                # Build SQL query
                knesset_select = "" if filters['is_single_knesset'] else "a.KnessetNum,"
                
                query = f"""
                    SELECT
                        {time_period_sql} AS TimePeriod,
                        {knesset_select}
                        COUNT(a.AgendaID) AS AgendaCount
                    FROM KNS_Agenda a
                    WHERE {date_column} IS NOT NULL
                        AND a.KnessetNum IS NOT NULL
                        AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
                        AND CAST(strftime(CAST({date_column} AS TIMESTAMP), '%Y') AS INTEGER) > 1940
                        AND {filters['knesset_condition']}
                """
                
                group_by_terms = ["TimePeriod"]
                if not filters['is_single_knesset']:
                    group_by_terms.append("a.KnessetNum")
                
                query += f" GROUP BY {', '.join(group_by_terms)}"
                query += f" ORDER BY {', '.join(group_by_terms)}, AgendaCount DESC"
                
                self.logger.debug(f"Executing agendas time series query: {query}")
                df = safe_execute_query(con, query, self.logger)
                
                if df.empty:
                    st.info(f"No agenda data found for '{filters['knesset_title']}' to visualize 'Agendas by {x_axis_label}' with the current filters.")
                    return None
                
                # Prepare data for plotting
                if "KnessetNum" in df.columns:
                    df["KnessetNum"] = df["KnessetNum"].astype(str)
                df["TimePeriod"] = df["TimePeriod"].astype(str)
                
                # Create the chart
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
                    labels={
                        "TimePeriod": x_axis_label,
                        "AgendaCount": "Number of Agenda Items",
                        "KnessetNum": "Knesset Number"
                    },
                    category_orders={"TimePeriod": sorted(df["TimePeriod"].unique())},
                    custom_data=custom_data_cols,
                    color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE
                )
                
                # Configure hover template
                if "KnessetNum" in df.columns and len(custom_data_cols) > 2:
                    hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Knesset:</b> %{customdata[2]}<br><b>Agendas:</b> %{y}<extra></extra>"
                else:
                    hovertemplate = "<b>Period:</b> %{customdata[0]}<br><b>Agendas:</b> %{y}<extra></extra>"
                
                fig.update_traces(hovertemplate=hovertemplate)
                
                # Update layout
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
                            y=avg_agendas,
                            line_dash="dash",
                            line_color="red",
                            annotation_text=f"Avg Agendas/Period: {avg_agendas:.1f}",
                            annotation_position="bottom right",
                            annotation_font_size=10,
                            annotation_font_color="red"
                        )
                
                return fig
                
        except Exception as e:
            self.logger.error(f"Error generating agendas time series: {e}", exc_info=True)
            st.error(f"Could not generate 'Agendas by {x_axis_label}' plot: {e}")
            return None
    
    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested time series chart."""
        chart_methods = {
            "queries_by_time": self.plot_queries_by_time_period,
            "agendas_by_time": self.plot_agendas_by_time_period,
        }
        
        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown time series chart type: {chart_type}")
            return None