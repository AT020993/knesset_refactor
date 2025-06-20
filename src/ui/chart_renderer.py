"""
Chart Renderer - Simple implementation for basic chart building.
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Optional, List

from .chart_builder import ChartBuilder
import ui.ui_utils as ui_utils


class ChartRenderer:
    """Simple chart renderer for basic functionality."""

    def __init__(self, chart_builder: ChartBuilder):
        """Initialize chart renderer."""
        self.chart_builder = chart_builder

    def render_table_selection(self) -> str:
        """Render table selection UI."""
        try:
            tables = ui_utils.get_db_table_list(
                self.chart_builder.db_path, self.chart_builder.logger
            )
            table_options = [""] + [t for t in tables if t.startswith("KNS_")]

            selected_table = st.selectbox(
                "1. Select Table:",
                options=table_options,
                index=0
                if not st.session_state.get("builder_selected_table")
                else table_options.index(st.session_state.builder_selected_table)
                if st.session_state.builder_selected_table in table_options
                else 0,
                key="builder_table_selector",
            )
            return selected_table
        except Exception as e:
            st.error(f"Error loading tables: {e}")
            return ""

    def render_chart_type_selection(self):
        """Render chart type selection UI."""
        if st.session_state.get("builder_selected_table"):
            chart_types = ["", "Bar Chart", "Pie Chart", "Line Chart"]
            st.selectbox(
                "2. Select Chart Type:", options=chart_types, key="builder_chart_type"
            )

    def render_chart_specific_filters(self):
        """Render chart-specific filters."""
        if st.session_state.get("builder_chart_type"):
            st.text_input("3. Column for X-axis (optional):", key="builder_x_column")
            st.text_input("4. Column for Y-axis (optional):", key="builder_y_column")

    def render_chart_aesthetics(self):
        """Render chart aesthetics options."""
        if st.session_state.get("builder_chart_type"):
            st.text_input("5. Chart Title (optional):", key="builder_chart_title")

    def render_advanced_layout_options(self):
        """Render advanced layout options."""
        pass  # Keep simple for now

    def generate_chart(self):
        """Generate and display chart."""
        try:
            table = st.session_state.get("builder_selected_table")
            chart_type = st.session_state.get("builder_chart_type")
            x_col = st.session_state.get("builder_x_column", "")
            y_col = st.session_state.get("builder_y_column", "")
            title = st.session_state.get("builder_chart_title", "Custom Chart")

            if not all([table, chart_type]):
                st.warning("Please select both table and chart type.")
                return

            # Get sample data
            query = f"SELECT * FROM {table} LIMIT 1000"
            df = pd.DataFrame()
            with ui_utils.get_db_connection(
                self.chart_builder.db_path,
                read_only=True,
                logger_obj=self.chart_builder.logger,
            ) as con:
                df = ui_utils.safe_execute_query(
                    con, query, _logger_obj=self.chart_builder.logger
                )

            if df.empty:
                st.warning("No data found in selected table.")
                return

            # Generate basic chart
            if chart_type == "Bar Chart" and x_col and y_col:
                if x_col in df.columns and y_col in df.columns:
                    fig = px.bar(df, x=x_col, y=y_col, title=title)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error(f"Columns {x_col} or {y_col} not found in table.")
            elif chart_type == "Pie Chart" and x_col and y_col:
                if x_col in df.columns and y_col in df.columns:
                    fig = px.pie(df, names=x_col, values=y_col, title=title)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error(f"Columns {x_col} or {y_col} not found in table.")
            else:
                st.info(
                    "Please specify both X and Y columns for the selected chart type."
                )
                st.write("Available columns:", list(df.columns))

        except Exception as e:
            st.error(f"Error generating chart: {e}")
            self.chart_builder.logger.error(
                f"Chart generation error: {e}", exc_info=True
            )

    def render_generated_chart(self):
        """Render any generated chart (handled in generate_chart)."""
        pass
