"""
Chart Renderer - Handles UI rendering and chart generation
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import streamlit as st
import plotly.express as px

from .chart_config import (
    PLOTLY_COLOR_SCALES, PLOTLY_FONT_FAMILIES, CHART_TYPES, 
    BARMODE_OPTIONS, LEGEND_ORIENTATIONS
)
from .chart_builder import ChartBuilder
from . import ui_utils


class ChartRenderer:
    """Handles UI rendering and chart generation"""
    
    def __init__(self, chart_builder: ChartBuilder):
        self.builder = chart_builder
        self.logger = chart_builder.logger
    
    def render_table_selection(self) -> str:
        """Render table selection dropdown"""
        db_tables = [""] + ui_utils.get_db_table_list(self.builder.db_path, self.logger)
        
        current_table = st.session_state.get('builder_selected_table')
        default_index = 0
        if current_table and current_table in db_tables:
            try:
                default_index = db_tables.index(current_table)
            except ValueError:
                default_index = 0
        
        return st.selectbox(
            "1. Select Table to Visualize:",
            options=db_tables,
            index=default_index,
            key="builder_table_select_widget",
        )
    
    def render_chart_type_selection(self) -> None:
        """Render chart type selection"""
        current_chart_type = st.session_state.get("builder_chart_type", "bar")
        st.session_state.builder_chart_type = st.selectbox(
            "2. Select Chart Type:",
            options=CHART_TYPES,
            index=CHART_TYPES.index(current_chart_type) if current_chart_type in CHART_TYPES else 0,
            key="builder_chart_type_selector"
        )
    
    def render_chart_specific_filters(self) -> None:
        """Render chart-specific filter options"""
        st.markdown("##### 3. Apply Chart-Specific Filters (Optional):")
        cs_filter_data = st.session_state.get("builder_data_for_cs_filters", pd.DataFrame())
        
        if cs_filter_data.empty and st.session_state.get("builder_selected_table"):
            st.info(f"No data available for table '{st.session_state.builder_selected_table}' with the current global sidebar filters. Chart-specific filters cannot be populated.")
            return
        
        # Knesset filter
        if not cs_filter_data.empty and 'KnessetNum' in cs_filter_data.columns:
            unique_knessets = self.builder.data_service.get_unique_values_for_filter(
                cs_filter_data, 'KnessetNum', sort_reverse=True
            )
            if unique_knessets:
                st.session_state.builder_knesset_filter_cs = st.multiselect(
                    "Filter by Knesset Number(s) (Chart Specific):",
                    options=unique_knessets,
                    default=st.session_state.get("builder_knesset_filter_cs", []),
                    key="builder_knesset_filter_cs_widget"
                )
            else:
                st.caption("No Knesset numbers available in the filtered data for chart-specific Knesset filtering.")
        
        # Faction filter
        if not cs_filter_data.empty and 'FactionID' in cs_filter_data.columns:
            faction_options = self.builder.data_service.get_faction_filter_options(
                cs_filter_data, self.builder.faction_display_map
            )
            if faction_options:
                st.session_state.builder_faction_filter_cs = st.multiselect(
                    "Filter by Faction(s) (Chart Specific):",
                    options=faction_options,
                    default=st.session_state.get("builder_faction_filter_cs", []),
                    key="builder_faction_filter_cs_widget"
                )
            else:
                st.caption("No factions available in the filtered data for chart-specific faction filtering.")
    
    def render_chart_aesthetics(self) -> None:
        """Render chart aesthetics configuration"""
        st.markdown("##### 4. Configure Chart Aesthetics:")
        cols_c1, cols_c2 = st.columns(2)
        
        with cols_c1:
            self._render_axis_options()
            self._render_color_and_size_options()
        
        with cols_c2:
            self._render_facet_options()
            self._render_misc_options()
    
    def _render_axis_options(self) -> None:
        """Render X and Y axis selection"""
        def get_safe_index(options_list, current_value_key, default_value=None):
            val = st.session_state.get(current_value_key, default_value)
            try:
                return options_list.index(val) if val and val in options_list else 0
            except ValueError: 
                return 0
        
        # X-axis
        x_axis_options = st.session_state.get("builder_columns", [""])
        st.session_state.builder_x_axis = st.selectbox(
            "X-axis:", 
            options=x_axis_options, 
            index=get_safe_index(x_axis_options, "builder_x_axis"), 
            key="cb_x_axis"
        )
        
        # Y-axis (not for pie or histogram)
        chart_type = st.session_state.get("builder_chart_type", "bar")
        if chart_type not in ["pie", "histogram"]:
            y_axis_options_all = st.session_state.get("builder_columns", [""])
            y_axis_options_numeric = st.session_state.get("builder_numeric_columns", [""])
            current_y_options = y_axis_options_numeric if chart_type not in ["bar", "box"] else y_axis_options_all
            
            st.session_state.builder_y_axis = st.selectbox(
                "Y-axis:", 
                options=current_y_options, 
                index=get_safe_index(current_y_options, "builder_y_axis"), 
                help="Select a numeric column for Y-axis (Bar and Box plots can also use categorical).", 
                key="cb_y_axis"
            )
        
        # Pie chart specific options
        if chart_type == "pie":
            pie_names_options = st.session_state.get("builder_categorical_columns", [""])
            st.session_state.builder_names = st.selectbox(
                "Names (for Pie chart slices):", 
                options=pie_names_options, 
                index=get_safe_index(pie_names_options, "builder_names"), 
                key="cb_pie_names"
            )
            
            pie_values_options = st.session_state.get("builder_numeric_columns", [""])
            st.session_state.builder_values = st.selectbox(
                "Values (for Pie chart sizes):", 
                options=pie_values_options, 
                index=get_safe_index(pie_values_options, "builder_values"), 
                key="cb_pie_values"
            )
    
    def _render_color_and_size_options(self) -> None:
        """Render color and size selection"""
        def get_safe_index(options_list, current_value_key, default_value=None):
            val = st.session_state.get(current_value_key, default_value)
            try:
                return options_list.index(val) if val and val in options_list else 0
            except ValueError: 
                return 0
        
        # Color by
        color_by_options = st.session_state.get("builder_columns", [""])
        st.session_state.builder_color = st.selectbox(
            "Color by:", 
            options=color_by_options, 
            index=get_safe_index(color_by_options, "builder_color"), 
            key="cb_color"
        )
        
        # Size by (scatter only)
        if st.session_state.get("builder_chart_type") == "scatter":
            size_by_options = st.session_state.get("builder_numeric_columns", [""])
            st.session_state.builder_size = st.selectbox(
                "Size by (for scatter):", 
                options=size_by_options, 
                index=get_safe_index(size_by_options, "builder_size"), 
                key="cb_size"
            )
    
    def _render_facet_options(self) -> None:
        """Render facet row/column options"""
        def get_safe_index(options_list, current_value_key, default_value=None):
            val = st.session_state.get(current_value_key, default_value)
            try:
                return options_list.index(val) if val and val in options_list else 0
            except ValueError: 
                return 0
        
        facet_options = st.session_state.get("builder_columns", [""])
        
        st.session_state.builder_facet_row = st.selectbox(
            "Facet Row by:", 
            options=facet_options, 
            index=get_safe_index(facet_options, "builder_facet_row"), 
            key="cb_facet_row"
        )
        
        st.session_state.builder_facet_col = st.selectbox(
            "Facet Column by:", 
            options=facet_options, 
            index=get_safe_index(facet_options, "builder_facet_col"), 
            key="cb_facet_col"
        )
        
        hover_name_options = st.session_state.get("builder_columns", [""])
        st.session_state.builder_hover_name = st.selectbox(
            "Hover Name:", 
            options=hover_name_options, 
            index=get_safe_index(hover_name_options, "builder_hover_name"), 
            key="cb_hover_name"
        )
    
    def _render_misc_options(self) -> None:
        """Render miscellaneous chart options"""
        chart_type = st.session_state.get("builder_chart_type", "bar")
        
        # Log scale options (not for pie)
        if chart_type not in ["pie"]:
            st.session_state.builder_log_x = st.checkbox(
                "Logarithmic X-axis", 
                value=st.session_state.get("builder_log_x", False), 
                key="cb_log_x"
            )
            if chart_type not in ["histogram"]:
                st.session_state.builder_log_y = st.checkbox(
                    "Logarithmic Y-axis", 
                    value=st.session_state.get("builder_log_y", False), 
                    key="cb_log_y"
                )
        
        # Bar mode (bar charts only)
        if chart_type == "bar":
            current_barmode = st.session_state.get("builder_barmode", "stack")
            st.session_state.builder_barmode = st.selectbox(
                "Bar Mode:", 
                options=BARMODE_OPTIONS, 
                index=BARMODE_OPTIONS.index(current_barmode) if current_barmode in BARMODE_OPTIONS else 0, 
                key="cb_barmode"
            )
    
    def render_advanced_layout_options(self) -> None:
        """Render advanced layout configuration"""
        st.markdown("##### 5. Advanced Layout Options:")
        adv_cols1, adv_cols2 = st.columns(2)
        
        def get_safe_index(options_list, current_value_key, default_value=None):
            val = st.session_state.get(current_value_key, default_value)
            try:
                return options_list.index(val) if val and val in options_list else 0
            except ValueError: 
                return 0
        
        with adv_cols1:
            # Font options
            st.session_state.builder_title_font_family = st.selectbox(
                "Title Font Family:", 
                options=PLOTLY_FONT_FAMILIES,
                index=get_safe_index(PLOTLY_FONT_FAMILIES, "builder_title_font_family", "Open Sans"),
                key="cb_title_font_family"
            )
            
            st.session_state.builder_title_font_size = st.slider(
                "Title Font Size:", 
                min_value=10, max_value=40,
                value=st.session_state.get("builder_title_font_size", 20),
                key="cb_title_font_size"
            )
            
            st.session_state.builder_axis_label_font_size = st.slider(
                "Axis Label Font Size:", 
                min_value=8, max_value=30,
                value=st.session_state.get("builder_axis_label_font_size", 14),
                key="cb_axis_label_font_size"
            )
            
            # Color palette
            st.session_state.builder_color_palette = st.selectbox(
                "Color Palette (Qualitative):", 
                options=list(PLOTLY_COLOR_SCALES.keys()),
                index=get_safe_index(list(PLOTLY_COLOR_SCALES.keys()), "builder_color_palette", "Plotly"),
                key="cb_color_palette",
                help="Applied if 'Color by' is a categorical column."
            )
        
        with adv_cols2:
            # Legend options
            st.session_state.builder_legend_orientation = st.selectbox(
                "Legend Orientation:", 
                options=LEGEND_ORIENTATIONS,
                index=LEGEND_ORIENTATIONS.index(st.session_state.get("builder_legend_orientation", "v")),
                key="cb_legend_orientation"
            )
            
            st.session_state.builder_legend_x = st.number_input(
                "Legend X Position (0-1.5):", 
                min_value=0.0, max_value=1.5, step=0.01,
                value=float(st.session_state.get("builder_legend_x", 1.02)),
                key="cb_legend_x"
            )
            
            st.session_state.builder_legend_y = st.number_input(
                "Legend Y Position (0-1.5):", 
                min_value=0.0, max_value=1.5, step=0.01,
                value=float(st.session_state.get("builder_legend_y", 1.0)),
                key="cb_legend_y"
            )
            
            # Scatter-specific options
            if st.session_state.get("builder_chart_type") == "scatter":
                st.session_state.builder_marker_opacity = st.slider(
                    "Marker Opacity (for scatter):", 
                    min_value=0.1, max_value=1.0, step=0.1,
                    value=st.session_state.get("builder_marker_opacity", 1.0),
                    key="cb_marker_opacity"
                )
    
    def generate_chart(self) -> None:
        """Generate the chart based on current parameters"""
        self.logger.info("--- 'Generate Chart' BUTTON CLICKED ---")
        
        # Get chart parameters
        chart_params = self.builder.convert_session_state_to_chart_params()
        chart_type = st.session_state.get("builder_chart_type", "bar")
        
        # Get filters
        knesset_filter = st.session_state.get("builder_knesset_filter_cs", [])
        faction_filter_names = st.session_state.get("builder_faction_filter_cs", [])
        faction_filter_ids = self.builder.convert_faction_names_to_ids(faction_filter_names)
        
        self.logger.debug(f"Chart Builder Selections: X='{chart_params['x']}', Y='{chart_params['y']}', ChartType='{chart_type}'")
        self.logger.debug(f"Chart Specific Filters: Knessets={knesset_filter}, Faction IDs={faction_filter_ids}")
        
        # Validate input
        active_table = st.session_state.get("builder_selected_table")
        if not active_table:
            st.error("Error: No table selected for chart generation.")
            return
        
        is_valid, error_msg = self.builder.validate_chart_parameters(chart_type, chart_params)
        if not is_valid:
            st.error(error_msg)
            return
        
        self.logger.info(f"Input validated for table '{active_table}'. Proceeding to fetch data and generate chart.")
        
        try:
            # Get and filter data
            df_for_chart = st.session_state.get("builder_data_for_cs_filters", pd.DataFrame()).copy()
            self.logger.info(f"Starting with {len(df_for_chart)} rows for chart (data already globally filtered).")
            
            df_for_chart = self.builder.data_service.apply_chart_specific_filters(
                df_for_chart, knesset_filter, faction_filter_ids
            )
            
            if df_for_chart.empty:
                st.warning("No data remains after applying all filters (global and chart-specific). Cannot generate chart.")
                self.logger.warning("DataFrame for chart is empty after all filters applied.")
                st.session_state.builder_generated_chart = None
                return
            
            # Validate facet cardinality
            facet_valid, facet_error = self.builder.validate_facet_cardinality(
                df_for_chart, chart_params.get('facet_row'), chart_params.get('facet_col')
            )
            if not facet_valid:
                st.error(facet_error)
                st.session_state.builder_generated_chart = None
                return
            
            # Build chart parameters for Plotly
            plotly_params = self._build_plotly_params(df_for_chart, chart_params, active_table, chart_type)
            
            self.logger.info(f"Attempting to generate {chart_type} chart with params: {plotly_params}")
            
            # Generate chart
            fig = getattr(px, chart_type)(**plotly_params)
            
            # Apply styling
            self._apply_chart_styling(fig, chart_type)
            
            st.session_state.builder_generated_chart = fig
            st.toast(f"Chart '{plotly_params['title']}' generated!", icon="✅")
            
        except Exception as e:
            self.logger.error(f"Error generating custom chart: {e}", exc_info=True)
            st.error(f"Could not generate chart: {e}")
            st.code(f"Error details: {str(e)}\n\nTraceback:\n{ui_utils.format_exception_for_ui(sys.exc_info())}")
            st.session_state.builder_generated_chart = None
    
    def _build_plotly_params(self, df: pd.DataFrame, chart_params: Dict[str, Any], table_name: str, chart_type: str) -> Dict[str, Any]:
        """Build parameters dictionary for Plotly chart functions"""
        plotly_params = {
            "data_frame": df,
            "title": f"{chart_type.capitalize()} of {table_name}"
        }
        
        # Add basic parameters
        for param in ['x', 'y', 'color', 'hover_name', 'facet_row', 'facet_col']:
            if chart_params.get(param):
                plotly_params[param] = chart_params[param]
        
        # Chart-specific parameters
        if chart_type == "pie":
            if chart_params.get('names'):
                plotly_params['names'] = chart_params['names']
            if chart_params.get('values'):
                plotly_params['values'] = chart_params['values']
        
        if chart_type == "scatter" and chart_params.get('size'):
            plotly_params['size'] = chart_params['size']
        
        # Add styling parameters
        if chart_type not in ["pie"]:
            plotly_params['log_x'] = st.session_state.get("builder_log_x", False)
            if chart_type not in ["histogram"]:
                plotly_params['log_y'] = st.session_state.get("builder_log_y", False)
        
        if chart_type == "bar" and st.session_state.get("builder_barmode"):
            plotly_params['barmode'] = st.session_state.builder_barmode
        
        # Color palette
        selected_palette = st.session_state.get("builder_color_palette", "Plotly")
        plotly_params['color_discrete_sequence'] = PLOTLY_COLOR_SCALES.get(selected_palette, px.colors.qualitative.Plotly)
        
        return plotly_params
    
    def _apply_chart_styling(self, fig, chart_type: str) -> None:
        """Apply styling to the chart"""
        fig.update_layout(
            title_font_family=st.session_state.get("builder_title_font_family"),
            title_font_size=st.session_state.get("builder_title_font_size"),
            xaxis_title_font_size=st.session_state.get("builder_axis_label_font_size"),
            yaxis_title_font_size=st.session_state.get("builder_axis_label_font_size"),
            legend_orientation=st.session_state.get("builder_legend_orientation"),
            legend_x=float(st.session_state.get("builder_legend_x", 1.02)),
            legend_y=float(st.session_state.get("builder_legend_y", 1.0)),
            legend_font_size=st.session_state.get("builder_axis_label_font_size")
        )
        
        # Scatter-specific styling
        if chart_type == "scatter" and "builder_marker_opacity" in st.session_state:
            fig.update_traces(marker=dict(opacity=st.session_state.builder_marker_opacity))
    
    def render_generated_chart(self) -> None:
        """Render the generated chart if it exists"""
        if st.session_state.get("builder_generated_chart"):
            st.plotly_chart(st.session_state.builder_generated_chart, use_container_width=True)