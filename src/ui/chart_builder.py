"""
Chart Builder Core Logic - Manages state and validation
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import streamlit as st

from .chart_config import (
    CHART_TYPES, CHART_REQUIREMENTS, NUMERIC_Y_REQUIRED, 
    NON_XY_CHARTS, DEFAULT_CONFIG
)
from .chart_data_service import ChartDataService
from . import ui_utils


class ChartBuilder:
    """Core chart builder logic and state management"""
    
    def __init__(
        self, 
        db_path: Path, 
        max_rows_for_chart_builder: int,
        max_unique_values_for_facet: int,
        faction_display_map: Dict[str, int],
        logger_obj: logging.Logger
    ):
        self.db_path = db_path
        self.max_rows = max_rows_for_chart_builder
        self.max_facet_values = max_unique_values_for_facet
        self.faction_display_map = faction_display_map
        self.logger = logger_obj
        self.data_service = ChartDataService(db_path, logger_obj)
    
    def initialize_session_state(self) -> None:
        """Initialize session state with default values"""
        defaults = {
            "builder_title_font_size": DEFAULT_CONFIG["title_font_size"],
            "builder_title_font_family": DEFAULT_CONFIG["title_font_family"],
            "builder_axis_label_font_size": DEFAULT_CONFIG["axis_label_font_size"],
            "builder_legend_orientation": DEFAULT_CONFIG["legend_orientation"],
            "builder_legend_x": DEFAULT_CONFIG["legend_x"],
            "builder_legend_y": DEFAULT_CONFIG["legend_y"],
            "builder_color_palette": DEFAULT_CONFIG["color_palette"],
            "builder_marker_opacity": DEFAULT_CONFIG["marker_opacity"],
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    def handle_table_selection(self, selected_table: str) -> bool:
        """
        Handle table selection changes and update dependent state.
        Returns True if rerun is needed.
        """
        previous_table = st.session_state.get("builder_selected_table_previous_run")
        
        if selected_table == previous_table:
            return False
        
        self.logger.info(f"Chart Builder: User selection for table changed from '{previous_table}' to '{selected_table}'.")
        
        if selected_table and selected_table != "":
            self._set_active_table(selected_table)
        else:
            self._reset_table_state()
        
        self._reset_dependent_selections()
        st.session_state.builder_selected_table_previous_run = selected_table
        return True
    
    def _set_active_table(self, table_name: str) -> None:
        """Set active table and update columns"""
        st.session_state.builder_selected_table = table_name
        self.logger.info(f"Chart Builder: Valid table '{table_name}' set. Updating columns and resetting axes.")
        
        all_cols, numeric_cols, cat_cols = ui_utils.get_table_columns(self.db_path, table_name, self.logger)
        st.session_state.builder_columns = [""] + all_cols
        st.session_state.builder_numeric_columns = [""] + numeric_cols
        st.session_state.builder_categorical_columns = [""] + cat_cols
        
        # Fetch data for chart-specific filters
        sidebar_knesset_filter = st.session_state.get("ms_knesset_filter", [])
        sidebar_faction_filter_names = st.session_state.get("ms_faction_filter", [])
        
        filter_data = self.data_service.get_table_data_for_filters(
            table_name, self.max_rows, sidebar_knesset_filter, 
            sidebar_faction_filter_names, self.faction_display_map
        )
        st.session_state.builder_data_for_cs_filters = filter_data
    
    def _reset_table_state(self) -> None:
        """Reset table-related state"""
        self.logger.info("Chart Builder: Placeholder selected for table. Resetting active table and dependent state.")
        st.session_state.builder_selected_table = None
        st.session_state.builder_columns = [""]
        st.session_state.builder_numeric_columns = [""]
        st.session_state.builder_categorical_columns = [""]
        st.session_state.builder_data_for_cs_filters = pd.DataFrame()
    
    def _reset_dependent_selections(self) -> None:
        """Reset all chart selections that depend on table"""
        selections = [
            "builder_x_axis", "builder_y_axis", "builder_color", "builder_size",
            "builder_facet_row", "builder_facet_col", "builder_hover_name",
            "builder_names", "builder_values", "builder_generated_chart"
        ]
        for selection in selections:
            st.session_state[selection] = None
        
        st.session_state.builder_knesset_filter_cs = []
        st.session_state.builder_faction_filter_cs = []
    
    def handle_chart_type_change(self) -> bool:
        """
        Handle chart type changes and validate axes.
        Returns True if rerun is needed.
        """
        current_type = st.session_state.get("builder_chart_type")
        previous_type = st.session_state.get("previous_builder_chart_type")
        
        if current_type == previous_type:
            return False
        
        self.logger.info(f"Chart type changed from {previous_type} to {current_type}. Validating axes.")
        rerun_needed = False
        
        # Validate Y-axis for numeric requirements
        if current_type in NUMERIC_Y_REQUIRED:
            y_axis_current = st.session_state.get("builder_y_axis")
            numeric_options = st.session_state.get("builder_numeric_columns", [])
            
            if y_axis_current and y_axis_current not in numeric_options:
                self.logger.info(f"Resetting Y-axis ('{y_axis_current}') as it's not numeric and chart type '{current_type}' requires numeric Y.")
                st.session_state.builder_y_axis = None
                rerun_needed = True
        
        # Reset pie-specific fields when moving away from pie
        if current_type != "pie" and previous_type == "pie":
            st.session_state.builder_names = None
            st.session_state.builder_values = None
            rerun_needed = True
        
        st.session_state.previous_builder_chart_type = current_type
        st.session_state.builder_generated_chart = None
        
        return rerun_needed
    
    def validate_chart_parameters(self, chart_type: str, params: Dict[str, Any]) -> tuple[bool, str]:
        """
        Validate chart parameters based on chart type requirements.
        Returns (is_valid, error_message)
        """
        if chart_type not in CHART_REQUIREMENTS:
            return False, f"Unknown chart type: {chart_type}"
        
        requirements = CHART_REQUIREMENTS[chart_type]
        
        # Check required parameters
        if "required" in requirements:
            for param in requirements["required"]:
                if not params.get(param):
                    return False, f"Please select valid {param.replace('_', ' ')} for {chart_type} chart."
        
        # Check required_any (at least one must be present)
        if "required_any" in requirements:
            if not any(params.get(param) for param in requirements["required_any"]):
                param_names = " or ".join(requirements["required_any"])
                return False, f"Please select at least one of: {param_names} for {chart_type} chart."
        
        return True, ""
    
    def validate_facet_cardinality(self, df: pd.DataFrame, facet_row: str, facet_col: str) -> tuple[bool, str]:
        """
        Validate facet columns don't have too many unique values.
        Returns (is_valid, error_message)
        """
        if facet_row and facet_row in df.columns:
            unique_rows = df[facet_row].nunique()
            if unique_rows > self.max_facet_values:
                return False, f"Cannot use '{facet_row}' for Facet Row: Too many unique values ({unique_rows}). Max allowed: {self.max_facet_values}."
        
        if facet_col and facet_col in df.columns:
            unique_cols = df[facet_col].nunique()
            if unique_cols > self.max_facet_values:
                return False, f"Cannot use '{facet_col}' for Facet Column: Too many unique values ({unique_cols}). Max allowed: {self.max_facet_values}."
        
        return True, ""
    
    def convert_session_state_to_chart_params(self) -> Dict[str, Any]:
        """Convert session state to chart parameters dictionary"""
        def get_non_empty(key: str) -> Optional[str]:
            value = st.session_state.get(key, "")
            return value if value and value != "" else None
        
        return {
            "x": get_non_empty('builder_x_axis'),
            "y": get_non_empty('builder_y_axis'),
            "names": get_non_empty('builder_names'),
            "values": get_non_empty('builder_values'),
            "color": get_non_empty('builder_color'),
            "size": get_non_empty('builder_size'),
            "facet_row": get_non_empty('builder_facet_row'),
            "facet_col": get_non_empty('builder_facet_col'),
            "hover_name": get_non_empty('builder_hover_name'),
        }
    
    def convert_faction_names_to_ids(self, faction_names: List[str]) -> List[int]:
        """Convert faction display names to IDs using current data"""
        if not faction_names:
            return []
        
        cs_filter_data = st.session_state.get("builder_data_for_cs_filters", pd.DataFrame())
        if cs_filter_data.empty or 'FactionID' not in cs_filter_data.columns:
            return []
        
        unique_faction_ids = cs_filter_data['FactionID'].dropna().unique()
        chart_specific_map = {
            name: f_id for name, f_id in self.faction_display_map.items()
            if f_id in unique_faction_ids
        }
        
        return [chart_specific_map[name] for name in faction_names if name in chart_specific_map]