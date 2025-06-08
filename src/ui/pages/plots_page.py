"""
Predefined visualizations page UI components.

This module handles the rendering of the plots selection interface
and plot generation with proper parameter handling.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any, Callable, Optional, List

import streamlit as st

from ui.state.session_manager import SessionStateManager
import ui.ui_utils as ui_utils


class PlotsPageRenderer:
    """Handles rendering of the predefined visualizations section."""
    
    def __init__(self, db_path: Path, logger: logging.Logger):
        """
        Initialize the plots page renderer.
        
        Args:
            db_path: Path to the database
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.logger = logger

    def render_plots_section(
        self, 
        available_plots: Dict[str, Dict[str, Callable]],
        knesset_options: List[str],
        faction_display_map: Dict[str, int],
        connect_func: Callable
    ) -> None:
        """
        Render the complete plots selection and generation interface.
        
        Args:
            available_plots: Dictionary of plot topics and their available plots
            knesset_options: List of available Knesset numbers
            faction_display_map: Mapping from faction display names to IDs
            connect_func: Function to create database connections
        """
        st.divider()
        st.header("📈 Predefined Visualizations")
        
        if not self.db_path.exists():
            st.warning("Database not found. Visualizations cannot be generated. Please run a data refresh.")
            return
        
        # Topic selection
        selected_topic = self._render_topic_selection(available_plots)
        if not selected_topic:
            st.info("Select a plot topic to see available visualizations.")
            return
        
        # Chart selection within topic
        selected_chart = self._render_chart_selection(available_plots, selected_topic)
        if not selected_chart:
            st.info("Please choose a specific visualization from the dropdown above.")
            return
        
        # Knesset selection and plot options
        self._render_plot_options(selected_chart, knesset_options)
        
        # Generate and display plot
        self._generate_and_display_plot(
            available_plots, selected_topic, selected_chart,
            faction_display_map, connect_func
        )

    def _render_topic_selection(self, available_plots: Dict[str, Dict[str, Callable]]) -> str:
        """
        Render the plot topic selection dropdown.
        
        Args:
            available_plots: Dictionary of plot topics and their available plots
            
        Returns:
            Selected topic name or empty string
        """
        plot_topic_options = [""] + list(available_plots.keys())
        current_selected_topic = SessionStateManager.get_selected_plot_topic()
        topic_select_default_index = (
            plot_topic_options.index(current_selected_topic) 
            if current_selected_topic in plot_topic_options else 0
        )
        
        selected_topic_widget = st.selectbox(
            "1. Choose Plot Topic:",
            options=plot_topic_options,
            index=topic_select_default_index,
            key="sb_selected_plot_topic_widget"
        )
        
        # Handle topic change
        if selected_topic_widget != current_selected_topic:
            SessionStateManager.reset_plot_state(keep_topic=False)
            SessionStateManager.set_plot_selection(selected_topic_widget, "")
            st.rerun()
        
        return selected_topic_widget

    def _render_chart_selection(
        self, 
        available_plots: Dict[str, Dict[str, Callable]], 
        selected_topic: str
    ) -> str:
        """
        Render the chart selection dropdown for the selected topic.
        
        Args:
            available_plots: Dictionary of plot topics and their available plots
            selected_topic: Currently selected topic
            
        Returns:
            Selected chart name or empty string
        """
        if not selected_topic:
            return ""
        
        charts_in_topic = available_plots[selected_topic]
        chart_options_for_topic = [""] + list(charts_in_topic.keys())
        current_selected_chart = SessionStateManager.get_selected_plot_name()
        chart_select_default_index = (
            chart_options_for_topic.index(current_selected_chart)
            if current_selected_chart in chart_options_for_topic else 0
        )
        
        selected_chart_widget = st.selectbox(
            f"2. Choose Visualization for '{selected_topic}':",
            options=chart_options_for_topic,
            index=chart_select_default_index,
            key=f"sb_selected_chart_for_topic_{selected_topic.replace(' ', '_')}"
        )
        
        # Handle chart change
        if selected_chart_widget != current_selected_chart:
            # Reset plot-specific state but keep topic and chart selection
            st.session_state.plot_aggregation_level = "Yearly"
            st.session_state.plot_show_average_line = False
            st.session_state.plot_start_date = None
            st.session_state.plot_end_date = None
            SessionStateManager.set_plot_selection(selected_topic, selected_chart_widget)
            st.rerun()
        
        return selected_chart_widget

    def _render_plot_options(self, selected_chart: str, knesset_options: List[str]) -> None:
        """
        Render plot-specific options like Knesset selection, aggregation, etc.
        
        Args:
            selected_chart: Currently selected chart name
            knesset_options: List of available Knesset numbers
        """
        if not selected_chart:
            return
        
        # Determine available Knesset options for this chart
        plot_knesset_options = [""] + knesset_options
        can_show_all_knessets = selected_chart in ["Queries by Time Period", "Agenda Items by Time Period"]
        
        if can_show_all_knessets and "All Knessets (Color Coded)" not in plot_knesset_options:
            plot_knesset_options.insert(1, "All Knessets (Color Coded)")
        
        # Get current selection
        current_main_knesset_selection = SessionStateManager.get_plot_main_knesset_selection()
        if current_main_knesset_selection not in plot_knesset_options:
            current_main_knesset_selection = ""
            SessionStateManager.set_plot_knesset_selection("")
        
        # Render options based on chart type
        if selected_chart in ["Queries by Time Period", "Agenda Items by Time Period"]:
            self._render_time_period_plot_options(
                selected_chart, plot_knesset_options, current_main_knesset_selection
            )
        else:
            self._render_single_knesset_plot_options(
                selected_chart, plot_knesset_options, current_main_knesset_selection, can_show_all_knessets
            )
        
        # Add date picker for specific plots
        if selected_chart == "Query Status Description with Faction Breakdown (Single Knesset)":
            self._render_date_filter_options(selected_chart)

    def _render_time_period_plot_options(
        self, 
        selected_chart: str, 
        plot_knesset_options: List[str], 
        current_selection: str
    ) -> None:
        """Render options for time period plots with aggregation controls."""
        knesset_select_default_index = (
            plot_knesset_options.index(current_selection)
            if current_selection in plot_knesset_options else 0
        )
        
        aggregation_level = SessionStateManager.get_plot_aggregation_level()
        show_average_line = SessionStateManager.get_plot_show_average_line()
        
        col_knesset_select, col_agg_select, col_avg_line = st.columns([2, 1, 1])
        
        with col_knesset_select:
            selected_knesset_val = st.selectbox(
                "3. Select Knesset for Plot:",
                options=plot_knesset_options,
                index=knesset_select_default_index,
                key=f"plot_main_knesset_selector_tp_{selected_chart.replace(' ', '_')}"
            )
        
        with col_agg_select:
            st.session_state.plot_aggregation_level = st.selectbox(
                "Aggregate:", 
                options=["Yearly", "Monthly", "Quarterly"],
                index=["Yearly", "Monthly", "Quarterly"].index(aggregation_level),
                key=f"agg_level_{selected_chart.replace(' ', '_')}"
            )
        
        with col_avg_line:
            st.session_state.plot_show_average_line = st.checkbox(
                "Avg Line", 
                value=show_average_line,
                key=f"avg_line_{selected_chart.replace(' ', '_')}"
            )
        
        # Update session state if selection changed
        if selected_knesset_val != current_selection:
            SessionStateManager.set_plot_knesset_selection(selected_knesset_val)
            st.rerun()

    def _render_single_knesset_plot_options(
        self, 
        selected_chart: str, 
        plot_knesset_options: List[str], 
        current_selection: str,
        can_show_all_knessets: bool
    ) -> None:
        """Render options for single Knesset plots."""
        # Filter out "All Knessets" option for single Knesset plots
        options_for_single_knesset_plot = [
            opt for opt in plot_knesset_options 
            if opt != "All Knessets (Color Coded)" and opt != ""
        ]
        
        if current_selection not in options_for_single_knesset_plot and current_selection != "":
            current_selection = ""
            SessionStateManager.set_plot_knesset_selection("")
        
        effective_options_single = [""] + options_for_single_knesset_plot
        single_knesset_default_idx = (
            effective_options_single.index(current_selection)
            if current_selection in effective_options_single else 0
        )
        
        selected_knesset_val = st.selectbox(
            "3. Select Knesset for Plot:",
            options=effective_options_single,
            index=single_knesset_default_idx,
            key=f"plot_main_knesset_selector_single_{selected_chart.replace(' ', '_')}"
        )
        
        # Update session state if selection changed
        if selected_knesset_val != current_selection:
            SessionStateManager.set_plot_knesset_selection(selected_knesset_val)
            st.rerun()

    def _render_date_filter_options(self, selected_chart: str) -> None:
        """Render date filter options for specific plots."""
        st.markdown("**Optional Date Range Filter:**")
        col_start_date, col_end_date = st.columns(2)
        
        with col_start_date:
            st.session_state.plot_start_date = st.date_input(
                "Start Date (optional)",
                value=SessionStateManager.get_plot_start_date(),
                key=f"start_date_{selected_chart.replace(' ', '_')}",
                help="Filter queries from this date onwards"
            )
        
        with col_end_date:
            st.session_state.plot_end_date = st.date_input(
                "End Date (optional)",
                value=SessionStateManager.get_plot_end_date(),
                key=f"end_date_{selected_chart.replace(' ', '_')}",
                help="Filter queries up to this date"
            )

    def _generate_and_display_plot(
        self,
        available_plots: Dict[str, Dict[str, Callable]],
        selected_topic: str,
        selected_chart: str,
        faction_display_map: Dict[str, int],
        connect_func: Callable
    ) -> None:
        """
        Generate and display the selected plot with current parameters.
        
        Args:
            available_plots: Dictionary of plot topics and their available plots
            selected_topic: Currently selected topic
            selected_chart: Currently selected chart
            faction_display_map: Mapping from faction display names to IDs
            connect_func: Function to create database connections
        """
        # Determine final Knesset filter
        final_knesset_filter = self._get_final_knesset_filter(selected_chart)
        
        # Check if we can generate the plot
        can_generate_plot = selected_chart and (final_knesset_filter is not False)
        
        if not can_generate_plot:
            requires_single_knesset = "(Single Knesset)" in selected_chart or selected_chart not in [
                "Queries by Time Period", "Agenda Items by Time Period"
            ]
            if requires_single_knesset:
                st.info(f"Please select a Knesset for the '{selected_chart}' plot.")
            return
        
        # Build plot arguments
        plot_function = available_plots[selected_topic][selected_chart]
        plot_args = self._build_plot_arguments(
            final_knesset_filter, faction_display_map, connect_func, selected_chart
        )
        
        # Generate and display plot
        with st.spinner(f"Generating '{selected_chart}'..."):
            try:
                figure = plot_function(**plot_args)
                if figure:
                    st.plotly_chart(figure, use_container_width=True)
                    SessionStateManager.set_plot_figure(figure)
            except Exception as e:
                self.logger.error(f"Error displaying plot '{selected_chart}': {e}", exc_info=True)
                st.error(f"An error occurred while generating the plot: {ui_utils.format_exception_for_ui(sys.exc_info())}")

    def _get_final_knesset_filter(self, selected_chart: str) -> Optional[List[int]]:
        """
        Determine the final Knesset filter based on current selection.
        
        Args:
            selected_chart: Currently selected chart name
            
        Returns:
            Knesset filter list, None for all Knessets, or False if invalid
        """
        current_selection = SessionStateManager.get_plot_main_knesset_selection()
        can_show_all_knessets = selected_chart in ["Queries by Time Period", "Agenda Items by Time Period"]
        
        if current_selection == "All Knessets (Color Coded)" and can_show_all_knessets:
            self.logger.info(f"Plot '{selected_chart}': Showing all Knessets (color coded).")
            return None
        elif current_selection and current_selection != "":
            try:
                final_knesset_filter = [int(current_selection)]
                self.logger.info(f"Plot '{selected_chart}': Using main area Knesset selection: {final_knesset_filter}")
                return final_knesset_filter
            except ValueError:
                st.error(f"Invalid Knesset number selected: {current_selection}")
                return False
        else:
            return False

    def _build_plot_arguments(
        self, 
        final_knesset_filter: Optional[List[int]], 
        faction_display_map: Dict[str, int],
        connect_func: Callable,
        selected_chart: str
    ) -> Dict[str, Any]:
        """
        Build the arguments dictionary for plot function calls.
        
        Args:
            final_knesset_filter: Resolved Knesset filter
            faction_display_map: Mapping from faction display names to IDs
            connect_func: Function to create database connections
            selected_chart: Currently selected chart name
            
        Returns:
            Dictionary of plot function arguments
        """
        faction_filter = SessionStateManager.get_faction_filter()
        plot_args = {
            "db_path": self.db_path,
            "connect_func": connect_func,
            "logger_obj": self.logger,
            "knesset_filter": final_knesset_filter,
            "faction_filter": [
                faction_display_map[name] for name in faction_filter 
                if name in faction_display_map
            ]
        }
        
        # Add chart-specific arguments
        if selected_chart in ["Queries by Time Period", "Agenda Items by Time Period"]:
            plot_args["aggregation_level"] = SessionStateManager.get_plot_aggregation_level()
            plot_args["show_average_line"] = SessionStateManager.get_plot_show_average_line()
        elif selected_chart == "Query Status Description with Faction Breakdown (Single Knesset)":
            # Convert dates to string format if they exist
            start_date = SessionStateManager.get_plot_start_date()
            end_date = SessionStateManager.get_plot_end_date()
            plot_args["start_date"] = start_date.strftime('%Y-%m-%d') if start_date else None
            plot_args["end_date"] = end_date.strftime('%Y-%m-%d') if end_date else None
        
        return plot_args