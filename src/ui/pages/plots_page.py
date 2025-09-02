"""
Predefined visualizations page UI components.

This module handles the rendering of the plots selection interface
and plot generation with proper parameter handling.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import streamlit as st

import ui.ui_utils as ui_utils
from ui.state.session_manager import SessionStateManager


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
        connect_func: Callable,
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
            st.warning(
                "Database not found. Visualizations cannot be generated. Please run a data refresh."
            )
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
            available_plots,
            selected_topic,
            selected_chart,
            faction_display_map,
            connect_func,
        )

    def _render_topic_selection(
        self, available_plots: Dict[str, Dict[str, Callable]]
    ) -> str:
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
            if current_selected_topic in plot_topic_options
            else 0
        )

        selected_topic_widget = st.selectbox(
            "1. Choose Plot Topic:",
            options=plot_topic_options,
            index=topic_select_default_index,
            key="sb_selected_plot_topic_widget",
        )

        # Handle topic change
        if selected_topic_widget != current_selected_topic:
            SessionStateManager.reset_plot_state(keep_topic=False)
            SessionStateManager.set_plot_selection(selected_topic_widget, "")
            st.rerun()

        return selected_topic_widget

    def _render_chart_selection(
        self, available_plots: Dict[str, Dict[str, Callable]], selected_topic: str
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
            if current_selected_chart in chart_options_for_topic
            else 0
        )

        selected_chart_widget = st.selectbox(
            f"2. Choose Visualization for '{selected_topic}':",
            options=chart_options_for_topic,
            index=chart_select_default_index,
            key=f"sb_selected_chart_for_topic_{selected_topic.replace(' ', '_')}",
        )

        # Handle chart change
        if selected_chart_widget != current_selected_chart:
            # Reset plot-specific state but keep topic and chart selection
            st.session_state.plot_aggregation_level = "Yearly"
            st.session_state.plot_show_average_line = False
            st.session_state.plot_start_date = None
            st.session_state.plot_end_date = None
            SessionStateManager.set_plot_selection(
                selected_topic, selected_chart_widget
            )
            st.rerun()

        return selected_chart_widget

    def _render_plot_options(
        self, selected_chart: str, knesset_options: List[str]
    ) -> None:
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
        can_show_all_knessets = selected_chart in [
            "Queries by Time Period",
            "Agenda Items by Time Period",
        ]

        if (
            can_show_all_knessets
            and "All Knessets (Color Coded)" not in plot_knesset_options
        ):
            plot_knesset_options.insert(1, "All Knessets (Color Coded)")

        # Get current selection
        current_main_knesset_selection = (
            SessionStateManager.get_plot_main_knesset_selection()
        )
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
                selected_chart,
                plot_knesset_options,
                current_main_knesset_selection,
                can_show_all_knessets,
            )

        # Add date picker for specific plots
        if selected_chart in [
            "Query Status Description with Faction Breakdown (Single Knesset)",
            "Query Types Distribution",
        ]:
            self._render_date_filter_options(selected_chart)
            
        # Populate available filter options from database
        self._populate_filter_options()
        
        # Add advanced filters for all charts
        self._render_advanced_filters(selected_chart)

    def _render_time_period_plot_options(
        self,
        selected_chart: str,
        plot_knesset_options: List[str],
        current_selection: str,
    ) -> None:
        """Render options for time period plots with aggregation controls."""
        knesset_select_default_index = (
            plot_knesset_options.index(current_selection)
            if current_selection in plot_knesset_options
            else 0
        )

        aggregation_level = SessionStateManager.get_plot_aggregation_level()
        show_average_line = SessionStateManager.get_plot_show_average_line()

        col_knesset_select, col_agg_select, col_avg_line = st.columns([2, 1, 1])

        with col_knesset_select:
            selected_knesset_val = st.selectbox(
                "3. Select Knesset for Plot:",
                options=plot_knesset_options,
                index=knesset_select_default_index,
                key=f"plot_main_knesset_selector_tp_{selected_chart.replace(' ', '_')}",
            )

        with col_agg_select:
            st.session_state.plot_aggregation_level = st.selectbox(
                "Aggregate:",
                options=["Yearly", "Monthly", "Quarterly"],
                index=["Yearly", "Monthly", "Quarterly"].index(aggregation_level),
                key=f"agg_level_{selected_chart.replace(' ', '_')}",
            )

        with col_avg_line:
            st.session_state.plot_show_average_line = st.checkbox(
                "Avg Line",
                value=show_average_line,
                key=f"avg_line_{selected_chart.replace(' ', '_')}",
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
        can_show_all_knessets: bool,
    ) -> None:
        """Render options for single Knesset plots."""
        # Filter out "All Knessets" option for single Knesset plots
        options_for_single_knesset_plot = [
            opt
            for opt in plot_knesset_options
            if opt != "All Knessets (Color Coded)" and opt != ""
        ]

        if (
            current_selection not in options_for_single_knesset_plot
            and current_selection != ""
        ):
            current_selection = ""
            SessionStateManager.set_plot_knesset_selection("")

        effective_options_single = [""] + options_for_single_knesset_plot
        single_knesset_default_idx = (
            effective_options_single.index(current_selection)
            if current_selection in effective_options_single
            else 0
        )

        selected_knesset_val = st.selectbox(
            "3. Select Knesset for Plot:",
            options=effective_options_single,
            index=single_knesset_default_idx,
            key=f"plot_main_knesset_selector_single_{selected_chart.replace(' ', '_')}",
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
                help="Filter queries from this date onwards",
            )

        with col_end_date:
            st.session_state.plot_end_date = st.date_input(
                "End Date (optional)",
                value=SessionStateManager.get_plot_end_date(),
                key=f"end_date_{selected_chart.replace(' ', '_')}",
                help="Filter queries up to this date",
            )

    def _render_advanced_filters(self, selected_chart: str) -> None:
        """Render advanced filters specific to each chart type."""
        if not selected_chart:
            return
        
        # Bill-specific filters (only private/governmental filter)
        if "Bill" in selected_chart or "Bills" in selected_chart:
            st.markdown("**Bill Filters:**")
            self._render_bill_filters(selected_chart)
            return
            
        # Query-specific filters
        if "Query" in selected_chart or "Queries" in selected_chart:
            st.markdown("**Additional Filters:**")
            self._render_query_filters(selected_chart)
        
        # Agenda-specific filters  
        elif "Agenda" in selected_chart or "Agendas" in selected_chart:
            st.markdown("**Additional Filters:**")
            self._render_agenda_filters(selected_chart)
            
        # Network/Collaboration-specific filters
        elif "Collaboration" in selected_chart or "Network" in selected_chart:
            st.markdown("**Collaboration Filters:**")
            self._render_collaboration_filters(selected_chart)

    def _render_query_filters(self, selected_chart: str) -> None:
        """Render query-specific filter options."""
        col1, col2 = st.columns(2)
        
        with col1:
            # Query Type filter
            query_types = st.session_state.get('available_query_types', [
                'שאילתה', 'בקשה לקדימות דיון', 'שאילתה לשם דיון בכנסת', 
                'הצעה לסדר היום', 'הודעה על בעיית התרחשות'
            ])
            selected_query_types = st.multiselect(
                "Query Types",
                options=query_types,
                default=st.session_state.get('plot_query_type_filter', []),
                key=f"query_type_filter_{selected_chart.replace(' ', '_')}",
                help="Filter by specific query types"
            )
            st.session_state.plot_query_type_filter = selected_query_types
            
        with col2:
            # Query Status filter
            query_statuses = st.session_state.get('available_query_statuses', [
                'הוגשה', 'נתקבלה תשובה', 'טרם נתקבלה תשובה', 'נדחתה'
            ])
            selected_query_statuses = st.multiselect(
                "Query Status",
                options=query_statuses,
                default=st.session_state.get('plot_query_status_filter', []),
                key=f"query_status_filter_{selected_chart.replace(' ', '_')}",
                help="Filter by query answer status"
            )
            st.session_state.plot_query_status_filter = selected_query_statuses

    def _render_agenda_filters(self, selected_chart: str) -> None:
        """Render agenda-specific filter options."""
        col1, col2 = st.columns(2)
        
        with col1:
            # Agenda Session Type filter
            session_types = st.session_state.get('available_session_types', [
                'מליאה', 'ועדה', 'ועדת משנה'
            ])
            selected_session_types = st.multiselect(
                "Session Types",
                options=session_types,
                default=st.session_state.get('plot_session_type_filter', []),
                key=f"session_type_filter_{selected_chart.replace(' ', '_')}",
                help="Filter by session type"
            )
            st.session_state.plot_session_type_filter = selected_session_types
            
        with col2:
            # Agenda Status filter
            agenda_statuses = st.session_state.get('available_agenda_statuses', [
                'קיים', 'בוטל', 'נדחה'
            ])
            selected_agenda_statuses = st.multiselect(
                "Agenda Status",
                options=agenda_statuses,
                default=st.session_state.get('plot_agenda_status_filter', []),
                key=f"agenda_status_filter_{selected_chart.replace(' ', '_')}",
                help="Filter by agenda status"
            )
            st.session_state.plot_agenda_status_filter = selected_agenda_statuses

    def _render_collaboration_filters(self, selected_chart: str) -> None:
        """Render collaboration/network-specific filter options."""
        
        # Specific filters for Faction Collaboration Matrix
        if "Faction Collaboration Matrix" in selected_chart:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Minimum collaborations threshold
                min_collaborations = st.number_input(
                    "Min. Collaborations",
                    min_value=1,
                    max_value=20,
                    value=st.session_state.get('plot_min_collaborations', 3),
                    key=f"min_collaborations_{selected_chart.replace(' ', '_')}",
                    help="Minimum number of collaborative bills to show faction-to-faction relationship"
                )
                st.session_state.plot_min_collaborations = min_collaborations
                
            with col2:
                # Show solo bills toggle
                show_solo_bills = st.checkbox(
                    "Show Solo Bills",
                    value=st.session_state.get('plot_show_solo_bills', True),
                    key=f"show_solo_bills_{selected_chart.replace(' ', '_')}",
                    help="Display solo bills (bills with only 1 initiator) on the diagonal"
                )
                st.session_state.plot_show_solo_bills = show_solo_bills
                
            with col3:
                # Minimum total bills for faction inclusion
                min_total_bills = st.number_input(
                    "Min. Total Bills",
                    min_value=1,
                    max_value=50,
                    value=st.session_state.get('plot_min_total_bills', 1),
                    key=f"min_total_bills_{selected_chart.replace(' ', '_')}",
                    help="Minimum total bills for a faction to be included in matrix"
                )
                st.session_state.plot_min_total_bills = min_total_bills
        
        # General collaboration filters for other network charts
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                # Minimum collaborations threshold
                min_collaborations = st.number_input(
                    "Min. Collaborations",
                    min_value=1,
                    max_value=20,
                    value=st.session_state.get('plot_min_collaborations', 3 if "Matrix" in selected_chart else 5),
                    key=f"min_collaborations_{selected_chart.replace(' ', '_')}",
                    help="Minimum number of collaborative bills to display relationship"
                )
                st.session_state.plot_min_collaborations = min_collaborations
                
            with col2:
                st.write("")  # Spacer for future additional filters

    def _render_bill_filters(self, selected_chart: str) -> None:
        """Render bill-specific filter options."""
        # Bill Origin filter (Private vs Governmental)
        bill_origin_options = ["All Bills", "Private Bills Only", "Governmental Bills Only"]
        selected_bill_origin = st.selectbox(
            "Bill Origin",
            options=bill_origin_options,
            index=bill_origin_options.index(st.session_state.get('plot_bill_origin_filter', 'All Bills')),
            key=f"bill_origin_filter_{selected_chart.replace(' ', '_')}",
            help="Filter bills by their origin: Private (initiated by MKs) or Governmental (initiated by government)"
        )
        st.session_state.plot_bill_origin_filter = selected_bill_origin

    def _populate_filter_options(self) -> None:
        """Populate available filter options from the database."""
        if not self.db_path.exists():
            return
            
        try:
            from backend.connection_manager import get_db_connection, safe_execute_query
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                # Query types
                query_types_query = "SELECT DISTINCT TypeDesc FROM KNS_Query WHERE TypeDesc IS NOT NULL ORDER BY TypeDesc"
                query_types_df = safe_execute_query(con, query_types_query, self.logger)
                if not query_types_df.empty:
                    st.session_state.available_query_types = query_types_df['TypeDesc'].tolist()
                
                # Query statuses - join with KNS_Status table
                query_status_query = """
                    SELECT DISTINCT s."Desc" as StatusDesc 
                    FROM KNS_Query q 
                    JOIN KNS_Status s ON q.StatusID = s.StatusID 
                    WHERE s."Desc" IS NOT NULL 
                    ORDER BY s."Desc"
                """
                query_status_df = safe_execute_query(con, query_status_query, self.logger)
                if not query_status_df.empty:
                    st.session_state.available_query_statuses = query_status_df['StatusDesc'].tolist()
                
                # Agenda session types - use SubTypeDesc instead of SessionType
                try:
                    session_types_query = "SELECT DISTINCT SubTypeDesc FROM KNS_Agenda WHERE SubTypeDesc IS NOT NULL ORDER BY SubTypeDesc"
                    session_types_df = safe_execute_query(con, session_types_query, self.logger)
                    if not session_types_df.empty:
                        st.session_state.available_session_types = session_types_df['SubTypeDesc'].tolist()
                except:
                    pass  # Table might not exist or column might be different
                
                # Agenda statuses - join with KNS_Status table
                try:
                    agenda_status_query = """
                        SELECT DISTINCT s."Desc" as StatusDesc 
                        FROM KNS_Agenda a 
                        JOIN KNS_Status s ON a.StatusID = s.StatusID 
                        WHERE s."Desc" IS NOT NULL 
                        ORDER BY s."Desc"
                    """
                    agenda_status_df = safe_execute_query(con, agenda_status_query, self.logger)
                    if not agenda_status_df.empty:
                        st.session_state.available_agenda_statuses = agenda_status_df['StatusDesc'].tolist()
                except:
                    pass
                
                # Bill types - use SubTypeDesc instead of BillTypeDesc
                try:
                    bill_types_query = "SELECT DISTINCT SubTypeDesc FROM KNS_Bill WHERE SubTypeDesc IS NOT NULL ORDER BY SubTypeDesc"
                    bill_types_df = safe_execute_query(con, bill_types_query, self.logger)
                    if not bill_types_df.empty:
                        st.session_state.available_bill_types = bill_types_df['SubTypeDesc'].tolist()
                except:
                    pass
                
                # Bill statuses - join with KNS_Status table
                try:
                    bill_status_query = """
                        SELECT DISTINCT s."Desc" as StatusDesc 
                        FROM KNS_Bill b 
                        JOIN KNS_Status s ON b.StatusID = s.StatusID 
                        WHERE s."Desc" IS NOT NULL 
                        ORDER BY s."Desc"
                    """
                    bill_status_df = safe_execute_query(con, bill_status_query, self.logger)
                    if not bill_status_df.empty:
                        st.session_state.available_bill_statuses = bill_status_df['StatusDesc'].tolist()
                except:
                    pass
                    
        except Exception as e:
            self.logger.error(f"Error populating filter options: {e}", exc_info=True)

    def _generate_and_display_plot(
        self,
        available_plots: Dict[str, Dict[str, Callable]],
        selected_topic: str,
        selected_chart: str,
        faction_display_map: Dict[str, int],
        connect_func: Callable,
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
            requires_single_knesset = (
                "(Single Knesset)" in selected_chart
                or selected_chart
                not in ["Queries by Time Period", "Agenda Items by Time Period"]
            )
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
                self.logger.error(
                    f"Error displaying plot '{selected_chart}': {e}", exc_info=True
                )
                st.error(
                    f"An error occurred while generating the plot: {ui_utils.format_exception_for_ui(sys.exc_info())}"
                )

    def _get_final_knesset_filter(self, selected_chart: str) -> Optional[List[int]]:
        """
        Determine the final Knesset filter based on current selection.

        Args:
            selected_chart: Currently selected chart name

        Returns:
            Knesset filter list, None for all Knessets, or False if invalid
        """
        current_selection = SessionStateManager.get_plot_main_knesset_selection()
        can_show_all_knessets = selected_chart in [
            "Queries by Time Period",
            "Agenda Items by Time Period",
        ]

        if current_selection == "All Knessets (Color Coded)" and can_show_all_knessets:
            self.logger.info(
                f"Plot '{selected_chart}': Showing all Knessets (color coded)."
            )
            return None
        elif current_selection and current_selection != "":
            try:
                final_knesset_filter = [int(current_selection)]
                self.logger.info(
                    f"Plot '{selected_chart}': Using main area Knesset selection: {final_knesset_filter}"
                )
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
        selected_chart: str,
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
                faction_display_map[name]
                for name in faction_filter
                if name in faction_display_map
            ],
        }

        # Add chart-specific arguments
        if selected_chart in ["Queries by Time Period", "Agenda Items by Time Period"]:
            plot_args["aggregation_level"] = (
                SessionStateManager.get_plot_aggregation_level()
            )
            plot_args["show_average_line"] = (
                SessionStateManager.get_plot_show_average_line()
            )
        elif selected_chart in [
            "Query Status Description with Faction Breakdown (Single Knesset)",
            "Query Types Distribution",
        ]:
            # Convert dates to string format if they exist
            start_date = SessionStateManager.get_plot_start_date()
            end_date = SessionStateManager.get_plot_end_date()
            plot_args["start_date"] = (
                start_date.strftime("%Y-%m-%d") if start_date else None
            )
            plot_args["end_date"] = end_date.strftime("%Y-%m-%d") if end_date else None

        # Add advanced filters based on chart type
        if "Query" in selected_chart or "Queries" in selected_chart:
            plot_args["query_type_filter"] = st.session_state.get('plot_query_type_filter', [])
            plot_args["query_status_filter"] = st.session_state.get('plot_query_status_filter', [])
            
        elif "Agenda" in selected_chart or "Agendas" in selected_chart:
            plot_args["session_type_filter"] = st.session_state.get('plot_session_type_filter', [])
            plot_args["agenda_status_filter"] = st.session_state.get('plot_agenda_status_filter', [])
            
        elif "Bill" in selected_chart or "Bills" in selected_chart:
            plot_args["bill_origin_filter"] = st.session_state.get('plot_bill_origin_filter', 'All Bills')
            
        elif "Collaboration" in selected_chart or "Network" in selected_chart:
            # Add collaboration-specific parameters
            plot_args["min_collaborations"] = st.session_state.get('plot_min_collaborations', 3)
            if "Faction Collaboration Matrix" in selected_chart:
                plot_args["show_solo_bills"] = st.session_state.get('plot_show_solo_bills', True)
                plot_args["min_total_bills"] = st.session_state.get('plot_min_total_bills', 1)

        return plot_args
