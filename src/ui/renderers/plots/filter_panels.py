"""
Filter panel components for plots page.

This module provides reusable filter panel components for different chart types:
- Query filters (type, status)
- Agenda filters (session type, status)
- Bill filters (origin)
- Collaboration filters (thresholds, options)
"""

import logging
from pathlib import Path
from typing import Dict, List

import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query


class PlotFilterPanels:
    """Provides filter panel rendering for different chart types."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        """
        Initialize the filter panels.

        Args:
            db_path: Path to the database
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.logger = logger

    def render_advanced_filters(self, selected_chart: str) -> None:
        """
        Render advanced filters specific to each chart type.

        Args:
            selected_chart: Currently selected chart name
        """
        if not selected_chart:
            return

        # Bill-specific filters (only private/governmental filter)
        if "Bill" in selected_chart or "Bills" in selected_chart:
            st.markdown("**Bill Filters:**")
            self.render_bill_filters(selected_chart)
            return

        # Query-specific filters
        if "Query" in selected_chart or "Queries" in selected_chart:
            st.markdown("**Additional Filters:**")
            self.render_query_filters(selected_chart)

        # Agenda-specific filters
        elif "Agenda" in selected_chart or "Agendas" in selected_chart:
            st.markdown("**Additional Filters:**")
            self.render_agenda_filters(selected_chart)

        # Network/Collaboration-specific filters (exclude Faction Collaboration Network)
        elif ("Collaboration" in selected_chart or "Network" in selected_chart) and "Faction Collaboration Network" not in selected_chart:
            st.markdown("**Collaboration Filters:**")
            self.render_collaboration_filters(selected_chart)

    def render_query_filters(self, selected_chart: str) -> None:
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

    def render_agenda_filters(self, selected_chart: str) -> None:
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

    def render_collaboration_filters(self, selected_chart: str) -> None:
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
                    value=int(st.session_state.get('plot_min_collaborations', 3)),
                    key=f"min_collaborations_{selected_chart.replace(' ', '_')}",
                    help="Minimum number of collaborative bills to show faction-to-faction relationship"
                )
                st.session_state.plot_min_collaborations = min_collaborations

            with col2:
                # Show solo bills toggle
                show_solo_bills = st.checkbox(
                    "Show Solo Bills",
                    value=bool(st.session_state.get('plot_show_solo_bills', True)),
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
                    value=int(st.session_state.get('plot_min_total_bills', 1)),
                    key=f"min_total_bills_{selected_chart.replace(' ', '_')}",
                    help="Minimum total bills for a faction to be included in matrix"
                )
                st.session_state.plot_min_total_bills = min_total_bills

        # General collaboration filters for other network charts (excluding Faction Collaboration Network)
        elif "Faction Collaboration Network" not in selected_chart:
            col1, col2 = st.columns(2)

            with col1:
                # Minimum collaborations threshold
                min_collaborations = st.number_input(
                    "Min. Collaborations",
                    min_value=1,
                    max_value=20,
                    value=int(st.session_state.get('plot_min_collaborations', 3 if "Matrix" in selected_chart else 5)),
                    key=f"min_collaborations_{selected_chart.replace(' ', '_')}",
                    help="Minimum number of collaborative bills to display relationship"
                )
                st.session_state.plot_min_collaborations = min_collaborations

            with col2:
                st.write("")  # Spacer for future additional filters

    def render_bill_filters(self, selected_chart: str) -> None:
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

    def populate_filter_options(self) -> None:
        """Populate available filter options from the database using cache."""
        if not self.db_path.exists():
            return

        try:
            # Use cached function to avoid repeated queries
            filter_options = self._fetch_filter_options_cached(str(self.db_path))

            # Populate session state from cached results
            if 'query_types' in filter_options:
                st.session_state.available_query_types = filter_options['query_types']
            if 'query_statuses' in filter_options:
                st.session_state.available_query_statuses = filter_options['query_statuses']
            if 'session_types' in filter_options:
                st.session_state.available_session_types = filter_options['session_types']
            if 'agenda_statuses' in filter_options:
                st.session_state.available_agenda_statuses = filter_options['agenda_statuses']
            if 'bill_types' in filter_options:
                st.session_state.available_bill_types = filter_options['bill_types']
            if 'bill_statuses' in filter_options:
                st.session_state.available_bill_statuses = filter_options['bill_statuses']

        except Exception as e:
            self.logger.error(f"Error populating filter options: {e}", exc_info=True)

    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def _fetch_filter_options_cached(_db_path: str) -> Dict[str, List[str]]:
        """Cached filter options fetching to avoid repeated database queries."""
        filter_options = {}
        db_path = Path(_db_path)

        if not db_path.exists():
            return filter_options

        try:
            logger = logging.getLogger("knesset.ui.filter_panels")
            with get_db_connection(db_path, read_only=True, logger_obj=logger) as con:
                # Query types
                query_types_query = "SELECT DISTINCT TypeDesc FROM KNS_Query WHERE TypeDesc IS NOT NULL ORDER BY TypeDesc"
                query_types_df = safe_execute_query(con, query_types_query, logger)
                if not query_types_df.empty:
                    filter_options['query_types'] = query_types_df['TypeDesc'].tolist()

                # Query statuses
                query_status_query = """
                    SELECT DISTINCT s."Desc" as StatusDesc
                    FROM KNS_Query q
                    JOIN KNS_Status s ON q.StatusID = s.StatusID
                    WHERE s."Desc" IS NOT NULL
                    ORDER BY s."Desc"
                """
                query_status_df = safe_execute_query(con, query_status_query, logger)
                if not query_status_df.empty:
                    filter_options['query_statuses'] = query_status_df['StatusDesc'].tolist()

                # Agenda session types
                try:
                    session_types_query = "SELECT DISTINCT SubTypeDesc FROM KNS_Agenda WHERE SubTypeDesc IS NOT NULL ORDER BY SubTypeDesc"
                    session_types_df = safe_execute_query(con, session_types_query, logger)
                    if not session_types_df.empty:
                        filter_options['session_types'] = session_types_df['SubTypeDesc'].tolist()
                except Exception as e:
                    logger.debug(f"Could not fetch agenda session types: {e}")

                # Agenda statuses
                try:
                    agenda_status_query = """
                        SELECT DISTINCT s."Desc" as StatusDesc
                        FROM KNS_Agenda a
                        JOIN KNS_Status s ON a.StatusID = s.StatusID
                        WHERE s."Desc" IS NOT NULL
                        ORDER BY s."Desc"
                    """
                    agenda_status_df = safe_execute_query(con, agenda_status_query, logger)
                    if not agenda_status_df.empty:
                        filter_options['agenda_statuses'] = agenda_status_df['StatusDesc'].tolist()
                except Exception as e:
                    logger.debug(f"Could not fetch agenda statuses: {e}")

                # Bill types
                try:
                    bill_types_query = "SELECT DISTINCT SubTypeDesc FROM KNS_Bill WHERE SubTypeDesc IS NOT NULL ORDER BY SubTypeDesc"
                    bill_types_df = safe_execute_query(con, bill_types_query, logger)
                    if not bill_types_df.empty:
                        filter_options['bill_types'] = bill_types_df['SubTypeDesc'].tolist()
                except Exception as e:
                    logger.debug(f"Could not fetch bill types: {e}")

                # Bill statuses
                try:
                    bill_status_query = """
                        SELECT DISTINCT s."Desc" as StatusDesc
                        FROM KNS_Bill b
                        JOIN KNS_Status s ON b.StatusID = s.StatusID
                        WHERE s."Desc" IS NOT NULL
                        ORDER BY s."Desc"
                    """
                    bill_status_df = safe_execute_query(con, bill_status_query, logger)
                    if not bill_status_df.empty:
                        filter_options['bill_statuses'] = bill_status_df['StatusDesc'].tolist()
                except Exception as e:
                    logger.debug(f"Could not fetch bill statuses: {e}")

        except Exception as e:
            logger.error(f"Error fetching filter options: {e}", exc_info=True)

        return filter_options
