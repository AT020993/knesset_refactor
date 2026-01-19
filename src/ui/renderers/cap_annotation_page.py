"""
CAP Bill Annotation Page

This module provides a Streamlit interface for researchers to annotate
bills according to the Democratic Erosion codebook.

Features:
- Multi-user authentication with role-based access
- Bill queue showing uncoded bills
- Annotation form with CAP code selection
- Direction coding (+1/-1/0)
- Progress dashboard
- Export functionality
- Admin panel for user management (admin role only)

This is the main orchestrator that coordinates the modular components:
- CAPAuthHandler: Authentication logic (multi-user)
- CAPStatsRenderer: Statistics dashboard
- CAPFormRenderer: Annotation forms
- CAPCodedBillsRenderer: View/edit existing annotations
- CAPAdminRenderer: Admin panel for user management
"""

import logging
from pathlib import Path
from typing import Optional

import streamlit as st

from ui.services.cap_service import get_cap_service
from ui.renderers.cap import (
    CAPAuthHandler,
    CAPStatsRenderer,
    CAPFormRenderer,
    CAPCodedBillsRenderer,
    CAPAdminRenderer,
)


class CAPAnnotationPageRenderer:
    """Orchestrator for the CAP annotation page."""

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the renderer."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
        self._service = None
        self._auth_handler = None

        # Initialize component renderers (lazy)
        self._stats_renderer = None
        self._form_renderer = None
        self._coded_bills_renderer = None
        self._admin_renderer = None

    @property
    def service(self):
        """Get or create the CAP service."""
        if self._service is None:
            self._service = get_cap_service(self.db_path, self.logger)
        return self._service

    @property
    def auth_handler(self) -> CAPAuthHandler:
        """Get or create the auth handler."""
        if self._auth_handler is None:
            self._auth_handler = CAPAuthHandler(self.db_path, self.logger)
        return self._auth_handler

    @property
    def stats_renderer(self) -> CAPStatsRenderer:
        """Get stats renderer."""
        if self._stats_renderer is None:
            self._stats_renderer = CAPStatsRenderer(self.service)
        return self._stats_renderer

    @property
    def form_renderer(self) -> CAPFormRenderer:
        """Get form renderer."""
        if self._form_renderer is None:
            self._form_renderer = CAPFormRenderer(
                self.service,
                self.logger,
                on_annotation_saved=self._clear_query_cache,
            )
        return self._form_renderer

    @property
    def coded_bills_renderer(self) -> CAPCodedBillsRenderer:
        """Get coded bills renderer."""
        if self._coded_bills_renderer is None:
            self._coded_bills_renderer = CAPCodedBillsRenderer(
                self.service,
                self.logger,
                on_annotation_changed=self._clear_query_cache,
            )
        return self._coded_bills_renderer

    @property
    def admin_renderer(self) -> CAPAdminRenderer:
        """Get admin renderer."""
        if self._admin_renderer is None:
            self._admin_renderer = CAPAdminRenderer(self.db_path, self.logger)
        return self._admin_renderer

    @staticmethod
    def _clear_query_cache():
        """Clear query-related caches when annotations change."""
        # Clear session state query results so predefined queries fetch fresh data
        if "query_results_df" in st.session_state:
            del st.session_state["query_results_df"]
        if "show_query_results" in st.session_state:
            st.session_state["show_query_results"] = False
        # Clear Streamlit's data cache
        st.cache_data.clear()

    def render_cap_annotation_section(self):
        """Main render method for the CAP annotation section."""
        st.header("🏛️ Democratic Bill Annotation")

        # Check if feature is enabled
        if not CAPAuthHandler.is_feature_enabled():
            CAPAuthHandler.render_disabled_message()
            return

        # Check authentication
        is_authenticated, researcher_name = CAPAuthHandler.check_authentication()

        if not is_authenticated:
            self.auth_handler.render_login_form()
            return

        # Show researcher info and logout
        CAPAuthHandler.render_user_info(researcher_name)

        # Initialize tables
        if "cap_tables_initialized" not in st.session_state:
            with st.spinner("Initializing annotation system..."):
                self.service.ensure_tables_exist()
                self.service.load_taxonomy_from_csv()
                st.session_state.cap_tables_initialized = True

        # Tab navigation with persistent state
        self._render_tab_navigation(researcher_name)

    def _render_tab_navigation(self, researcher_name: str):
        """Render tab navigation and content."""
        # Base tabs available to all users
        tab_options = [
            "📝 New Annotation",
            "🌐 Fetch from API",
            "📚 View Coded",
            "📊 Statistics",
        ]

        # Add admin tab for admins
        is_admin = CAPAuthHandler.is_admin()
        if is_admin:
            tab_options.append("👥 Admin")

        # Initialize tab state if not exists
        if "cap_active_tab" not in st.session_state:
            st.session_state.cap_active_tab = tab_options[0]

        # Ensure active tab is valid (in case user was admin and is no longer)
        if st.session_state.cap_active_tab not in tab_options:
            st.session_state.cap_active_tab = tab_options[0]

        # Radio button for tab selection (persists in session state)
        selected_tab = st.radio(
            "Navigation",
            options=tab_options,
            index=tab_options.index(st.session_state.cap_active_tab),
            horizontal=True,
            key="cap_tab_selector",
            label_visibility="collapsed",
        )

        # Update session state when tab changes
        st.session_state.cap_active_tab = selected_tab

        st.markdown("---")

        # Render selected tab content
        if selected_tab == "📝 New Annotation":
            self._render_new_annotation_tab(researcher_name)
        elif selected_tab == "🌐 Fetch from API":
            self.form_renderer.render_api_fetch_section(researcher_name)
        elif selected_tab == "📚 View Coded":
            self.coded_bills_renderer.render_coded_bills_view()
        elif selected_tab == "📊 Statistics":
            self.stats_renderer.render_stats_dashboard()
        elif selected_tab == "👥 Admin" and is_admin:
            self.admin_renderer.render_admin_panel()

    def _render_new_annotation_tab(self, researcher_name: str):
        """Render the new annotation tab."""
        # Bill queue and annotation form
        bill_id, submission_date = self.form_renderer.render_bill_queue()

        if bill_id:
            st.markdown("---")
            success = self.form_renderer.render_annotation_form(
                bill_id, researcher_name, submission_date or ""
            )
            if success:
                # Clear and refresh
                st.rerun()


def render_cap_page(db_path: Path, logger_obj: Optional[logging.Logger] = None):
    """Convenience function to render the CAP annotation page."""
    renderer = CAPAnnotationPageRenderer(db_path, logger_obj)
    renderer.render_cap_annotation_section()
