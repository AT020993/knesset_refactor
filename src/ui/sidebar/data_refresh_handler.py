"""
Data refresh handler for sidebar.

Handles the logic for refreshing database tables from the Knesset API.
"""

import logging
import time
from pathlib import Path
from typing import Callable

import streamlit as st

from config.database import DatabaseConfig

# Use canonical source for table list
TABLES = DatabaseConfig.TABLES


# Constants
SELECT_ALL_TABLES_OPTION = "🔄 Select/Deselect All Tables"


def handle_data_refresh_button_click(
    db_path: Path,
    ui_logger: logging.Logger,
    format_exc_func: Callable[[], str],
) -> None:
    """Handles the logic for the 'Refresh Selected Data' button click.

    Args:
        db_path: Path to the database
        ui_logger: Logger instance
        format_exc_func: Function to format exceptions
    """
    if st.session_state.get("data_refresh_process_running", False):
        st.sidebar.warning("Refresh process is already running.")
        return

    # Read directly from the widget state (more reliable than intermediate sync)
    all_tables_selected = st.session_state.get("ms_tables_to_refresh_widget", [])

    # Also check the synced state as fallback
    if not all_tables_selected:
        all_tables_selected = st.session_state.get("ms_tables_to_refresh", [])

    ui_logger.info(f"Data refresh button clicked. Selected tables: {all_tables_selected}")

    if not all_tables_selected:
        st.sidebar.warning("⚠️ No tables selected. Please select tables from the dropdown above.")
        return

    tables_to_run = [t for t in all_tables_selected if t != SELECT_ALL_TABLES_OPTION]
    if SELECT_ALL_TABLES_OPTION in all_tables_selected or not tables_to_run:
        tables_to_run = TABLES

    # Show immediate feedback that button was clicked
    st.sidebar.info(f"🚀 Starting refresh for {len(tables_to_run)} tables...")
    if not tables_to_run:
        st.sidebar.info("No tables are defined or selected for refresh.")
        return

    st.session_state.data_refresh_process_running = True
    start_time = time.time()
    ui_logger.info(f"Starting data refresh for tables: {tables_to_run}")

    def _format_time(seconds: float) -> str:
        """Format seconds into human-readable time string."""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m"

    try:
        # Show spinner while downloading (progress callbacks don't work in threaded mode)
        with st.sidebar.status(
            f"🔄 Downloading {len(tables_to_run)} tables from Knesset API...",
            expanded=True
        ) as status:
            status.write(f"Tables: {', '.join(tables_to_run[:5])}{'...' if len(tables_to_run) > 5 else ''}")
            status.write("⏳ This may take several minutes for large tables...")

            ui_logger.info("Starting data refresh using synchronous wrapper...")

            # Lazy import: DataRefreshService depends on aiohttp which may
            # not be installed on Streamlit Cloud
            from data.services.data_refresh_service import DataRefreshService

            # Use the synchronous wrapper (handles Streamlit's event loop)
            refresh_service = DataRefreshService(db_path=db_path, logger_obj=ui_logger)
            ui_logger.info(f"DataRefreshService created. Calling refresh_tables_sync for {len(tables_to_run)} tables")

            result = refresh_service.refresh_tables_sync(tables=tables_to_run)

            elapsed = time.time() - start_time
            ui_logger.info(f"refresh_tables_sync completed with result: {result} in {_format_time(elapsed)}")

            if result:
                status.update(label="✅ Download complete!", state="complete", expanded=False)
            else:
                status.update(label="⚠️ Download completed with errors", state="error", expanded=True)

        if result:
            st.sidebar.success(f"✅ Data refresh completed in {_format_time(elapsed)}!")
        else:
            st.sidebar.warning("⚠️ Data refresh completed with some errors. Check logs for details.")

        refresh_succeeded = True
    except Exception as e:
        ui_logger.error(f"❌ Data Refresh Error: {e}", exc_info=True)
        st.sidebar.error(f"❌ Data Refresh Error: {e}")
        st.sidebar.code(f"Error: {str(e)}\n\nTraceback:\n{format_exc_func()}")
        refresh_succeeded = False
    finally:
        st.session_state.data_refresh_process_running = False

        # Always clear caches after refresh attempt (even on partial failure)
        # to ensure filter options and other cached data reflect current DB state
        st.cache_data.clear()
        st.cache_resource.clear()
        ui_logger.info("Cleared all Streamlit caches after data refresh")

        # Only trigger rerun on success to show updated UI
        if refresh_succeeded:
            st.rerun()


def handle_multiselect_change():
    """Handles the 'Select/Deselect All' logic for the tables multiselect."""
    current_selection = st.session_state.get("ms_tables_to_refresh_widget", [])
    is_select_all_currently_checked = st.session_state.get(
        "all_tables_selected_for_refresh_flag", False
    )

    # Case 1: "Select All" was just checked
    if (
        SELECT_ALL_TABLES_OPTION in current_selection
        and not is_select_all_currently_checked
    ):
        st.session_state.ms_tables_to_refresh = [SELECT_ALL_TABLES_OPTION] + TABLES
        st.session_state.all_tables_selected_for_refresh_flag = True
    # Case 2: "Select All" was just unchecked
    elif (
        SELECT_ALL_TABLES_OPTION not in current_selection
        and is_select_all_currently_checked
    ):
        st.session_state.ms_tables_to_refresh = []
        st.session_state.all_tables_selected_for_refresh_flag = False
    # Case 3: Normal selection change
    else:
        st.session_state.ms_tables_to_refresh = current_selection


def get_tables_multiselect_options() -> list:
    """Get options for the tables multiselect widget."""
    return [SELECT_ALL_TABLES_OPTION] + TABLES
