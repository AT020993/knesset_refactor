"""
Data refresh handler for sidebar.

Handles the logic for refreshing database tables from the Knesset API.
"""

import asyncio
import logging
from pathlib import Path

import streamlit as st

from config.database import DatabaseConfig
import backend.fetch_table as ft

# Use canonical source for table list
TABLES = DatabaseConfig.TABLES


# Constants
SELECT_ALL_TABLES_OPTION = "🔄 Select/Deselect All Tables"


def handle_data_refresh_button_click(
    db_path: Path, ui_logger: logging.Logger, format_exc_func: callable
):
    """Handles the logic for the 'Refresh Selected Data' button click.

    Args:
        db_path: Path to the database
        ui_logger: Logger instance
        format_exc_func: Function to format exceptions
    """
    if st.session_state.get("data_refresh_process_running", False):
        st.sidebar.warning("Refresh process is already running.")
        return

    all_tables_selected = st.session_state.get("ms_tables_to_refresh", [])
    if not all_tables_selected:
        st.sidebar.warning("No tables selected for refresh.")
        return

    tables_to_run = [t for t in all_tables_selected if t != SELECT_ALL_TABLES_OPTION]
    if SELECT_ALL_TABLES_OPTION in all_tables_selected or not tables_to_run:
        tables_to_run = TABLES
    if not tables_to_run:
        st.sidebar.info("No tables are defined or selected for refresh.")
        return

    st.session_state.data_refresh_process_running = True
    ui_logger.info(f"Starting data refresh for tables: {tables_to_run}")
    progress_bar_sidebar = st.sidebar.progress(0, text="Preparing refresh...")
    status_text_sidebar = st.sidebar.empty()
    status_text_sidebar.text("Initializing refresh...")

    def _sidebar_progress_cb(table_name_done: str, num_rows_fetched: int):
        """Progress callback for table refresh."""
        total_tables = len(tables_to_run)
        if "completed_tables_count" not in st.session_state:
            st.session_state.completed_tables_count = 0
        st.session_state.completed_tables_count += 1
        percentage = (st.session_state.completed_tables_count / total_tables) * 100
        message = (
            f"Fetched {num_rows_fetched} rows for {table_name_done}. "
            f"({st.session_state.completed_tables_count}/{total_tables} tables done)"
        )
        # Clamp percentage to 100 to prevent errors from state inconsistencies
        progress_bar_sidebar.progress(min(int(percentage), 100), text=message)
        status_text_sidebar.text(message)
        ui_logger.info(message)

    async def _refresh_async_wrapper(tables_list_async):
        """Async wrapper for table refresh."""
        st.session_state.completed_tables_count = 0
        await ft.refresh_tables(
            tables=tables_list_async, progress_cb=_sidebar_progress_cb, db_path=db_path
        )

    try:
        asyncio.run(_refresh_async_wrapper(tables_to_run))
        st.sidebar.success("Data refresh process complete!")
        status_text_sidebar.success("All selected tables refreshed successfully.")
        progress_bar_sidebar.progress(100, text="Refresh complete!")
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    except Exception as e:
        ui_logger.error(f"❌ Data Refresh Error: {e}", exc_info=True)
        st.sidebar.error(f"❌ Data Refresh Error: {e}")
        st.sidebar.code(f"Error: {str(e)}\n\nTraceback:\n{format_exc_func()}")
        status_text_sidebar.error(f"Error during refresh: {e}")
        progress_bar_sidebar.progress(0, text=f"Error: {e}")
    finally:
        st.session_state.data_refresh_process_running = False
        if "completed_tables_count" in st.session_state:
            del st.session_state.completed_tables_count


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
