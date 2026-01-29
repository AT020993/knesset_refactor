"""
Sidebar components for the Knesset data application.

This module provides the main sidebar display function and coordinates
the various sidebar handlers for data refresh, queries, and table exploration.

Handler implementations have been extracted to focused modules:
- data_refresh_handler.py: Data refresh button logic
- query_handler.py: Query execution logic
- table_explorer_handler.py: Table exploration logic

Configuration has been moved to:
- config/table_config.py: Table display names and mappings
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import streamlit as st

# Local imports from extracted modules
from config.table_config import (
    TABLE_DISPLAY_NAMES,
    TABLE_NAME_FROM_DISPLAY,
    get_table_display_name,
    get_table_name_from_display,
)
from ui.sidebar.data_refresh_handler import (
    handle_data_refresh_button_click,
    handle_multiselect_change,
    get_tables_multiselect_options,
)
from ui.sidebar.query_handler import handle_run_query_button_click
from ui.sidebar.table_explorer_handler import handle_explore_table_button_click


# Re-export for backward compatibility
__all__ = [
    "display_sidebar",
    "render_sync_status",
    "TABLE_DISPLAY_NAMES",
    "TABLE_NAME_FROM_DISPLAY",
    "get_table_display_name",
    "get_table_name_from_display",
]


def render_sync_status() -> None:
    """Render cloud sync status indicator in sidebar.

    Shows whether cloud sync is enabled and working. Called after user
    login in CAP annotation section.
    """
    try:
        from data.storage.credential_resolver import GCSCredentialResolver

        # Check if sync is configured
        bucket_name = GCSCredentialResolver.get_bucket_name()

        if not bucket_name:
            st.sidebar.caption("☁️ Cloud sync: Disabled")
            with st.sidebar.expander("Enable sync", expanded=False):
                st.caption(
                    "Set `GOOGLE_APPLICATION_CREDENTIALS` and `GCS_BUCKET_NAME` "
                    "environment variables, or configure Streamlit secrets."
                )
            return

        # Sync is configured
        st.sidebar.success("☁️ Cloud sync: Enabled")

    except ImportError:
        st.sidebar.caption("☁️ Cloud sync: Not available")
    except Exception as e:
        st.sidebar.caption(f"☁️ Cloud sync: Error - {e}")


def display_sidebar(
    db_path_arg: Path,
    exports_arg: Dict[str, Any],
    connect_func_arg: Callable[..., Any],
    get_db_table_list_func_arg: Callable[[], List[str]],
    get_table_columns_func_arg: Callable[[str], List[str]],
    get_filter_options_func_arg: Callable[[], Tuple[List[int], List[str]]],
    faction_display_map_arg: Dict[str, int],
    ui_logger_arg: logging.Logger,
    format_exc_func_arg: Callable[[Exception], str],
) -> None:
    """Renders all sidebar components.

    This is the main orchestrator that assembles and displays all sidebar
    sections including data management, query templates, table explorer,
    and global filters.

    Args:
        db_path_arg: Path to the database
        exports_arg: Dictionary of available query templates
        connect_func_arg: Database connection function
        get_db_table_list_func_arg: Function to get list of database tables
        get_table_columns_func_arg: Function to get columns for a table
        get_filter_options_func_arg: Function to get filter options
        faction_display_map_arg: Mapping from faction names to IDs
        ui_logger_arg: Logger instance
        format_exc_func_arg: Function to format exceptions
    """
    # --- Cloud Sync Status ---
    render_sync_status()

    st.sidebar.divider()

    # --- Data Management Section ---
    _render_data_management_section(db_path_arg, ui_logger_arg, format_exc_func_arg)

    st.sidebar.divider()

    # --- Query Templates Section ---
    _render_query_templates_section(
        exports_arg,
        db_path_arg,
        connect_func_arg,
        ui_logger_arg,
        format_exc_func_arg,
        faction_display_map_arg,
    )

    st.sidebar.divider()

    # --- Table Explorer Section ---
    _render_table_explorer_section(
        db_path_arg,
        connect_func_arg,
        get_db_table_list_func_arg,
        get_table_columns_func_arg,
        ui_logger_arg,
        format_exc_func_arg,
        faction_display_map_arg,
    )

    st.sidebar.divider()

    # --- Global Filters Section ---
    _render_global_filters_section(
        get_filter_options_func_arg,
        faction_display_map_arg,
    )


def _render_data_management_section(
    db_path: Path, ui_logger: logging.Logger, format_exc_func: Callable[[Exception], str]
) -> None:
    """Render the data management section of the sidebar."""
    st.sidebar.header("💾 Data Management")

    st.sidebar.multiselect(
        label="Select tables to update:",
        options=get_tables_multiselect_options(),
        default=st.session_state.get("ms_tables_to_refresh", []),
        key="ms_tables_to_refresh_widget",
        on_change=handle_multiselect_change,
    )
    # Note: on_change callback handles state sync; no need for manual call

    # Disable button while refresh is running to prevent double-clicks
    is_refreshing = st.session_state.get("data_refresh_process_running", False)
    if st.sidebar.button(
        "🔄 Refresh Selected Data",
        key="btn_refresh_data",
        disabled=is_refreshing
    ):
        handle_data_refresh_button_click(db_path, ui_logger, format_exc_func)


def _render_query_templates_section(
    exports_arg: Dict[str, Any],
    db_path: Path,
    connect_func: Callable[..., Any],
    ui_logger: logging.Logger,
    format_exc_func: Callable[[Exception], str],
    faction_display_map: Dict[str, int],
) -> None:
    """Render the query templates section of the sidebar."""
    st.sidebar.header("📊 Query Templates")
    query_names_options = [""] + list(exports_arg.keys())

    # Get current value safely
    current_query = st.session_state.get("selected_query_name", "")
    default_index = 0
    if current_query and current_query in query_names_options:
        default_index = query_names_options.index(current_query)

    st.sidebar.selectbox(
        "Choose a template:",
        options=query_names_options,
        index=default_index,
        key="selected_query_name",
    )
    st.sidebar.info(
        "ℹ️ Results limited to 1,000 rows (download for full dataset)", icon="💡"
    )

    if st.sidebar.button(
        "▶️ Run Selected Query",
        disabled=(not st.session_state.get("selected_query_name")),
        key="btn_run_query",
    ):
        handle_run_query_button_click(
            exports_arg,
            db_path,
            connect_func,
            ui_logger,
            format_exc_func,
            faction_display_map,
        )


def _render_table_explorer_section(
    db_path: Path,
    connect_func: Callable[..., Any],
    get_db_table_list_func: Callable[[], List[str]],
    get_table_columns_func: Callable[[str], List[str]],
    ui_logger: logging.Logger,
    format_exc_func: Callable[[Exception], str],
    faction_display_map: Dict[str, int],
) -> None:
    """Render the table explorer section of the sidebar."""
    st.sidebar.header("📑 Browse Raw Data")

    # Create display name list for the dropdown
    raw_table_list = get_db_table_list_func()
    display_name_list = [""] + [get_table_display_name(t) for t in raw_table_list]

    # Build mapping from display name to actual table name
    display_to_table = {"": ""}
    for t in raw_table_list:
        display_to_table[get_table_display_name(t)] = t

    # Get current value safely
    current_table = st.session_state.get("selected_table_for_explorer", "")
    current_display_name = get_table_display_name(current_table) if current_table else ""
    default_table_index = 0
    if current_display_name and current_display_name in display_name_list:
        default_table_index = display_name_list.index(current_display_name)

    # Selectbox shows user-friendly names
    selected_display_name = st.sidebar.selectbox(
        "Choose a table:",
        options=display_name_list,
        index=default_table_index,
        key="selected_table_display_name",
    )

    # Convert display name back to actual table name
    st.session_state.selected_table_for_explorer = display_to_table.get(
        selected_display_name, selected_display_name
    )

    st.sidebar.info("ℹ️ Preview limited to 1,000 rows", icon="💡")

    if st.sidebar.button(
        "🔍 Explore Selected Table",
        disabled=(not st.session_state.get("selected_table_for_explorer")),
        key="btn_explore_table",
    ):
        handle_explore_table_button_click(
            db_path,
            connect_func,
            get_db_table_list_func,
            get_table_columns_func,
            ui_logger,
            format_exc_func,
            faction_display_map,
        )


def _render_global_filters_section(
    get_filter_options_func: Callable[[], Tuple[List[int], List[str]]],
    faction_display_map: Dict[str, int],
) -> None:
    """Render the global filters section of the sidebar."""
    st.sidebar.header("🔍 Global Filters")
    knesset_nums_options, _ = get_filter_options_func()

    # Knesset filter
    st.sidebar.multiselect(
        "🏛️ Knesset Session(s):",
        options=knesset_nums_options,
        key="ms_knesset_filter",
    )

    # Faction filter
    st.sidebar.multiselect(
        "🏘️ Political Faction(s):",
        options=list(faction_display_map.keys()),
        help="Select factions. The Knesset number shows which session they belonged to.",
        key="ms_faction_filter",
    )

    # Document type filter (for bill queries)
    st.sidebar.multiselect(
        "📄 Bill Document Type(s):",
        options=[
            "Published Law",
            "First Reading",
            "Second & Third Reading",
            "Early Stage Discussion",
            "Other",
        ],
        help="Filter bills by document type (applies to Bills query only)",
        key="ms_document_type_filter",
    )
