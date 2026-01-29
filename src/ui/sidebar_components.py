"""Sidebar UI components for the Knesset data console.

This module is a facade for backward compatibility.
The actual implementation has been moved to the sidebar/ package.

All imports from this module will continue to work:
    from src.ui.sidebar_components import display_sidebar, TABLE_DISPLAY_NAMES

For new code, you can also import directly from the package:
    from src.ui.sidebar.components import display_sidebar, TABLE_DISPLAY_NAMES
"""

# Re-export all public symbols from the package for backward compatibility
from .sidebar import (
    TABLE_DISPLAY_NAMES,
    get_table_display_name,
    get_table_name_from_display,
    display_sidebar,
    render_sync_status,
    # Private functions exported for backward compatibility (used by tests)
    _handle_data_refresh_button_click,
    _handle_run_query_button_click,
    _handle_explore_table_button_click,
    _handle_multiselect_change,
    _SELECT_ALL_TABLES_OPTION,
)

__all__ = [
    'TABLE_DISPLAY_NAMES',
    'get_table_display_name',
    'get_table_name_from_display',
    'display_sidebar',
    'render_sync_status',
    '_handle_data_refresh_button_click',
    '_handle_run_query_button_click',
    '_handle_explore_table_button_click',
    '_handle_multiselect_change',
    '_SELECT_ALL_TABLES_OPTION',
]
