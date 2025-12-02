"""Sidebar components package.

This package contains sidebar UI components including:
- Table selection and display
- Knesset and faction filters
- Data refresh controls
- Query execution controls

For backward compatibility, all functions and constants are re-exported at the package level.
"""

from .components import (
    TABLE_DISPLAY_NAMES,
    get_table_display_name,
    get_table_name_from_display,
    display_sidebar,
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
    '_handle_data_refresh_button_click',
    '_handle_run_query_button_click',
    '_handle_explore_table_button_click',
    '_handle_multiselect_change',
    '_SELECT_ALL_TABLES_OPTION',
]
