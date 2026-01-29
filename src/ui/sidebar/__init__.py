"""Sidebar components package.

This package contains sidebar UI components including:
- Table selection and display
- Knesset and faction filters
- Data refresh controls
- Query execution controls

For backward compatibility, all functions and constants are re-exported at the package level.

Module structure:
- components.py: Main orchestrator (display_sidebar)
- data_refresh_handler.py: Data refresh button logic
- query_handler.py: Query execution logic
- table_explorer_handler.py: Table exploration logic
"""

from .components import (
    TABLE_DISPLAY_NAMES,
    TABLE_NAME_FROM_DISPLAY,
    get_table_display_name,
    get_table_name_from_display,
    display_sidebar,
    render_sync_status,
)

# Re-export handler functions for backward compatibility
from .data_refresh_handler import (
    handle_data_refresh_button_click as _handle_data_refresh_button_click,
    handle_multiselect_change as _handle_multiselect_change,
    SELECT_ALL_TABLES_OPTION as _SELECT_ALL_TABLES_OPTION,
)
from .query_handler import (
    handle_run_query_button_click as _handle_run_query_button_click,
)
from .table_explorer_handler import (
    handle_explore_table_button_click as _handle_explore_table_button_click,
)

__all__ = [
    # Main display function
    "display_sidebar",
    "render_sync_status",
    # Table config exports
    "TABLE_DISPLAY_NAMES",
    "TABLE_NAME_FROM_DISPLAY",
    "get_table_display_name",
    "get_table_name_from_display",
    # Backward compatibility exports (with underscore prefix)
    "_handle_data_refresh_button_click",
    "_handle_run_query_button_click",
    "_handle_explore_table_button_click",
    "_handle_multiselect_change",
    "_SELECT_ALL_TABLES_OPTION",
]
