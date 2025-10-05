from __future__ import annotations

# Standard Library Imports
import io
import logging
import re
import sys
from pathlib import Path
from textwrap import dedent

# Third-Party Imports
import pandas as pd
import streamlit as st
import duckdb

# Add the 'src' directory to sys.path
_CURRENT_FILE_DIR = Path(__file__).resolve().parent
_SRC_DIR = _CURRENT_FILE_DIR.parent
_PROJECT_ROOT = _SRC_DIR.parent

if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Local Application Imports
from utils.logger_setup import setup_logging
from backend.fetch_table import TABLES
from ui.state.session_manager import SessionStateManager
from ui.pages.data_refresh_page import DataRefreshPageRenderer
from ui.pages.plots_page import PlotsPageRenderer
from ui.queries.predefined_queries import PREDEFINED_QUERIES
import ui.plot_generators as pg
import ui.sidebar_components as sc
import ui.ui_utils as ui_utils

# Initialize logger for the UI module
ui_logger = logging.getLogger("knesset.ui.data_refresh")  # Use logging.getLogger
if not ui_logger.handlers:
    setup_logging("knesset.ui.data_refresh", console_output=True)

ui_logger.info("--- data_refresh.py script started ---")


# --- Constants and Global-like Configurations ---
DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/parquet")
MAX_ROWS_FOR_CHART_BUILDER = 50000
MAX_UNIQUE_VALUES_FOR_FACET = 50

st.set_page_config(
    page_title="Knesset OData – Refresh & Export", 
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get help': None,
        'Report a Bug': None,
        'About': None
    }
)

ui_logger.info("--- Initializing session state ---")
SessionStateManager.initialize_all_session_state()
ui_logger.info("--- Finished initializing session state ---")

# Initialize cloud storage sync on first load
if 'cloud_sync_checked' not in st.session_state:
    st.session_state.cloud_sync_checked = False

if not st.session_state.cloud_sync_checked:
    from data.services.storage_sync_service import StorageSyncService

    sync_service = StorageSyncService(logger_obj=ui_logger)

    if sync_service.is_enabled():
        ui_logger.info("Cloud storage enabled, checking for data sync...")

        # Check if local database exists
        if not DB_PATH.exists():
            ui_logger.info("Local database not found, attempting cloud sync...")

            with st.spinner("Syncing data from cloud storage..."):
                try:
                    success = sync_service.smart_sync_on_startup(
                        progress_callback=lambda msg: ui_logger.info(f"Sync: {msg}")
                    )

                    if success:
                        st.success("Data synced from cloud storage successfully!")
                        ui_logger.info("Cloud sync completed successfully")
                    else:
                        st.info("No data found in cloud storage. Please refresh data using the sidebar.")
                        ui_logger.info("No cloud data available, local refresh required")
                except Exception as e:
                    st.warning(f"Cloud sync failed: {e}. Please refresh data manually.")
                    ui_logger.error(f"Cloud sync error: {e}", exc_info=True)
        else:
            ui_logger.info("Local database exists, skipping cloud sync")
    else:
        ui_logger.info("Cloud storage not enabled")

    st.session_state.cloud_sync_checked = True

knesset_nums_options_global, factions_options_df_global = (
    ui_utils.get_filter_options_from_db(DB_PATH, ui_logger)
)
faction_display_map_global = {
    f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"]
    for _, row in factions_options_df_global.iterrows()
}

sc.display_sidebar(
    db_path_arg=DB_PATH,
    exports_arg=PREDEFINED_QUERIES,
    connect_func_arg=lambda read_only=True: ui_utils.connect_db(
        DB_PATH, read_only, _logger_obj=ui_logger
    ),
    get_db_table_list_func_arg=lambda: ui_utils.get_db_table_list(
        DB_PATH, _logger_obj=ui_logger
    ),
    get_table_columns_func_arg=lambda table_name: ui_utils.get_table_columns(
        DB_PATH, table_name, _logger_obj=ui_logger
    ),
    get_filter_options_func_arg=lambda: (
        knesset_nums_options_global,
        factions_options_df_global,
    ),
    faction_display_map_arg=faction_display_map_global,
    ui_logger_arg=ui_logger,
    format_exc_func_arg=ui_utils.format_exception_for_ui,
)

# Initialize page renderers
data_refresh_renderer = DataRefreshPageRenderer(DB_PATH, ui_logger)
plots_renderer = PlotsPageRenderer(DB_PATH, ui_logger)

# Render main page header
data_refresh_renderer.render_page_header()

# Main content sections
data_refresh_renderer.render_query_results_section()
data_refresh_renderer.render_table_explorer_section()

# Render plots/visualizations section
try:
    plots_renderer.render_plots_section(
        available_plots=pg.get_available_plots(),
        knesset_options=knesset_nums_options_global,
        faction_display_map=faction_display_map_global,
        connect_func=lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger)
    )
except Exception as e:
    st.error(f"Error loading visualizations section: {e}")
    ui_logger.error(f"Error in plots section: {e}", exc_info=True)


ui_logger.info("--- data_refresh.py script finished loading UI components ---")
