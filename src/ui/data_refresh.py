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
from config.settings import Settings
from config.database import DatabaseConfig
from utils.logger_setup import setup_logging
from ui.state.session_manager import SessionStateManager

# Use canonical source for table list
TABLES = DatabaseConfig.TABLES
from ui.renderers.data_refresh_page import DataRefreshPageRenderer
from ui.renderers.plots_page import PlotsPageRenderer
from ui.renderers.cap_annotation_page import CAPAnnotationPageRenderer
from ui.queries.predefined_queries import PREDEFINED_QUERIES
import ui.plot_generators as pg
import ui.sidebar_components as sc
import ui.ui_utils as ui_utils

# Initialize logger for the UI module
ui_logger = logging.getLogger("knesset.ui.data_refresh")  # Use logging.getLogger
if not ui_logger.handlers:
    setup_logging("knesset.ui.data_refresh", console_output=True)

ui_logger.info("--- data_refresh.py script started ---")


# --- Configuration from Settings (no hardcoded duplicates) ---
DB_PATH = Settings.DEFAULT_DB_PATH
PARQUET_DIR = Settings.PARQUET_DIR
MAX_ROWS_FOR_CHART_BUILDER = Settings.MAX_ROWS_FOR_CHART_BUILDER
MAX_UNIQUE_VALUES_FOR_FACET = Settings.MAX_UNIQUE_VALUES_FOR_FACET

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

# --- Lazy-loaded filter options (only computed when first accessed) ---
@st.cache_data(ttl=3600, show_spinner=False)
def _get_cached_filter_options():
    """Lazily load and cache filter options."""
    knesset_nums, factions_df = ui_utils.get_filter_options_from_db(DB_PATH, ui_logger)
    faction_map = {
        f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"]
        for _, row in factions_df.iterrows()
    }
    return knesset_nums, factions_df, faction_map


def _get_filter_options():
    """Get filter options (uses cache)."""
    knesset_nums, factions_df, _ = _get_cached_filter_options()
    return knesset_nums, factions_df


def _get_faction_display_map():
    """Get faction display map (uses cache)."""
    _, _, faction_map = _get_cached_filter_options()
    return faction_map


# --- Sidebar (uses lazy-loaded options) ---
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
    get_filter_options_func_arg=_get_filter_options,
    faction_display_map_arg=_get_faction_display_map(),
    ui_logger_arg=ui_logger,
    format_exc_func_arg=ui_utils.format_exception_for_ui,
)

# --- Section Navigation (lazy loading) ---
SECTIONS = ["📊 Data Explorer", "📈 Visualizations", "🏷️ CAP Annotation"]

# Initialize active section in session state
if "active_main_section" not in st.session_state:
    st.session_state.active_main_section = SECTIONS[0]

# Section selector - horizontal radio buttons for tab-like behavior
selected_section = st.radio(
    "Navigate to:",
    options=SECTIONS,
    index=SECTIONS.index(st.session_state.active_main_section),
    horizontal=True,
    key="main_section_selector",
    label_visibility="collapsed",
)
st.session_state.active_main_section = selected_section

st.markdown("---")

# --- Lazy render only the active section ---
if selected_section == "📊 Data Explorer":
    # Initialize renderer only when needed
    if "data_refresh_renderer" not in st.session_state:
        st.session_state.data_refresh_renderer = DataRefreshPageRenderer(DB_PATH, ui_logger)

    data_renderer = st.session_state.data_refresh_renderer
    data_renderer.render_page_header()
    data_renderer.render_query_results_section()
    data_renderer.render_table_explorer_section()

elif selected_section == "📈 Visualizations":
    # Initialize renderer only when needed
    if "plots_renderer" not in st.session_state:
        st.session_state.plots_renderer = PlotsPageRenderer(DB_PATH, ui_logger)

    try:
        st.session_state.plots_renderer.render_plots_section(
            available_plots=pg.get_available_plots(),
            knesset_options=_get_cached_filter_options()[0],
            faction_display_map=_get_faction_display_map(),
            connect_func=lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger)
        )
    except Exception as e:
        st.error(f"Error loading visualizations section: {e}")
        ui_logger.error(f"Error in plots section: {e}", exc_info=True)

elif selected_section == "🏷️ CAP Annotation":
    # Initialize renderer only when needed
    if "cap_annotation_renderer" not in st.session_state:
        st.session_state.cap_annotation_renderer = CAPAnnotationPageRenderer(DB_PATH, ui_logger)

    try:
        st.session_state.cap_annotation_renderer.render_cap_annotation_section()
    except Exception as e:
        st.error(f"Error loading CAP annotation section: {e}")
        ui_logger.error(f"Error in CAP annotation section: {e}", exc_info=True)


ui_logger.info("--- data_refresh.py script finished loading UI components ---")
