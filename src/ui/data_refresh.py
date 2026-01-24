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

    # Debug: Show secrets info
    try:
        secret_sections = list(st.secrets.keys()) if hasattr(st.secrets, 'keys') else []
        has_storage = 'storage' in st.secrets
        has_gcp = 'gcp_service_account' in st.secrets
        bucket_name = st.secrets.get('storage', {}).get('gcs_bucket_name', 'NOT_FOUND')
        ui_logger.info(f"Secrets debug: sections={secret_sections}, storage={has_storage}, gcp={has_gcp}, bucket={bucket_name}")
    except Exception as e:
        ui_logger.error(f"Error reading secrets: {e}")
        secret_sections = []
        has_storage = False
        has_gcp = False
        bucket_name = "ERROR"

    # Try to create sync service and capture any errors
    sync_error = None
    try:
        sync_service = StorageSyncService(logger_obj=ui_logger)
    except Exception as e:
        sync_error = str(e)
        sync_service = None

    # Also try to directly create GCS manager to see the actual error
    gcs_init_error = None
    gcs_creds_keys = []
    gcs_creds_source = "none"
    try:
        from data.storage.cloud_storage import CloudStorageManager, GCS_AVAILABLE
        import json

        # Check if GCS library is available
        if not GCS_AVAILABLE:
            gcs_init_result = "google-cloud-storage library NOT installed"
            gcs_init_error = "Missing dependency: pip install google-cloud-storage"
        else:
            # Get credentials and show what keys are present
            if has_gcp:
                import base64
                gcp_secrets = st.secrets['gcp_service_account']
                gcs_creds_keys = list(gcp_secrets.keys()) if hasattr(gcp_secrets, 'keys') else []

                # Try credentials_base64 format first
                if 'credentials_base64' in gcp_secrets:
                    gcs_creds_source = "credentials_base64"
                    decoded = base64.b64decode(gcp_secrets['credentials_base64']).decode('utf-8')
                    gcs_creds = json.loads(decoded)
                # Then try credentials_json format
                elif 'credentials_json' in gcp_secrets:
                    gcs_creds_source = "credentials_json"
                    gcs_creds = json.loads(gcp_secrets['credentials_json'])
                else:
                    gcs_creds_source = "direct_fields"
                    gcs_creds = dict(gcp_secrets)

                # Try to create manager directly
                test_manager = CloudStorageManager(
                    bucket_name=bucket_name,
                    credentials_dict=gcs_creds,
                    logger_obj=ui_logger
                )
                gcs_init_result = "Success!" if test_manager else "Returned None"
            else:
                gcs_init_result = "No gcp_service_account credentials"
    except Exception as e:
        import traceback
        gcs_init_error = f"{type(e).__name__}: {e}"
        gcs_init_result = f"Error: {type(e).__name__}"

    # Debug: Show sync status in UI
    with st.expander("🔧 Cloud Storage Debug", expanded=True):
        st.write(f"**Secret sections found:** `{secret_sections}`")
        st.write(f"**[storage] section:** `{has_storage}`")
        st.write(f"**[gcp_service_account] section:** `{has_gcp}`")
        st.write(f"**Bucket name:** `{bucket_name}`")
        st.write(f"**Sync service created:** `{sync_service is not None}`")
        if sync_error:
            st.error(f"**Sync service error:** `{sync_error}`")
        st.write(f"**Sync enabled:** `{sync_service.is_enabled() if sync_service else False}`")
        st.write(f"**DB exists locally:** `{DB_PATH.exists()}`")
        st.write(f"**GCS credentials keys:** `{gcs_creds_keys}`")
        st.write(f"**GCS credentials source:** `{gcs_creds_source}`")
        st.write(f"**GCS manager init:** `{gcs_init_result}`")
        if gcs_init_error:
            st.error(f"**GCS init error:** `{gcs_init_error}`")

    if sync_service and sync_service.is_enabled():
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
        st.warning("⚠️ Cloud storage sync is NOT enabled. Check your secrets configuration.")
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
cap_annotation_renderer = CAPAnnotationPageRenderer(DB_PATH, ui_logger)

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

# Render CAP Bill Annotation section (password-protected)
try:
    st.markdown("---")
    cap_annotation_renderer.render_cap_annotation_section()
except Exception as e:
    st.error(f"Error loading CAP annotation section: {e}")
    ui_logger.error(f"Error in CAP annotation section: {e}", exc_info=True)


ui_logger.info("--- data_refresh.py script finished loading UI components ---")
