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
import ui.chart_builder_ui as cb_ui

# Initialize logger for the UI module
ui_logger = logging.getLogger("knesset.ui.data_refresh") # Use logging.getLogger
if not ui_logger.handlers: 
    setup_logging("knesset.ui.data_refresh", console_output=True)

ui_logger.info("--- data_refresh.py script started ---")


# --- Constants and Global-like Configurations ---
DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/parquet")
MAX_ROWS_FOR_CHART_BUILDER = 50000
MAX_UNIQUE_VALUES_FOR_FACET = 50

# Plot names updated to reflect single Knesset focus where applicable
AVAILABLE_PLOTS_BY_TOPIC = {
    "Queries": {
        "Queries by Time Period": pg.plot_queries_by_time_period,
        "Distribution of Query Types (Single Knesset)": pg.plot_query_types_distribution,
        "Query Status Description with Faction Breakdown (Single Knesset)": pg.plot_query_status_by_faction,
        "Queries per Faction (Single Knesset)": pg.plot_queries_per_faction_in_knesset,
        "Queries by Coalition & Answer Status (Single Knesset)": pg.plot_queries_by_coalition_and_answer_status,
        "Query Performance by Ministry (Single Knesset)": pg.plot_queries_by_ministry_and_status,
        "Query Response Times (Single Knesset)": pg.plot_query_response_times,
        "Ministry Workload Hierarchy (Single Knesset)": pg.plot_ministry_workload_sunburst,
    },
    "Agendas": {
        "Agenda Items by Time Period": pg.plot_agendas_by_time_period,
        "Distribution of Agenda Classifications (Single Knesset)": pg.plot_agenda_classifications_pie,
        "Agenda Item Status Distribution (Single Knesset)": pg.plot_agenda_status_distribution,
        "Agendas per Faction (Single Knesset)": pg.plot_agendas_per_faction_in_knesset,
        "Agendas by Coalition & Status (Single Knesset)": pg.plot_agendas_by_coalition_and_status,
    },
    "Advanced Analytics": {
        "Parliamentary Activity Heatmap (Single Knesset)": pg.plot_parliamentary_activity_heatmap,
        "MK Collaboration Network (Single Knesset)": pg.plot_mk_collaboration_network,
    }
}

st.set_page_config(page_title="Knesset OData – Refresh & Export", layout="wide")

ui_logger.info("--- Initializing session state ---")
SessionStateManager.initialize_all_session_state()
ui_logger.info("--- Finished initializing session state ---")

knesset_nums_options_global, factions_options_df_global = ui_utils.get_filter_options_from_db(DB_PATH, ui_logger)
faction_display_map_global = {
    f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"]
    for _, row in factions_options_df_global.iterrows()
}

sc.display_sidebar(
    db_path_arg=DB_PATH,
    exports_arg=PREDEFINED_QUERIES,
    connect_func_arg=lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger),
    get_db_table_list_func_arg=lambda: ui_utils.get_db_table_list(DB_PATH, _logger_obj=ui_logger),
    get_table_columns_func_arg=lambda table_name: ui_utils.get_table_columns(DB_PATH, table_name, _logger_obj=ui_logger),
    get_filter_options_func_arg=lambda: (knesset_nums_options_global, factions_options_df_global),
    faction_display_map_arg=faction_display_map_global,
    ui_logger_arg=ui_logger,
    format_exc_func_arg=ui_utils.format_exception_for_ui
)

# Initialize page renderers
page_renderer = DataRefreshPageRenderer(DB_PATH, ui_logger)
plots_renderer = PlotsPageRenderer(DB_PATH, ui_logger)

# Render page sections
page_renderer.render_page_header()
page_renderer.render_query_results_section()
page_renderer.render_table_explorer_section()

# Render plots section
plots_renderer.render_plots_section(
    available_plots=AVAILABLE_PLOTS_BY_TOPIC,
    knesset_options=sorted([str(k) for k in knesset_nums_options_global], key=int, reverse=True) if knesset_nums_options_global else [],
    faction_display_map=faction_display_map_global,
    connect_func=lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger)
)


st.divider()
cb_ui.display_chart_builder(
    db_path=DB_PATH,
    max_rows_for_chart_builder=MAX_ROWS_FOR_CHART_BUILDER,
    max_unique_values_for_facet=MAX_UNIQUE_VALUES_FOR_FACET,
    faction_display_map_global=faction_display_map_global,
    logger_obj=ui_logger
)

# Render remaining sections
page_renderer.render_ad_hoc_sql_section(
    connect_func=lambda read_only=True: ui_utils.connect_db(DB_PATH, read_only, _logger_obj=ui_logger)
)
page_renderer.render_table_status_section(PARQUET_DIR, TABLES)

ui_logger.info("--- data_refresh.py script finished loading UI components ---")

