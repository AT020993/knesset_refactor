"""Sidebar query execution handler built on the typed QueryExecutor."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st

from backend.connection_manager import safe_execute_query
from ui.queries.query_executor import QueryExecutor


def handle_run_query_button_click(
    exports_dict: dict,
    db_path: Path,
    connect_func: Callable,
    ui_logger: logging.Logger,
    format_exc_func: Callable,
    faction_display_map: dict,
):
    """Handle the "Run Selected Query" sidebar action."""
    _ = exports_dict  # Retained for backward-compatible signature.
    _ = connect_func

    selected_query = st.session_state.get("selected_query_name")
    if not selected_query:
        st.warning("Please select a query before running.")
        return

    if not db_path.exists():
        st.error(
            "Database not found. Please ensure 'data/warehouse.duckdb' exists or run data refresh."
        )
        st.session_state.show_query_results = False
        return

    try:
        page_offset = st.session_state.get("query_page_offset", 0)
        if not page_offset:
            st.session_state.query_page_number = 1
            st.session_state.query_page_offset = 0

        selected_faction_ids = [
            faction_display_map[name]
            for name in st.session_state.get("ms_faction_filter", [])
            if name in faction_display_map
        ]

        document_type_filter = st.session_state.get("ms_document_type_filter", [])
        if "bill" not in selected_query.lower():
            document_type_filter = []

        executor = QueryExecutor(db_path=db_path, connect_func=None, logger=ui_logger)
        results_df, executed_sql, applied_filters, query_params = (
            executor.execute_query_with_filters(
                query_name=selected_query,
                knesset_filter=st.session_state.get("ms_knesset_filter", []),
                faction_filter=selected_faction_ids,
                safe_execute_func=safe_execute_query,
                document_type_filter=document_type_filter,
                page_offset=page_offset,
            )
        )

        if results_df.empty and not applied_filters:
            # Preserve previous behavior for invalid query names.
            st.session_state.show_query_results = False
            st.session_state.query_results_df = pd.DataFrame()
            st.error(f"Query '{selected_query}' was not found.")
            return

        st.session_state.query_results_df = results_df
        st.session_state.executed_query_name = selected_query
        st.session_state.show_query_results = True
        st.session_state.show_table_explorer_results = False
        st.session_state.applied_filters_info_query = applied_filters
        st.session_state.last_executed_sql = executed_sql
        st.session_state.last_query_params = query_params

        st.toast(f"✅ Query '{selected_query}' executed.", icon="📊")

    except Exception as exc:
        ui_logger.error(
            "Error executing query '%s': %s", selected_query, exc, exc_info=True
        )
        st.error(f"Error executing query '{selected_query}': {exc}")
        st.code(f"Error: {exc}\n\nTraceback:\n{format_exc_func()}")
        st.session_state.show_query_results = False
        st.session_state.query_results_df = pd.DataFrame()
