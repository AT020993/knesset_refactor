"""Main data refresh page renderer.

This module keeps DataRefreshPageRenderer's public API stable while delegating
query result rendering and ad-hoc SQL execution to specialized operations.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st

from ui.state.session_manager import SessionStateManager

from .dataset_exporter import DatasetExporter
from .document_handler import DocumentHandler
from .query_results_ops import (
    apply_knesset_filter_callback,
    execute_ad_hoc_query,
    get_query_type_from_name,
    render_download_options,
    render_local_knesset_filter,
    render_pagination_controls,
    render_query_results_display,
    rerun_query_with_pagination,
)
from .table_explorer import TableExplorer


class DataRefreshPageRenderer:
    """Handles rendering of the main data refresh page components."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger

        self._document_handler = DocumentHandler(db_path, logger)
        self._dataset_exporter = DatasetExporter(db_path, logger)
        self._table_explorer = TableExplorer(db_path, logger)

    def render_page_header(self) -> None:
        """Render page title and top-level help."""
        st.title("🇮🇱 Knesset Data Console")

    def render_query_results_section(self) -> None:
        """Render predefined query results panel."""
        st.divider()
        st.header("📄 Predefined Query Results")

        if (
            SessionStateManager.get_show_query_results()
            and SessionStateManager.get_executed_query_name()
        ):
            self._render_query_results_display()
        else:
            st.info("Run a predefined query from the sidebar to see results here.")

    def _render_query_results_display(self) -> None:
        render_query_results_display(self)

    def _render_download_options(self, display_df: pd.DataFrame, safe_name: str) -> None:
        render_download_options(self, display_df, safe_name)

    def _apply_knesset_filter_callback(self):
        apply_knesset_filter_callback(self)

    def _render_local_knesset_filter(self, results_df: pd.DataFrame) -> None:
        render_local_knesset_filter(self, results_df)

    def _render_pagination_controls(self, results_df: pd.DataFrame) -> None:
        render_pagination_controls(self, results_df)

    def _rerun_query_with_pagination(self):
        rerun_query_with_pagination(self)

    @staticmethod
    def _get_query_type_from_name(query_name: str) -> str:
        return get_query_type_from_name(query_name)

    def render_table_explorer_section(self) -> None:
        """Render table explorer results section."""
        self._table_explorer.render_section()

    def render_ad_hoc_sql_section(self, connect_func: Callable) -> None:
        """Render ad-hoc SQL query section."""
        st.divider()
        with st.expander("🧑‍🔬 Run an Ad-hoc SQL Query (Advanced)", expanded=False):
            if not self.db_path.exists():
                st.warning("Database not found. Cannot run SQL queries.")
                return

            st.markdown(
                "Construct your SQL query. Sidebar filters are **not** automatically "
                "applied here. Include them in your `WHERE` clause if needed."
            )

            default_sql_query = (
                "SELECT t.table_name, t.row_count FROM duckdb_tables() t "
                "WHERE t.schema_name = 'main' ORDER BY t.table_name;"
            )
            sql_query_input = st.text_area(
                "Enter your SQL query:",
                default_sql_query,
                height=150,
                key="adhoc_sql_query",
            )

            if st.button("▶︎ Run Ad-hoc SQL", key="run_adhoc_sql"):
                if sql_query_input.strip():
                    self._execute_ad_hoc_query(sql_query_input, connect_func)
                else:
                    st.warning("SQL query cannot be empty.")

    def _execute_ad_hoc_query(self, sql_query: str, connect_func: Callable) -> None:
        execute_ad_hoc_query(self, sql_query, connect_func)

    def render_table_status_section(self, parquet_dir: Path, tables_list: list[str]) -> None:
        """Render table update status section."""
        self._table_explorer.render_table_status_section(parquet_dir, tables_list)

    # Backward compatibility wrappers
    @staticmethod
    def _create_document_badge(row: pd.Series) -> str:
        return DocumentHandler.create_document_badge(row)

    def _format_bill_document_links(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._document_handler.format_bill_document_links(df)

    @staticmethod
    def _get_column_config(df: pd.DataFrame) -> dict:
        return DocumentHandler.get_column_config(df)

    def _create_excel_with_hyperlinks(self, df: pd.DataFrame) -> io.BytesIO:
        return self._document_handler.create_excel_with_hyperlinks(df)

    def _render_multi_document_view(self, df: pd.DataFrame) -> None:
        self._document_handler.render_multi_document_view(df)

    def _get_bill_documents(self, bill_id: int) -> pd.DataFrame:
        return self._document_handler._get_bill_documents(bill_id)

    @staticmethod
    def _render_document_list(docs_df: pd.DataFrame, bill_id: int, bill_row_idx: int = 0) -> None:
        DocumentHandler._render_document_list(docs_df, bill_id, bill_row_idx)

    @staticmethod
    def _remove_limit_offset_from_query(sql: str) -> str:
        return DatasetExporter.remove_limit_offset_from_query(sql)

    def _render_full_dataset_download(self, modified_sql: str, safe_name: str) -> None:
        self._dataset_exporter.render_full_dataset_download(modified_sql, safe_name)

    def _render_table_explorer_display(self) -> None:
        self._table_explorer._render_display()
