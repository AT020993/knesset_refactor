"""
Main data refresh page UI components.

This module contains the primary UI rendering logic for the data refresh page,
separated from business logic and session state management.

The page renderer orchestrates several specialized handlers:
- DocumentHandler: Bill/agenda document formatting and display
- DatasetExporter: Full dataset downloads and export
- TableExplorer: Raw table browsing and exploration
"""

import io
import logging
import re
import sys
from pathlib import Path
from typing import Callable, List

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from ui.state.session_manager import SessionStateManager
from ui.queries.predefined_queries import get_all_query_names, get_query_info
from utils.export_verifier import ExportVerifier
import ui.ui_utils as ui_utils

# Import specialized handlers
from .document_handler import DocumentHandler
from .dataset_exporter import DatasetExporter
from .table_explorer import TableExplorer


class DataRefreshPageRenderer:
    """Handles rendering of the main data refresh page components.

    This class orchestrates several specialized handlers for different concerns:
    - DocumentHandler: Document link formatting, Excel hyperlinks, multi-doc views
    - DatasetExporter: Full dataset downloads and SQL query modifications
    - TableExplorer: Table exploration UI and status display
    """

    def __init__(self, db_path: Path, logger: logging.Logger):
        """
        Initialize the page renderer.

        Args:
            db_path: Path to the database
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.logger = logger

        # Initialize specialized handlers
        self._document_handler = DocumentHandler(db_path, logger)
        self._dataset_exporter = DatasetExporter(db_path, logger)
        self._table_explorer = TableExplorer(db_path, logger)

    def render_page_header(self) -> None:
        """Render the page title and help information."""
        st.title("🇮🇱 Knesset Data Console")

    def render_query_results_section(self) -> None:
        """Render the predefined query results section."""
        st.divider()
        st.header("📄 Predefined Query Results")

        if SessionStateManager.get_show_query_results() and SessionStateManager.get_executed_query_name():
            self._render_query_results_display()
        else:
            st.info("Run a predefined query from the sidebar to see results here.")

    def _render_query_results_display(self) -> None:
        """Render the actual query results with download options."""
        query_name = SessionStateManager.get_executed_query_name()
        results_df = SessionStateManager.get_query_results_df()
        applied_filters = SessionStateManager.get_applied_filters_info_query()
        last_sql = SessionStateManager.get_last_executed_sql()

        # Build header with filters info
        subheader_text = f"Results for: **{query_name}**"
        if applied_filters:
            filters_applied_text = '; '.join(applied_filters)
            if filters_applied_text and filters_applied_text != "Knesset(s): All; Faction(s): All":
                subheader_text += f" (Active Filters: *{filters_applied_text}*)"
        st.markdown(subheader_text)

        # Add local Knesset filter for query results
        if not results_df.empty and 'KnessetNum' in results_df.columns:
            self._render_local_knesset_filter(results_df)

        # Use results directly (filtering is now done in SQL query)
        display_df = results_df

        if not display_df.empty:
            # Add document links formatting if bill document fields exist
            if 'BillPrimaryDocumentURL' in display_df.columns:
                display_df = self._document_handler.format_bill_document_links(display_df)

            # Display results with formatted dates
            formatted_df = ui_utils.format_dataframe_dates(display_df, _logger_obj=self.logger)

            # Get column configuration for clickable links and hidden columns
            column_config = self._document_handler.get_column_config(formatted_df)

            st.dataframe(formatted_df, use_container_width=True, height=400, column_config=column_config)

            # Multi-document expandable view for bills with many documents
            if 'BillID' in display_df.columns and 'BillDocumentCount' in display_df.columns:
                self._document_handler.render_multi_document_view(display_df)

            # Download options with verification
            safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", query_name)
            self._render_download_options(display_df, safe_name)

            # Full Dataset Download Section
            self._dataset_exporter.render_full_dataset_download(last_sql, safe_name)
        else:
            st.info("The query returned no results with the current filters.")

        # Show executed SQL
        with st.expander("Show Executed SQL", expanded=False):
            st.code(last_sql if last_sql else "No SQL executed yet.", language="sql")

    def _render_download_options(self, display_df: pd.DataFrame, safe_name: str) -> None:
        """
        Render CSV/Excel download options with verification.

        Args:
            display_df: DataFrame to export
            safe_name: Sanitized query name for file naming
        """
        col_csv, col_excel, col_verify = st.columns([1, 1, 1])

        # Create export verifier
        verifier = ExportVerifier(self.logger)

        with col_csv:
            csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ CSV",
                csv_data,
                f"{safe_name}_results.csv",
                "text/csv",
                key=f"csv_dl_{safe_name}"
            )

        with col_excel:
            # Create Excel with clickable hyperlinks for URL columns
            excel_buffer = self._document_handler.create_excel_with_hyperlinks(display_df)
            st.download_button(
                "⬇️ Excel (with hyperlinks)",
                excel_buffer.getvalue(),
                f"{safe_name}_results.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"excel_dl_{safe_name}",
                help="Excel file with clickable document and website links"
            )

        with col_verify:
            # Verify CSV export consistency
            csv_buffer = io.BytesIO(csv_data)
            verification = verifier.verify_csv_export(display_df, csv_buffer)
            if verification['is_valid']:
                st.success(f"✅ Data verified: {verification['source_rows']} rows", icon="✅")
            else:
                st.warning(f"⚠️ {verification['details']}", icon="⚠️")

    def _apply_knesset_filter_callback(self):
        """Callback function to apply the Knesset filter."""
        # Update the actual filter based on temp selection
        if st.session_state.temp_knesset_filter == "All Knessetes":
            st.session_state.ms_knesset_filter = []
        else:
            try:
                knesset_num = int(st.session_state.temp_knesset_filter.replace("Knesset ", ""))
                st.session_state.ms_knesset_filter = [knesset_num]
            except (ValueError, AttributeError) as e:
                self.logger.error(f"Invalid Knesset filter format: {st.session_state.temp_knesset_filter}, error: {e}")
                st.error(f"Invalid Knesset filter format")
                return  # Early exit on error

        # Reset pagination when filter changes
        st.session_state.query_page_number = 1
        st.session_state.query_page_offset = 0

        # Re-execute the query immediately with updated filter
        query_name = SessionStateManager.get_executed_query_name()
        if query_name:
            from ui.sidebar import _handle_run_query_button_click
            from ui.queries.predefined_queries import PREDEFINED_QUERIES

            _handle_run_query_button_click(
                exports_dict=PREDEFINED_QUERIES,
                db_path=self.db_path,
                connect_func=lambda read_only=True: ui_utils.connect_db(self.db_path, read_only, self.logger),
                ui_logger=self.logger,
                format_exc_func=ui_utils.format_exception_for_ui,
                faction_display_map=st.session_state.get("faction_display_map", {})
            )

    def _render_local_knesset_filter(self, results_df: pd.DataFrame) -> None:
        """Render local Knesset filter widget for query results."""
        # Get the query type from the executed query name
        query_name = SessionStateManager.get_executed_query_name()
        query_type = self._get_query_type_from_name(query_name)

        # Get ALL available Knessetes from the database (not just from limited results)
        available_knessetes = ui_utils.get_available_knessetes_for_query(
            self.db_path,
            query_type,
            _logger_obj=self.logger
        )

        # Create a container for the filter
        st.markdown("**Filter Results by Knesset:**")
        st.info("💡 Select a Knesset and click 'Apply Filter' to re-run the query with up to 1,000 rows from the selected Knesset.", icon="ℹ️")

        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            # Determine current filter from ms_knesset_filter (the actual query filter)
            current_ms_filter = st.session_state.get("ms_knesset_filter", [])
            if current_ms_filter and len(current_ms_filter) == 1:
                current_value = f"Knesset {current_ms_filter[0]}"
            else:
                current_value = "All Knessetes"

            # Initialize temp filter state
            if "temp_knesset_filter" not in st.session_state:
                st.session_state.temp_knesset_filter = current_value

            # Knesset filter selectbox (not bound to widget state)
            knesset_options = ["All Knessetes"] + [f"Knesset {k}" for k in available_knessetes]

            selected_filter = st.selectbox(
                "Select Knesset:",
                options=knesset_options,
                index=knesset_options.index(st.session_state.temp_knesset_filter) if st.session_state.temp_knesset_filter in knesset_options else 0,
                key="local_knesset_filter_widget",
                help="Select a Knesset and click 'Apply Filter' to re-run the query."
            )

            # Update temp state
            st.session_state.temp_knesset_filter = selected_filter

        with col2:
            # Apply filter button with callback
            st.button(
                "🔄 Apply Filter",
                key="apply_knesset_filter_btn",
                on_click=self._apply_knesset_filter_callback,
                use_container_width=True
            )

        with col3:
            # Show count of current results
            st.metric("Rows", len(results_df))

        # Add pagination controls
        self._render_pagination_controls(results_df)

    def _render_pagination_controls(self, results_df: pd.DataFrame) -> None:
        """Render pagination controls for navigating through query results."""
        # Initialize page number if not set
        if "query_page_number" not in st.session_state:
            st.session_state.query_page_number = 1

        st.markdown("---")
        st.markdown("**Navigate Results:**")

        # Calculate pagination info
        current_page = st.session_state.query_page_number
        rows_per_page = 1000
        start_row = (current_page - 1) * rows_per_page + 1
        end_row = start_row + len(results_df) - 1

        # Show page info
        col1, col2, col3, col4 = st.columns([2, 1, 1, 2])

        with col1:
            st.info(f"📄 Showing rows {start_row:,}-{end_row:,} (Page {current_page})")

        with col2:
            # Previous page button
            if st.button("◀ Previous", key="prev_page_btn", disabled=(current_page == 1), use_container_width=True):
                st.session_state.query_page_number = max(1, current_page - 1)
                st.session_state.query_page_offset = (st.session_state.query_page_number - 1) * 1000
                self._rerun_query_with_pagination()
                # Note: st.rerun() removed - _rerun_query_with_pagination already updates state

        with col3:
            # Next page button
            has_more = len(results_df) == rows_per_page  # If we got full 1000 rows, there might be more
            if st.button("Next ▶", key="next_page_btn", disabled=not has_more, use_container_width=True):
                st.session_state.query_page_number = current_page + 1
                st.session_state.query_page_offset = (st.session_state.query_page_number - 1) * 1000
                self._rerun_query_with_pagination()
                # Note: st.rerun() removed - _rerun_query_with_pagination already updates state

        with col4:
            # Reset to first page button
            if current_page > 1:
                if st.button("⏮ First Page", key="first_page_btn", use_container_width=True):
                    st.session_state.query_page_number = 1
                    st.session_state.query_page_offset = 0
                    self._rerun_query_with_pagination()
                    # Note: st.rerun() removed - _rerun_query_with_pagination already updates state

    def _rerun_query_with_pagination(self):
        """Re-execute the current query with updated pagination offset."""
        query_name = SessionStateManager.get_executed_query_name()
        if query_name:
            from ui.sidebar import _handle_run_query_button_click
            from ui.queries.predefined_queries import PREDEFINED_QUERIES

            _handle_run_query_button_click(
                exports_dict=PREDEFINED_QUERIES,
                db_path=self.db_path,
                connect_func=lambda read_only=True: ui_utils.connect_db(self.db_path, read_only, self.logger),
                ui_logger=self.logger,
                format_exc_func=ui_utils.format_exception_for_ui,
                faction_display_map=st.session_state.get("faction_display_map", {})
            )

    @staticmethod
    def _get_query_type_from_name(query_name: str) -> str:
        """
        Determine the query type (queries, agendas, bills) from the query name.

        Args:
            query_name: Name of the predefined query

        Returns:
            Query type string ("queries", "agendas", or "bills")
        """
        if not query_name:
            return "queries"

        query_name_lower = query_name.lower()
        if "bill" in query_name_lower:
            return "bills"
        elif "agenda" in query_name_lower:
            return "agendas"
        else:
            return "queries"

    def render_table_explorer_section(self) -> None:
        """Render the table explorer results section."""
        self._table_explorer.render_section()

    def render_ad_hoc_sql_section(self, connect_func: Callable) -> None:
        """
        Render the ad-hoc SQL query section.

        Args:
            connect_func: Function to create database connections
        """
        st.divider()
        with st.expander("🧑‍🔬 Run an Ad-hoc SQL Query (Advanced)", expanded=False):
            if not self.db_path.exists():
                st.warning("Database not found. Cannot run SQL queries.")
                return

            st.markdown("Construct your SQL query. Sidebar filters are **not** automatically applied here. Include them in your `WHERE` clause if needed.")

            default_sql_query = "SELECT t.table_name, t.row_count FROM duckdb_tables() t WHERE t.schema_name = 'main' ORDER BY t.table_name;"
            sql_query_input = st.text_area(
                "Enter your SQL query:",
                default_sql_query,
                height=150,
                key="adhoc_sql_query"
            )

            if st.button("▶︎ Run Ad-hoc SQL", key="run_adhoc_sql"):
                if sql_query_input.strip():
                    self._execute_ad_hoc_query(sql_query_input, connect_func)
                else:
                    st.warning("SQL query cannot be empty.")

    def _execute_ad_hoc_query(self, sql_query: str, connect_func: Callable) -> None:
        """
        Execute an ad-hoc SQL query and display results.

        Args:
            sql_query: The SQL query to execute
            connect_func: Function to create database connections
        """
        con = None
        try:
            con = connect_func(read_only=True)
            adhoc_result_df = ui_utils.safe_execute_query(con, sql_query, _logger_obj=self.logger)

            formatted_adhoc_df = ui_utils.format_dataframe_dates(adhoc_result_df, _logger_obj=self.logger)
            st.dataframe(formatted_adhoc_df, use_container_width=True)

            if not adhoc_result_df.empty:
                csv_data = adhoc_result_df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "⬇️ CSV",
                    csv_data,
                    "adhoc_results.csv",
                    "text/csv",
                    key="adhoc_csv_dl"
                )
        except Exception as e:
            self.logger.error(f"❌ Ad-hoc SQL Query Error: {e}", exc_info=True)
            st.error(f"❌ SQL Query Error: {ui_utils.format_exception_for_ui(sys.exc_info())}")
        finally:
            if con:
                con.close()

    def render_table_status_section(self, parquet_dir: Path, tables_list: List[str]) -> None:
        """
        Render the table update status section.

        Args:
            parquet_dir: Path to parquet files directory
            tables_list: List of table names to check status for
        """
        self._table_explorer.render_table_status_section(parquet_dir, tables_list)

    # ============================================================
    # Backward compatibility methods - delegate to handlers
    # These are kept for any external code that might use them directly
    # ============================================================

    @staticmethod
    def _create_document_badge(row: pd.Series) -> str:
        """Create a readable document badge for a bill row. (Backward compatibility)"""
        return DocumentHandler.create_document_badge(row)

    def _format_bill_document_links(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format bill document links. (Backward compatibility)"""
        return self._document_handler.format_bill_document_links(df)

    @staticmethod
    def _get_column_config(df: pd.DataFrame) -> dict:
        """Get column configuration. (Backward compatibility)"""
        return DocumentHandler.get_column_config(df)

    def _create_excel_with_hyperlinks(self, df: pd.DataFrame) -> io.BytesIO:
        """Create Excel with hyperlinks. (Backward compatibility)"""
        return self._document_handler.create_excel_with_hyperlinks(df)

    def _render_multi_document_view(self, df: pd.DataFrame) -> None:
        """Render multi-document view. (Backward compatibility)"""
        self._document_handler.render_multi_document_view(df)

    def _get_bill_documents(self, bill_id: int) -> pd.DataFrame:
        """Get bill documents. (Backward compatibility)"""
        return self._document_handler._get_bill_documents(bill_id)

    @staticmethod
    def _render_document_list(docs_df: pd.DataFrame, bill_id: int, bill_row_idx: int = 0) -> None:
        """Render document list. (Backward compatibility)"""
        DocumentHandler._render_document_list(docs_df, bill_id, bill_row_idx)

    @staticmethod
    def _remove_limit_offset_from_query(sql: str) -> str:
        """Remove LIMIT/OFFSET from query. (Backward compatibility)"""
        return DatasetExporter.remove_limit_offset_from_query(sql)

    def _render_full_dataset_download(self, modified_sql: str, safe_name: str) -> None:
        """Render full dataset download. (Backward compatibility)"""
        self._dataset_exporter.render_full_dataset_download(modified_sql, safe_name)

    def _render_table_explorer_display(self) -> None:
        """Render table explorer display. (Backward compatibility)"""
        self._table_explorer._render_display()
