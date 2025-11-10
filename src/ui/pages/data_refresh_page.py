"""
Main data refresh page UI components.

This module contains the primary UI rendering logic for the data refresh page,
separated from business logic and session state management.
"""

import io
import logging
import re
import sys
from pathlib import Path
from textwrap import dedent
from typing import Dict, Any, Callable, Optional, List

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from ui.state.session_manager import SessionStateManager
from ui.queries.predefined_queries import get_all_query_names, get_query_info
import ui.ui_utils as ui_utils


class DataRefreshPageRenderer:
    """Handles rendering of the main data refresh page components."""
    
    def __init__(self, db_path: Path, logger: logging.Logger):
        """
        Initialize the page renderer.
        
        Args:
            db_path: Path to the database
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.logger = logger

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
            # Display results with formatted dates
            formatted_df = ui_utils.format_dataframe_dates(display_df, _logger_obj=self.logger)
            st.dataframe(formatted_df, use_container_width=True, height=400)

            # Download options
            safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", query_name)
            col_csv, col_excel = st.columns(2)

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
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                    display_df.to_excel(writer, index=False, sheet_name="Results")
                st.download_button(
                    "⬇️ Excel",
                    excel_buffer.getvalue(),
                    f"{safe_name}_results.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"excel_dl_{safe_name}"
                )

            # Full Dataset Download Section
            self._render_full_dataset_download(last_sql, safe_name)
        else:
            st.info("The query returned no results with the current filters.")

        # Show executed SQL
        with st.expander("Show Executed SQL", expanded=False):
            st.code(last_sql if last_sql else "No SQL executed yet.", language="sql")

    @staticmethod
    def _remove_limit_offset_from_query(sql: str) -> str:
        """
        Remove LIMIT and OFFSET clauses from SQL query.

        Args:
            sql: SQL query string with LIMIT/OFFSET

        Returns:
            Modified SQL query without LIMIT/OFFSET clauses
        """
        # Remove LIMIT clause (handles "LIMIT 1000" or "LIMIT 1000 OFFSET 1000")
        sql = re.sub(r'\s+LIMIT\s+\d+', '', sql, flags=re.IGNORECASE)
        # Remove standalone OFFSET clause
        sql = re.sub(r'\s+OFFSET\s+\d+', '', sql, flags=re.IGNORECASE)
        return sql.strip()

    def _render_full_dataset_download(self, modified_sql: str, safe_name: str) -> None:
        """
        Render the full dataset download section.

        Args:
            modified_sql: The SQL query with filters applied
            safe_name: Sanitized query name for file naming
        """
        st.markdown("---")
        st.markdown("### 📦 Download Full Filtered Dataset")
        st.caption("⚠️ This will download ALL rows matching your filters (not just 1000 displayed)")

        # Get row count for full dataset
        full_sql_no_limit = self._remove_limit_offset_from_query(modified_sql)
        full_count_sql = f"SELECT COUNT(*) as total FROM ({full_sql_no_limit}) as subquery"

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                count_result = safe_execute_query(con, full_count_sql, self.logger)
                total_rows = count_result['total'].iloc[0] if count_result is not None and not count_result.empty else 0
        except Exception as e:
            self.logger.error(f"Error counting full dataset rows: {e}", exc_info=True)
            st.error(f"Error counting rows: {e}")
            total_rows = 0

        st.info(f"📊 Total rows in filtered dataset: **{total_rows:,}**")

        if total_rows > 50000:
            st.warning(f"⚠️ Large dataset ({total_rows:,} rows). Download may take some time.")

        # Download buttons for full dataset
        col1, col2 = st.columns(2)

        with col1:
            if st.button("⬇️ Download Full CSV", disabled=(total_rows == 0), key=f"full_csv_btn_{safe_name}"):
                with st.spinner("Preparing full dataset..."):
                    try:
                        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                            full_df = safe_execute_query(con, full_sql_no_limit, self.logger)

                        if full_df is not None and not full_df.empty:
                            csv_data = full_df.to_csv(index=False).encode("utf-8-sig")
                            st.download_button(
                                "💾 Click to Save Full CSV",
                                csv_data,
                                f"{safe_name}_FULL_results.csv",
                                "text/csv",
                                key=f"full_csv_download_{safe_name}"
                            )
                            st.success(f"✅ Prepared {len(full_df):,} rows for download")
                        else:
                            st.error("Failed to retrieve full dataset")
                    except Exception as e:
                        self.logger.error(f"Error preparing full CSV: {e}", exc_info=True)
                        st.error(f"Error preparing CSV: {e}")

        with col2:
            if st.button("⬇️ Download Full Excel", disabled=(total_rows == 0), key=f"full_excel_btn_{safe_name}"):
                with st.spinner("Preparing full dataset..."):
                    try:
                        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                            full_df = safe_execute_query(con, full_sql_no_limit, self.logger)

                        if full_df is not None and not full_df.empty:
                            excel_buffer = io.BytesIO()
                            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                                full_df.to_excel(writer, index=False, sheet_name="Results")
                            st.download_button(
                                "💾 Click to Save Full Excel",
                                excel_buffer.getvalue(),
                                f"{safe_name}_FULL_results.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"full_excel_download_{safe_name}"
                            )
                            st.success(f"✅ Prepared {len(full_df):,} rows for download")
                        else:
                            st.error("Failed to retrieve full dataset")
                    except Exception as e:
                        self.logger.error(f"Error preparing full Excel: {e}", exc_info=True)
                        st.error(f"Error preparing Excel: {e}")

    def _apply_knesset_filter_callback(self):
        """Callback function to apply the Knesset filter."""
        # Update the actual filter based on temp selection
        if st.session_state.temp_knesset_filter == "All Knessetes":
            st.session_state.ms_knesset_filter = []
        else:
            knesset_num = int(st.session_state.temp_knesset_filter.replace("Knesset ", ""))
            st.session_state.ms_knesset_filter = [knesset_num]

        # Reset pagination when filter changes
        st.session_state.query_page_number = 1
        st.session_state.query_page_offset = 0

        # Re-execute the query immediately with updated filter
        query_name = SessionStateManager.get_executed_query_name()
        if query_name:
            from ui.sidebar_components import _handle_run_query_button_click
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
                st.rerun()

        with col3:
            # Next page button
            has_more = len(results_df) == rows_per_page  # If we got full 1000 rows, there might be more
            if st.button("Next ▶", key="next_page_btn", disabled=not has_more, use_container_width=True):
                st.session_state.query_page_number = current_page + 1
                st.session_state.query_page_offset = (st.session_state.query_page_number - 1) * 1000
                self._rerun_query_with_pagination()
                st.rerun()

        with col4:
            # Reset to first page button
            if current_page > 1:
                if st.button("⏮ First Page", key="first_page_btn", use_container_width=True):
                    st.session_state.query_page_number = 1
                    st.session_state.query_page_offset = 0
                    self._rerun_query_with_pagination()
                    st.rerun()

    def _rerun_query_with_pagination(self):
        """Re-execute the current query with updated pagination offset."""
        query_name = SessionStateManager.get_executed_query_name()
        if query_name:
            from ui.sidebar_components import _handle_run_query_button_click
            from ui.queries.predefined_queries import PREDEFINED_QUERIES

            _handle_run_query_button_click(
                exports_dict=PREDEFINED_QUERIES,
                db_path=self.db_path,
                connect_func=lambda read_only=True: ui_utils.connect_db(self.db_path, read_only, self.logger),
                ui_logger=self.logger,
                format_exc_func=ui_utils.format_exception_for_ui,
                faction_display_map=st.session_state.get("faction_display_map", {})
            )

    def _get_query_type_from_name(self, query_name: str) -> str:
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
        st.divider()
        st.header("📖 Interactive Table Explorer Results")
        
        if SessionStateManager.get_show_table_explorer_results() and SessionStateManager.get_executed_table_explorer_name():
            self._render_table_explorer_display()
        else:
            st.info("Explore a table from the sidebar to see its data here.")

    def _render_table_explorer_display(self) -> None:
        """Render the actual table explorer results with download options."""
        table_name = SessionStateManager.get_executed_table_explorer_name()
        results_df = SessionStateManager.get_table_explorer_df()
        
        st.subheader(f"Exploring: **{table_name}**")
        
        # Show applied filters
        k_filters = SessionStateManager.get_knesset_filter()
        f_filters = SessionStateManager.get_faction_filter()
        
        filter_display_parts = []
        if k_filters:
            filter_display_parts.append(f"Knesset(s): `{', '.join(map(str, k_filters))}`")
        else:
            filter_display_parts.append("Knesset(s): `All`")
        
        if f_filters:
            filter_display_parts.append(f"Faction(s): `{', '.join(f_filters)}`")
        else:
            filter_display_parts.append("Faction(s): `All`")
        
        st.markdown(f"Active Sidebar Filters: {'; '.join(filter_display_parts)}")
        
        if not results_df.empty:
            # Display results with formatted dates
            formatted_df = ui_utils.format_dataframe_dates(results_df, _logger_obj=self.logger)
            st.dataframe(formatted_df, use_container_width=True, height=400)
            
            # Download options
            safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", table_name)
            col_csv_ex, col_excel_ex = st.columns(2)
            
            with col_csv_ex:
                csv_data = results_df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "⬇️ CSV", 
                    csv_data, 
                    f"{safe_name}_data.csv", 
                    "text/csv", 
                    key=f"csv_dl_ex_{safe_name}"
                )
            
            with col_excel_ex:
                excel_buffer_ex = io.BytesIO()
                with pd.ExcelWriter(excel_buffer_ex, engine="openpyxl") as writer:
                    results_df.to_excel(writer, index=False, sheet_name="TableData")
                st.download_button(
                    "⬇️ Excel", 
                    excel_buffer_ex.getvalue(), 
                    f"{safe_name}_data.xlsx", 
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                    key=f"excel_dl_ex_{safe_name}"
                )
        else:
            st.info("The table exploration returned no results with the current filters.")

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
        st.divider()
        with st.expander("🗓️ Table Update Status (Click to Expand)", expanded=False):
            if self.db_path.exists():
                tables_to_check = sorted(list(set(tables_list)))
                status_data = [
                    {
                        "Table": table_name, 
                        "Last Updated (Parquet Mod Time)": ui_utils.get_last_updated_for_table(
                            parquet_dir, table_name, self.logger
                        )
                    }
                    for table_name in tables_to_check
                ]
                
                if status_data:
                    st.dataframe(pd.DataFrame(status_data), hide_index=True, use_container_width=True)
                else:
                    st.info("No tables found to display status, or TABLES list is empty.")
            else:
                st.info("Database not found. Table status cannot be displayed.")