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

from ui.state.session_manager import SessionStateManager
from ui.queries.predefined_queries import get_all_query_names
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
        st.title("🇮🇱 Knesset Data Warehouse Console")
        
        with st.expander("ℹ️ How This Works", expanded=False):
            st.markdown(dedent("""
                * **Data Refresh:** Use sidebar controls to fetch OData tables or update faction statuses.
                * **Predefined Queries & Table Explorer:** These sections use the **sidebar filters** for Knesset and Faction.
                * **Predefined Visualizations:**
                    * Select a plot topic, then a specific plot.
                    * **A Knesset selector will appear below these dropdowns.** This is the primary Knesset filter for the plots.
                    * For plots like "Queries/Agendas by Time Period", you can select "All Knessets (Color Coded)" to see multiple Knessets, or pick a specific one. Other plots will typically focus on the single selected Knesset.
                    * Time-based plots also offer aggregation level and average line options.
                * **Interactive Chart Builder:** Data for charts is filtered by sidebar selections and then by chart-specific filters.
                * **Ad-hoc SQL:** Use the sandbox at the bottom to run custom SQL.
            """))

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
        
        if not results_df.empty:
            # Display results
            st.dataframe(results_df, use_container_width=True, height=400)
            
            # Download options
            safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", query_name)
            col_csv, col_excel = st.columns(2)
            
            with col_csv:
                csv_data = results_df.to_csv(index=False).encode("utf-8-sig")
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
                    results_df.to_excel(writer, index=False, sheet_name="Results")
                st.download_button(
                    "⬇️ Excel", 
                    excel_buffer.getvalue(), 
                    f"{safe_name}_results.xlsx", 
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                    key=f"excel_dl_{safe_name}"
                )
        else:
            st.info("The query returned no results with the current filters.")
        
        # Show executed SQL
        with st.expander("Show Executed SQL", expanded=False):
            st.code(last_sql if last_sql else "No SQL executed yet.", language="sql")

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
            # Display results
            st.dataframe(results_df, use_container_width=True, height=400)
            
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
            
            st.dataframe(adhoc_result_df, use_container_width=True)
            
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