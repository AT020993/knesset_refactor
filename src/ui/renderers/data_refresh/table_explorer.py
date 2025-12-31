"""
Table explorer UI components.

This module provides the TableExplorer class for displaying
and interacting with raw database tables.
"""

import io
import logging
import re
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

from ui.state.session_manager import SessionStateManager
import ui.ui_utils as ui_utils


class TableExplorer:
    """Handles table exploration UI and functionality."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        """
        Initialize the table explorer.

        Args:
            db_path: Path to the database
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.logger = logger

    def render_section(self) -> None:
        """Render the table explorer results section."""
        st.divider()
        st.header("📖 Interactive Table Explorer Results")

        if SessionStateManager.get_show_table_explorer_results() and SessionStateManager.get_executed_table_explorer_name():
            self._render_display()
        else:
            st.info("Explore a table from the sidebar to see its data here.")

    def _render_display(self) -> None:
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
            self._render_download_options(results_df, table_name)
        else:
            st.info("The table exploration returned no results with the current filters.")

    def _render_download_options(self, results_df: pd.DataFrame, table_name: str) -> None:
        """
        Render download options for table explorer results.

        Args:
            results_df: DataFrame with results
            table_name: Name of the table being explored
        """
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
