"""
Dataset export utilities for query results.

This module provides the DatasetExporter class for handling full dataset
downloads and SQL query modifications.
"""

import io
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query


class DatasetExporter:
    """Handles dataset export functionality including full dataset downloads."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        """
        Initialize the dataset exporter.

        Args:
            db_path: Path to the database
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.logger = logger

    @staticmethod
    def remove_limit_offset_from_query(sql: str) -> str:
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

    def get_full_dataset_row_count(self, modified_sql: str) -> int:
        """
        Get the row count for a full dataset query (without LIMIT/OFFSET).

        Args:
            modified_sql: The SQL query with filters applied

        Returns:
            Total row count, or 0 if error
        """
        full_sql_no_limit = self.remove_limit_offset_from_query(modified_sql)
        full_count_sql = f"SELECT COUNT(*) as total FROM ({full_sql_no_limit}) as subquery"

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                count_result = safe_execute_query(con, full_count_sql, self.logger)
                # Convert to native Python int to ensure boolean comparisons work with Streamlit
                if count_result is not None and not count_result.empty:
                    return int(count_result['total'].iloc[0])
                return 0
        except Exception as e:
            self.logger.error(f"Error counting full dataset rows: {e}", exc_info=True)
            return 0

    def fetch_full_dataset(self, modified_sql: str) -> Optional[pd.DataFrame]:
        """
        Fetch the full dataset without LIMIT/OFFSET.

        Args:
            modified_sql: The SQL query with filters applied

        Returns:
            DataFrame with full results, or None if error
        """
        full_sql_no_limit = self.remove_limit_offset_from_query(modified_sql)

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                result = safe_execute_query(con, full_sql_no_limit, self.logger)
                if isinstance(result, pd.DataFrame):
                    return result
                return None
        except Exception as e:
            self.logger.error(f"Error fetching full dataset: {e}", exc_info=True)
            return None

    def render_full_dataset_download(self, modified_sql: str, safe_name: str) -> None:
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
        total_rows = self.get_full_dataset_row_count(modified_sql)

        if total_rows == 0:
            st.error("Error counting rows or no rows found")
            return

        st.info(f"📊 Total rows in filtered dataset: **{total_rows:,}**")

        if total_rows > 50000:
            st.warning(f"⚠️ Large dataset ({total_rows:,} rows). Download may take some time.")

        # Download buttons for full dataset
        col1, col2 = st.columns(2)

        with col1:
            if st.button("⬇️ Download Full CSV", disabled=(total_rows == 0), key=f"full_csv_btn_{safe_name}"):
                self._handle_full_csv_download(modified_sql, safe_name)

        with col2:
            if st.button("⬇️ Download Full Excel", disabled=(total_rows == 0), key=f"full_excel_btn_{safe_name}"):
                self._handle_full_excel_download(modified_sql, safe_name)

    def _handle_full_csv_download(self, modified_sql: str, safe_name: str) -> None:
        """Handle full CSV dataset download."""
        with st.spinner("Preparing full dataset..."):
            try:
                full_df = self.fetch_full_dataset(modified_sql)

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

    def _handle_full_excel_download(self, modified_sql: str, safe_name: str) -> None:
        """Handle full Excel dataset download."""
        with st.spinner("Preparing full dataset..."):
            try:
                full_df = self.fetch_full_dataset(modified_sql)

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
