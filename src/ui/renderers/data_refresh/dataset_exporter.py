"""
Dataset export utilities for query results.

This module provides the DatasetExporter class for handling full dataset
downloads and SQL query modifications.
"""

import io
import logging
import re
import tempfile
from pathlib import Path
from typing import Any, Optional, Sequence

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
        return sql.strip().rstrip(";").strip()

    @staticmethod
    def _find_top_level_keyword(sql: str, keyword: str) -> int:
        """Find a keyword outside parentheses and string literals."""
        keyword_lower = keyword.lower()
        depth = 0
        quote: str | None = None
        i = 0

        while i < len(sql):
            char = sql[i]

            if quote:
                if char == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
                i += 1
                continue

            if char in {"'", '"'}:
                quote = char
                i += 1
                continue

            if char == "(":
                depth += 1
                i += 1
                continue
            if char == ")":
                depth = max(0, depth - 1)
                i += 1
                continue

            if depth == 0 and sql[i : i + len(keyword)].lower() == keyword_lower:
                before = sql[i - 1] if i > 0 else " "
                after_index = i + len(keyword)
                after = sql[after_index] if after_index < len(sql) else " "
                if not (before.isalnum() or before == "_") and not (
                    after.isalnum() or after == "_"
                ):
                    return i

            i += 1

        return -1

    @classmethod
    def remove_top_level_order_by(cls, sql: str) -> str:
        """Remove only the final top-level ORDER BY clause from a SELECT query."""
        stripped = sql.strip().rstrip(";")
        order_index = cls._find_top_level_keyword(stripped, "ORDER BY")
        if order_index == -1:
            return stripped
        return stripped[:order_index].strip()

    @classmethod
    def build_count_query(cls, modified_sql: str) -> str:
        """Build a memory-efficient COUNT query for a displayed SQL statement."""
        full_sql_no_limit = cls.remove_limit_offset_from_query(modified_sql)
        count_sql = cls.remove_top_level_order_by(full_sql_no_limit)
        return f"SELECT COUNT(*) as total FROM ({count_sql}) as subquery"

    @staticmethod
    def _quote_duckdb_path(path: Path) -> str:
        return "'" + path.as_posix().replace("'", "''") + "'"

    def get_full_dataset_row_count(
        self, modified_sql: str, params: Optional[Sequence[Any]] = None
    ) -> int:
        """
        Get the row count for a full dataset query (without LIMIT/OFFSET).

        Args:
            modified_sql: The SQL query with filters applied
            params: Bound parameter values for filter placeholders

        Returns:
            Total row count, or 0 if error
        """
        full_count_sql = self.build_count_query(modified_sql)

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                count_result = safe_execute_query(
                    con, full_count_sql, self.logger, params=list(params) if params else None
                )
                # Convert to native Python int to ensure boolean comparisons work with Streamlit
                if count_result is not None and not count_result.empty:
                    return int(count_result['total'].iloc[0])
                return 0
        except Exception as e:
            self.logger.error(f"Error counting full dataset rows: {e}", exc_info=True)
            return 0

    def fetch_full_dataset(
        self, modified_sql: str, params: Optional[Sequence[Any]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch the full dataset without LIMIT/OFFSET.

        Args:
            modified_sql: The SQL query with filters applied
            params: Bound parameter values for filter placeholders

        Returns:
            DataFrame with full results, or None if error
        """
        full_sql_no_limit = self.remove_limit_offset_from_query(modified_sql)

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                result = safe_execute_query(
                    con, full_sql_no_limit, self.logger, params=list(params) if params else None
                )
                if isinstance(result, pd.DataFrame):
                    return result
                return None
        except Exception as e:
            self.logger.error(f"Error fetching full dataset: {e}", exc_info=True)
            return None

    def export_full_dataset_csv_bytes(
        self, modified_sql: str, params: Optional[Sequence[Any]] = None
    ) -> tuple[bytes, int]:
        """Export the full result set to CSV without materializing a pandas DataFrame."""
        full_sql_no_limit = self.remove_limit_offset_from_query(modified_sql)
        row_count = self.get_full_dataset_row_count(modified_sql, params)
        temp_path: Path | None = None

        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                temp_path = Path(temp_file.name)

            copy_sql = (
                f"COPY ({full_sql_no_limit}) TO {self._quote_duckdb_path(temp_path)} "
                "(HEADER, DELIMITER ',')"
            )
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if params:
                    con.execute(copy_sql, list(params))
                else:
                    con.execute(copy_sql)

            csv_data = temp_path.read_bytes()
            if not csv_data.startswith(b"\xef\xbb\xbf"):
                csv_data = b"\xef\xbb\xbf" + csv_data
            return csv_data, row_count
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    def render_full_dataset_download(
        self,
        modified_sql: str,
        safe_name: str,
        params: Optional[Sequence[Any]] = None,
    ) -> None:
        """
        Render the full dataset download section.

        Args:
            modified_sql: The SQL query with filters applied
            safe_name: Sanitized query name for file naming
            params: Bound parameter values for filter placeholders
        """
        st.markdown("---")
        st.markdown("### 📦 Download Full Filtered Dataset")
        st.caption("⚠️ This will download ALL rows matching your filters (not just 1000 displayed)")

        # Get row count for full dataset
        total_rows = self.get_full_dataset_row_count(modified_sql, params)

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
                self._handle_full_csv_download(modified_sql, safe_name, params)

        with col2:
            if st.button("⬇️ Download Full Excel", disabled=(total_rows == 0), key=f"full_excel_btn_{safe_name}"):
                self._handle_full_excel_download(modified_sql, safe_name, params)

    def _handle_full_csv_download(
        self, modified_sql: str, safe_name: str, params: Optional[Sequence[Any]] = None
    ) -> None:
        """Handle full CSV dataset download."""
        with st.spinner("Preparing full dataset..."):
            try:
                csv_data, row_count = self.export_full_dataset_csv_bytes(
                    modified_sql, params
                )

                if csv_data:
                    st.download_button(
                        "💾 Click to Save Full CSV",
                        csv_data,
                        f"{safe_name}_FULL_results.csv",
                        "text/csv",
                        key=f"full_csv_download_{safe_name}"
                    )
                    st.success(f"✅ Prepared {row_count:,} rows for download")
                else:
                    st.error("Failed to retrieve full dataset")
            except Exception as e:
                self.logger.error(f"Error preparing full CSV: {e}", exc_info=True)
                st.error(f"Error preparing CSV: {e}")

    def _handle_full_excel_download(
        self, modified_sql: str, safe_name: str, params: Optional[Sequence[Any]] = None
    ) -> None:
        """Handle full Excel dataset download."""
        with st.spinner("Preparing full dataset..."):
            try:
                full_df = self.fetch_full_dataset(modified_sql, params)

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
