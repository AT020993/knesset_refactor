"""
Query execution handler for sidebar.

Handles the logic for running predefined SQL queries with filter application.
"""

import logging
import re
from pathlib import Path

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query


def handle_run_query_button_click(
    exports_dict: dict,
    db_path: Path,
    connect_func: callable,
    ui_logger: logging.Logger,
    format_exc_func: callable,
    faction_display_map: dict,
):
    """Handles the logic for the 'Run Selected Query' button click.

    Args:
        exports_dict: Dictionary of available query templates
        db_path: Path to the database
        connect_func: Database connection function
        ui_logger: Logger instance
        format_exc_func: Function to format exceptions
        faction_display_map: Mapping from faction names to IDs
    """
    # Access the session state key directly
    if st.session_state.get("selected_query_name") and db_path.exists():
        try:
            # Reset pagination to first page unless explicitly navigating pages
            if not st.session_state.get("query_page_offset", 0):
                st.session_state.query_page_number = 1
                st.session_state.query_page_offset = 0

            query_info = exports_dict[st.session_state.selected_query_name]
            base_sql = query_info["sql"]
            knesset_filter_col = query_info.get("knesset_filter_column")
            faction_filter_col = query_info.get("faction_filter_column")
            modified_sql = base_sql.strip().rstrip(";")
            applied_filters_info = []
            where_conditions = []

            # Apply filters if specified
            ui_logger.debug(
                f"Applying filters - Knesset: {st.session_state.get('ms_knesset_filter', [])}, "
                f"Faction: {st.session_state.get('ms_faction_filter', [])}"
            )

            where_conditions, applied_filters_info = _build_filter_conditions(
                knesset_filter_col,
                faction_filter_col,
                faction_display_map,
                ui_logger,
            )

            if where_conditions:
                modified_sql = _apply_where_conditions(
                    modified_sql, where_conditions, ui_logger
                )

            # Add OFFSET for pagination if needed
            modified_sql = _apply_pagination(modified_sql, ui_logger)

            if applied_filters_info:
                ui_logger.info(f"Applied filters: {', '.join(applied_filters_info)}")
            else:
                ui_logger.info("No filters applied")

            ui_logger.info(
                f"Executing predefined query: {st.session_state.selected_query_name}"
            )
            ui_logger.info(f"Final SQL:\n{modified_sql}")

            with get_db_connection(
                db_path, read_only=True, logger_obj=ui_logger
            ) as con:
                st.session_state.query_results_df = safe_execute_query(
                    con, modified_sql, ui_logger
                )
                st.session_state.executed_query_name = (
                    st.session_state.selected_query_name
                )
                st.session_state.show_query_results = True
                st.session_state.show_table_explorer_results = False
                st.session_state.applied_filters_info_query = applied_filters_info
                st.session_state.last_executed_sql = modified_sql
                st.toast(
                    f"✅ Query '{st.session_state.executed_query_name}' executed.",
                    icon="📊",
                )
        except Exception as e:
            ui_logger.error(
                f"Error executing query '{st.session_state.selected_query_name}': {e}",
                exc_info=True,
            )
            ui_logger.error(
                f"Failed SQL for '{st.session_state.selected_query_name}':\n"
                f"{modified_sql if 'modified_sql' in locals() else base_sql}"
            )
            st.error(
                f"Error executing query '{st.session_state.selected_query_name}': {e}"
            )
            st.code(str(e) + "\n\n" + format_exc_func())
            st.session_state.show_query_results = False
            st.session_state.query_results_df = pd.DataFrame()
    elif not db_path.exists():
        st.error(
            "Database not found. Please ensure 'data/warehouse.duckdb' exists or run data refresh."
        )
        st.session_state.show_query_results = False


def _build_filter_conditions(
    knesset_filter_col: str,
    faction_filter_col: str,
    faction_display_map: dict,
    ui_logger: logging.Logger,
) -> tuple[list, list]:
    """Build WHERE conditions from session state filters.

    Returns:
        Tuple of (where_conditions, applied_filters_info)
    """
    where_conditions = []
    applied_filters_info = []

    # Knesset filter
    if knesset_filter_col and st.session_state.get("ms_knesset_filter"):
        selected_knesset_nums = st.session_state.ms_knesset_filter
        where_conditions.append(
            f"{knesset_filter_col} IN ({', '.join(map(str, selected_knesset_nums))})"
        )
        applied_filters_info.append(
            f"KnessetNum IN ({', '.join(map(str, selected_knesset_nums))})"
        )

    # Faction filter
    if (
        faction_filter_col
        and faction_filter_col != "NULL"
        and st.session_state.get("ms_faction_filter")
    ):
        selected_faction_names = st.session_state.ms_faction_filter
        selected_faction_ids = []
        missing_factions = []

        for name in selected_faction_names:
            if name in faction_display_map:
                selected_faction_ids.append(faction_display_map[name])
            else:
                missing_factions.append(name)

        if missing_factions:
            ui_logger.warning(f"Missing factions in display map: {missing_factions}")

        if selected_faction_ids:
            where_conditions.append(
                f"{faction_filter_col} IN ({', '.join(map(str, selected_faction_ids))})"
            )
            applied_filters_info.append(
                f"FactionID IN ({', '.join(map(str, selected_faction_ids))})"
            )

    # Document type filter (for bill queries only)
    if st.session_state.get("ms_document_type_filter") and "bill" in st.session_state.get(
        "selected_query_name", ""
    ).lower():
        doc_conditions, doc_info = _build_document_filter_conditions()
        if doc_conditions:
            where_conditions.append(doc_conditions)
            applied_filters_info.append(doc_info)

    return where_conditions, applied_filters_info


def _build_document_filter_conditions() -> tuple[str, str]:
    """Build document type filter conditions.

    Returns:
        Tuple of (combined_condition, filter_info)
    """
    selected_doc_types = st.session_state.ms_document_type_filter
    doc_type_conditions = []

    for doc_type in selected_doc_types:
        if doc_type == "Published Law":
            doc_type_conditions.append("BillPublishedLawDocCount > 0")
        elif doc_type == "First Reading":
            doc_type_conditions.append("BillFirstReadingDocCount > 0")
        elif doc_type in ["2nd/3rd Reading", "Second & Third Reading"]:
            doc_type_conditions.append("BillSecondThirdReadingDocCount > 0")
        elif doc_type in ["Early Discussion", "Early Stage Discussion"]:
            doc_type_conditions.append("BillEarlyDiscussionDocCount > 0")
        elif doc_type == "Other":
            doc_type_conditions.append("BillOtherDocCount > 0")

    if doc_type_conditions:
        combined_doc_filter = f"({' OR '.join(doc_type_conditions)})"
        return combined_doc_filter, f"Document Types: {', '.join(selected_doc_types)}"

    return "", ""


def _apply_where_conditions(
    sql: str, where_conditions: list, ui_logger: logging.Logger
) -> str:
    """Apply WHERE conditions to SQL query.

    Handles CTE-based queries by finding the main SELECT.
    """
    combined_where_clause = " AND ".join(where_conditions)

    try:
        # Find insertion point before GROUP BY, ORDER BY, etc.
        insertion_point = len(sql)
        main_query_start = 0

        if "WITH " in sql.upper():
            # For CTE queries, find the main SELECT after the CTE definitions
            cte_end_patterns = [r"\)\s*SELECT\b"]
            for pattern in cte_end_patterns:
                matches = list(re.finditer(pattern, sql, re.IGNORECASE))
                if matches:
                    main_query_start = matches[-1].end()
                    break

        if main_query_start < 0 or main_query_start >= len(sql):
            raise ValueError(f"Invalid main query start position: {main_query_start}")

        # Check for WHERE clause ONLY in the main query portion
        main_query_portion = sql[main_query_start:]
        has_where_clause = re.search(r"\bWHERE\b", main_query_portion, re.IGNORECASE)
        keyword_to_use = "AND" if has_where_clause else "WHERE"
        filter_string_to_add = f" {keyword_to_use} {combined_where_clause}"

        # Define clauses to look for after the main SELECT
        clauses_keywords = [
            r"\bGROUP\s+BY\b",
            r"\bHAVING\b",
            r"\bWINDOW\b",
            r"\bORDER\s+BY\b",
            r"\bLIMIT\b",
            r"\bOFFSET\b",
            r"\bFETCH\b",
        ]

        for pattern_str in clauses_keywords:
            matches = list(
                re.finditer(pattern_str, sql[main_query_start:], re.IGNORECASE)
            )
            if matches:
                match_pos = main_query_start + matches[0].start()
                if match_pos < insertion_point:
                    insertion_point = match_pos

        if insertion_point < main_query_start or insertion_point > len(sql):
            raise ValueError(f"Invalid insertion point: {insertion_point}")

        prefix = sql[:insertion_point].strip()
        suffix = sql[insertion_point:].strip()

        if suffix:
            return f"{prefix}{filter_string_to_add} {suffix}".strip()
        else:
            return f"{prefix}{filter_string_to_add}".strip()

    except (ValueError, IndexError, AttributeError) as e:
        ui_logger.warning(
            f"Failed to apply filters via regex manipulation: {e}. "
            "Continuing with unmodified query."
        )
        return sql


def _apply_pagination(sql: str, ui_logger: logging.Logger) -> str:
    """Apply pagination OFFSET to SQL if needed."""
    page_offset = st.session_state.get("query_page_offset", 0)
    if page_offset > 0:
        limit_match = re.search(r"\bLIMIT\s+\d+", sql, re.IGNORECASE)
        if limit_match:
            limit_pos = limit_match.start()
            prefix = sql[:limit_pos].strip()
            suffix = sql[limit_pos:].strip()
            sql = f"{prefix} OFFSET {page_offset} {suffix}".strip()
            ui_logger.info(f"Added pagination OFFSET {page_offset}")
    return sql
