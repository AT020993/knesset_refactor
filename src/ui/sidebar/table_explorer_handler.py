"""
Table exploration handler for sidebar.

Handles the logic for exploring database tables with filtering and JOINs.
"""

import logging
from pathlib import Path

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query


def handle_explore_table_button_click(
    db_path: Path,
    connect_func: callable,
    get_db_table_list_func: callable,
    get_table_columns_func: callable,
    ui_logger: logging.Logger,
    format_exc_func: callable,
    faction_display_map: dict,
):
    """Handles the logic for the 'Explore Selected Table' button click.

    Args:
        db_path: Path to the database
        connect_func: Database connection function
        get_db_table_list_func: Function to get list of database tables
        get_table_columns_func: Function to get columns for a table
        ui_logger: Logger instance
        format_exc_func: Function to format exceptions
        faction_display_map: Mapping from faction names to IDs
    """
    if st.session_state.get("selected_table_for_explorer") and db_path.exists():
        table_to_explore = st.session_state.selected_table_for_explorer
        try:
            all_table_cols, _, _ = get_table_columns_func(table_to_explore)
            db_tables_list_lower = [t.lower() for t in get_db_table_list_func()]

            # Build query with potential JOINs
            query_parts = _build_explorer_query(
                table_to_explore, all_table_cols, db_tables_list_lower, faction_display_map
            )

            final_query = query_parts["query"]
            ui_logger.info(
                f"Exploring table '{table_to_explore}' with SQL: {final_query}"
            )

            with get_db_connection(
                db_path, read_only=True, logger_obj=ui_logger
            ) as con:
                st.session_state.table_explorer_df = safe_execute_query(
                    con, final_query, ui_logger
                )
                st.session_state.executed_table_explorer_name = table_to_explore
                st.session_state.show_table_explorer_results = True
                st.session_state.show_query_results = False
                st.toast(f"🔍 Explored table: {table_to_explore}", icon="📖")
        except Exception as e:
            ui_logger.error(
                f"Error exploring table '{table_to_explore}': {e}", exc_info=True
            )
            st.error(f"Error exploring table '{table_to_explore}': {e}")
            st.code(
                f"Query attempt: {final_query if 'final_query' in locals() else 'N/A'}\n\n"
                f"Error: {str(e)}\n\nTraceback:\n{format_exc_func()}"
            )
            st.session_state.show_table_explorer_results = False
            st.session_state.table_explorer_df = pd.DataFrame()
    elif not st.session_state.get("selected_table_for_explorer"):
        st.warning("Please select a table to explore.")
    elif not db_path.exists():
        st.error("Database not found. Cannot explore tables.")


def _build_explorer_query(
    table_to_explore: str,
    all_table_cols: list,
    db_tables_list_lower: list,
    faction_display_map: dict,
) -> dict:
    """Build the explorer query with JOINs and filters.

    Args:
        table_to_explore: Name of table to explore
        all_table_cols: List of columns in the table
        db_tables_list_lower: List of all table names (lowercase)
        faction_display_map: Mapping from faction names to IDs

    Returns:
        Dictionary with 'query' key containing the final SQL
    """
    join_clause = ""
    select_prefix = f'"{table_to_explore}".*'
    base_query_table_ref = f'"{table_to_explore}"'

    # Handle special cases for faction-related tables
    if (
        table_to_explore.lower() == "kns_faction"
        and "userfactioncoalitionstatus" in db_tables_list_lower
    ):
        select_prefix = (
            "f.*, ufs.CoalitionStatus AS UserCoalitionStatus, "
            "ufs.DateJoinedCoalition, ufs.DateLeftCoalition"
        )
        base_query_table_ref = "KNS_Faction f"
        join_clause = (
            "LEFT JOIN UserFactionCoalitionStatus ufs "
            "ON f.FactionID = ufs.FactionID AND f.KnessetNum = ufs.KnessetNum"
        )
    elif (
        table_to_explore.lower() == "kns_persontoposition"
        and "userfactioncoalitionstatus" in db_tables_list_lower
    ):
        select_prefix = (
            "p2p.*, ufs.CoalitionStatus AS UserCoalitionStatus, "
            "ufs.DateJoinedCoalition, ufs.DateLeftCoalition"
        )
        base_query_table_ref = "KNS_PersonToPosition p2p"
        join_clause = (
            "LEFT JOIN UserFactionCoalitionStatus ufs "
            "ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum"
        )

    base_query = f"SELECT {select_prefix} FROM {base_query_table_ref}"

    # Determine table alias for filter columns
    table_alias = _get_table_alias(table_to_explore, join_clause)

    # Build WHERE clauses from session state filters
    where_clauses = _build_where_clauses(
        all_table_cols, table_alias, faction_display_map
    )

    # Assemble final query
    final_query = base_query
    if join_clause:
        final_query += f" {join_clause}"
    if where_clauses:
        final_query += " WHERE " + " AND ".join(where_clauses)

    # Add ORDER BY clause
    order_by_col = _get_order_by_column(all_table_cols, table_alias)
    if order_by_col:
        final_query += f" ORDER BY {order_by_col} DESC"

    final_query += " LIMIT 1000"

    return {"query": final_query}


def _get_table_alias(table_to_explore: str, join_clause: str) -> str:
    """Get the appropriate table alias for filter columns."""
    if table_to_explore.lower() == "kns_faction" and join_clause:
        return "f."
    elif table_to_explore.lower() == "kns_persontoposition" and join_clause:
        return "p2p."
    else:
        return f'"{table_to_explore}".'


def _build_where_clauses(
    all_table_cols: list, table_alias: str, faction_display_map: dict
) -> list:
    """Build WHERE clauses from session state filters."""
    where_clauses = []

    # Knesset filter
    actual_knesset_col = next(
        (col for col in all_table_cols if col.lower() == "knessetnum"), None
    )
    if actual_knesset_col and st.session_state.get("ms_knesset_filter"):
        knesset_col_name = f'{table_alias}"{actual_knesset_col}"'
        where_clauses.append(
            f"{knesset_col_name} IN ({', '.join(map(str, st.session_state.ms_knesset_filter))})"
        )

    # Faction filter
    actual_faction_col = next(
        (col for col in all_table_cols if col.lower() == "factionid"), None
    )
    if actual_faction_col and st.session_state.get("ms_faction_filter"):
        faction_col_name = f'{table_alias}"{actual_faction_col}"'
        selected_faction_ids = [
            faction_display_map[name]
            for name in st.session_state.ms_faction_filter
            if name in faction_display_map
        ]
        if selected_faction_ids:
            where_clauses.append(
                f"{faction_col_name} IN ({', '.join(map(str, selected_faction_ids))})"
            )

    return where_clauses


def _get_order_by_column(all_table_cols: list, table_alias: str) -> str:
    """Get the appropriate ORDER BY column."""
    # Check for preferred date columns
    if next(
        (col for col in all_table_cols if col.lower() == "lastupdateddate"), None
    ):
        return f'{table_alias}"LastUpdatedDate"'
    elif next((col for col in all_table_cols if col.lower() == "startdate"), None):
        return f'{table_alias}"StartDate"'
    elif all_table_cols:
        return f'{table_alias}"{all_table_cols[0]}"'
    return ""
