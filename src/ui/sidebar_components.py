from __future__ import annotations

# Standard Library Imports
import asyncio
import logging
import re
from pathlib import Path

# Third-Party Imports
import streamlit as st
import pandas as pd  # Required for type hinting if df is passed around

# Add the 'src' directory to sys.path to allow absolute imports
# This might be needed if this file is run in certain contexts,
# but typically Streamlit handles the root path well.
# However, for backend imports, ensuring sys.path is correct is crucial.
import sys

_CURRENT_FILE_DIR = Path(__file__).resolve().parent
_SRC_DIR = _CURRENT_FILE_DIR.parent
_PROJECT_ROOT = _SRC_DIR.parent

if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Local Application Imports
# Assuming these are accessible from the new location
from backend.fetch_table import TABLES  # type: ignore
import backend.fetch_table as ft  # type: ignore
from backend.connection_manager import get_db_connection, safe_execute_query
# Assuming _connect, get_db_table_list, get_filter_options_from_db, faction_display_map,
# EXPORTS, DB_PATH, _format_exc, ui_logger are defined in data_refresh or a shared ui_utils
# For now, we'll pass them as arguments or assume they are accessible via st.session_state or globally if not refactored out.

# Constants from data_refresh.py that are needed here
_SELECT_ALL_TABLES_OPTION = "🔄 Select/Deselect All Tables"


def _handle_data_refresh_button_click(
    db_path: Path, ui_logger: logging.Logger, format_exc_func: callable
):
    """Handles the logic for the 'Refresh Selected Data' button click."""
    if st.session_state.get("data_refresh_process_running", False):
        st.sidebar.warning("Refresh process is already running.")
        return

    all_tables_selected = st.session_state.get("ms_tables_to_refresh", [])
    if not all_tables_selected:
        st.sidebar.warning("No tables selected for refresh.")
        return

    tables_to_run = [t for t in all_tables_selected if t != _SELECT_ALL_TABLES_OPTION]
    if _SELECT_ALL_TABLES_OPTION in all_tables_selected or not tables_to_run:
        tables_to_run = TABLES
    if not tables_to_run:
        st.sidebar.info("No tables are defined or selected for refresh.")
        return

    st.session_state.data_refresh_process_running = True
    ui_logger.info(f"Starting data refresh for tables: {tables_to_run}")
    progress_bar_sidebar = st.sidebar.progress(0, text="Preparing refresh...")
    status_text_sidebar = st.sidebar.empty()
    status_text_sidebar.text("Initializing refresh...")

    def _sidebar_progress_cb(table_name_done: str, num_rows_fetched: int):
        total_tables = len(tables_to_run)
        if "completed_tables_count" not in st.session_state:
            st.session_state.completed_tables_count = 0
        st.session_state.completed_tables_count += 1
        percentage = (st.session_state.completed_tables_count / total_tables) * 100
        message = f"Fetched {num_rows_fetched} rows for {table_name_done}. ({st.session_state.completed_tables_count}/{total_tables} tables done)"
        # Clamp percentage to 100 to prevent errors from state inconsistencies
        progress_bar_sidebar.progress(min(int(percentage), 100), text=message)
        status_text_sidebar.text(message)
        ui_logger.info(message)

    async def _refresh_async_wrapper(tables_list_async):
        st.session_state.completed_tables_count = 0
        await ft.refresh_tables(
            tables=tables_list_async, progress_cb=_sidebar_progress_cb, db_path=db_path
        )

    try:
        asyncio.run(_refresh_async_wrapper(tables_to_run))
        st.sidebar.success("Data refresh process complete!")
        status_text_sidebar.success("All selected tables refreshed successfully.")
        progress_bar_sidebar.progress(100, text="Refresh complete!")
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    except Exception as e:
        ui_logger.error(f"❌ Data Refresh Error: {e}", exc_info=True)
        st.sidebar.error(f"❌ Data Refresh Error: {e}")
        st.sidebar.code(f"Error: {str(e)}\n\nTraceback:\n{format_exc_func()}")
        status_text_sidebar.error(f"Error during refresh: {e}")
        progress_bar_sidebar.progress(0, text=f"Error: {e}")
    finally:
        st.session_state.data_refresh_process_running = False
        if "completed_tables_count" in st.session_state:
            del st.session_state.completed_tables_count


def _handle_run_query_button_click(
    exports_dict: dict,
    db_path: Path,
    connect_func: callable,
    ui_logger: logging.Logger,
    format_exc_func: callable,
    faction_display_map: dict,
):
    """Handles the logic for the 'Run Selected Query' button click."""
    if st.session_state.selected_query_name and db_path.exists():
        try:
            query_info = exports_dict[st.session_state.selected_query_name]
            base_sql = query_info["sql"]
            knesset_filter_col = query_info.get("knesset_filter_column")
            faction_filter_col = query_info.get("faction_filter_column")
            modified_sql = base_sql.strip().rstrip(";")
            applied_filters_info = []
            where_conditions = []
            
            # Apply filters if specified
            ui_logger.debug(f"Applying filters - Knesset: {st.session_state.get('ms_knesset_filter', [])}, Faction: {st.session_state.get('ms_faction_filter', [])}")

            if knesset_filter_col and st.session_state.ms_knesset_filter:
                selected_knesset_nums = st.session_state.ms_knesset_filter
                where_conditions.append(
                    f"{knesset_filter_col} IN ({', '.join(map(str, selected_knesset_nums))})"
                )
                applied_filters_info.append(
                    f"KnessetNum IN ({', '.join(map(str, selected_knesset_nums))})"
                )
            if faction_filter_col and faction_filter_col != "NULL" and st.session_state.ms_faction_filter:
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

            if where_conditions:
                combined_where_clause = " AND ".join(where_conditions)
                # Improved WHERE clause detection - check for WHERE anywhere in the query
                has_where_clause = re.search(r"\bWHERE\b", modified_sql, re.IGNORECASE)
                keyword_to_use = "AND" if has_where_clause else "WHERE"
                filter_string_to_add = f" {keyword_to_use} {combined_where_clause}"
                
                # Find insertion point before GROUP BY, ORDER BY, etc.
                # For CTE-based queries, we need to find the main query's clauses, not CTE clauses
                insertion_point = len(modified_sql)
                found_clause = None
                
                # First, try to find the main SELECT statement (after CTEs)
                main_select_match = None
                if "WITH " in modified_sql.upper():
                    # For CTE queries, find the main SELECT after the CTE definitions
                    # Look for SELECT that's not inside parentheses or CTE definitions
                    cte_end_patterns = [
                        r"\)\s*SELECT\b",  # End of CTE followed by main SELECT
                    ]
                    for pattern in cte_end_patterns:
                        matches = list(re.finditer(pattern, modified_sql, re.IGNORECASE))
                        if matches:
                            # Take the last match (main SELECT)
                            main_select_match = matches[-1]
                            break
                
                # Define clauses to look for after the main SELECT
                clauses_keywords_to_find = [
                    r"\bGROUP\s+BY\b",
                    r"\bHAVING\b", 
                    r"\bWINDOW\b",
                    r"\bORDER\s+BY\b",
                    r"\bLIMIT\b",
                    r"\bOFFSET\b",
                    r"\bFETCH\b",
                ]
                
                # Search for clauses starting from main SELECT position
                search_start = main_select_match.end() if main_select_match else 0
                
                for pattern_str in clauses_keywords_to_find:
                    # Search from the main SELECT position onward
                    matches = list(re.finditer(pattern_str, modified_sql[search_start:], re.IGNORECASE))
                    if matches:
                        # Adjust position relative to full string
                        match_pos = search_start + matches[0].start()
                        if match_pos < insertion_point:
                            insertion_point = match_pos
                            found_clause = pattern_str
                
                # Insert filter before the first found clause
                prefix = modified_sql[:insertion_point].strip()
                suffix = modified_sql[insertion_point:].strip()
                
                if suffix:
                    modified_sql = f"{prefix}{filter_string_to_add} {suffix}".strip()
                else:
                    modified_sql = f"{prefix}{filter_string_to_add}".strip()

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
                st.session_state.show_table_explorer_results = (
                    False  # Ensure explorer is hidden
                )
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
                f"Failed SQL for '{st.session_state.selected_query_name}':\n{modified_sql if 'modified_sql' in locals() else base_sql}"
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


def _handle_explore_table_button_click(
    db_path: Path,
    connect_func: callable,
    get_db_table_list_func: callable,
    get_table_columns_func: callable,
    ui_logger: logging.Logger,
    format_exc_func: callable,
    faction_display_map: dict,
):
    """Handles the logic for the 'Explore Selected Table' button click."""
    if st.session_state.selected_table_for_explorer and db_path.exists():
        table_to_explore = st.session_state.selected_table_for_explorer
        try:
            all_table_cols, _, _ = get_table_columns_func(table_to_explore)

            db_tables_list_lower = [t.lower() for t in get_db_table_list_func()]
            join_clause = ""
            # Default select prefix, assuming the table name itself is the alias or no alias needed
            select_prefix = f'"{table_to_explore}".*'
            base_query_table_ref = (
                f'"{table_to_explore}"'  # Default reference to the table
            )

            # Handle special cases for KNS_Faction and KNS_PersonToPosition to join with UserFactionCoalitionStatus
            if (
                table_to_explore.lower() == "kns_faction"
                and "userfactioncoalitionstatus" in db_tables_list_lower
            ):
                select_prefix = "f.*, ufs.CoalitionStatus AS UserCoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition"
                base_query_table_ref = "KNS_Faction f"  # Use alias 'f'
                join_clause = "LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID AND f.KnessetNum = ufs.KnessetNum"
            elif (
                table_to_explore.lower() == "kns_persontoposition"
                and "userfactioncoalitionstatus" in db_tables_list_lower
            ):
                select_prefix = "p2p.*, ufs.CoalitionStatus AS UserCoalitionStatus, ufs.DateJoinedCoalition, ufs.DateLeftCoalition"
                base_query_table_ref = "KNS_PersonToPosition p2p"  # Use alias 'p2p'
                join_clause = "LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum"

            base_query = f"SELECT {select_prefix} FROM {base_query_table_ref}"
            where_clauses = []

            # Determine alias for filter columns based on whether a join happened
            table_alias_for_filter = ""
            if table_to_explore.lower() == "kns_faction" and join_clause:
                table_alias_for_filter = "f."
            elif table_to_explore.lower() == "kns_persontoposition" and join_clause:
                table_alias_for_filter = "p2p."
            else:
                table_alias_for_filter = f'"{table_to_explore}".'

            actual_knesset_col_in_table = next(
                (col for col in all_table_cols if col.lower() == "knessetnum"), None
            )
            if actual_knesset_col_in_table and st.session_state.ms_knesset_filter:
                knesset_col_name_explorer = (
                    f'{table_alias_for_filter}"{actual_knesset_col_in_table}"'
                )
                where_clauses.append(
                    f"{knesset_col_name_explorer} IN ({', '.join(map(str, st.session_state.ms_knesset_filter))})"
                )

            actual_faction_col_in_table = next(
                (col for col in all_table_cols if col.lower() == "factionid"), None
            )
            if actual_faction_col_in_table and st.session_state.ms_faction_filter:
                faction_col_name_explorer = (
                    f'{table_alias_for_filter}"{actual_faction_col_in_table}"'
                )
                selected_faction_ids_explorer = [
                    faction_display_map[name]
                    for name in st.session_state.ms_faction_filter
                    if name in faction_display_map
                ]
                if selected_faction_ids_explorer:
                    where_clauses.append(
                        f"{faction_col_name_explorer} IN ({', '.join(map(str, selected_faction_ids_explorer))})"
                    )

            final_query = base_query
            if join_clause:
                final_query += f" {join_clause}"
            if where_clauses:
                final_query += " WHERE " + " AND ".join(where_clauses)

            order_by_col_explorer = None
            order_by_prefix = table_alias_for_filter  # Use the determined alias

            # Check for preferred date columns for ordering, using the alias
            if next(
                (col for col in all_table_cols if col.lower() == "lastupdateddate"),
                None,
            ):
                order_by_col_explorer = f'{order_by_prefix}"LastUpdatedDate"'
            elif next(
                (col for col in all_table_cols if col.lower() == "startdate"), None
            ):
                order_by_col_explorer = f'{order_by_prefix}"StartDate"'
            elif all_table_cols:  # Fallback to the first column of the base table
                order_by_col_explorer = f'{order_by_prefix}"{all_table_cols[0]}"'

            if order_by_col_explorer:
                final_query += f" ORDER BY {order_by_col_explorer} DESC"
            final_query += " LIMIT 1000"

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
                st.session_state.show_query_results = (
                    False  # Ensure query results are hidden
                )
                st.toast(f"🔍 Explored table: {table_to_explore}", icon="📖")
        except Exception as e:
            ui_logger.error(
                f"Error exploring table '{table_to_explore}': {e}", exc_info=True
            )
            st.error(f"Error exploring table '{table_to_explore}': {e}")
            st.code(
                f"Query attempt: {final_query if 'final_query' in locals() else 'N/A'}\n\nError: {str(e)}\n\nTraceback:\n{format_exc_func()}"
            )
            st.session_state.show_table_explorer_results = False
            st.session_state.table_explorer_df = pd.DataFrame()
    elif not st.session_state.selected_table_for_explorer:
        st.warning("Please select a table to explore.")
    elif not db_path.exists():
        st.error("Database not found. Cannot explore tables.")


def _handle_multiselect_change():
    """Handles the 'Select/Deselect All' logic for the tables multiselect without reruns."""
    current_selection = st.session_state.get("ms_tables_to_refresh_widget", [])
    is_select_all_currently_checked = st.session_state.get(
        "all_tables_selected_for_refresh_flag", False
    )

    # Case 1: "Select All" was just checked
    if (
        _SELECT_ALL_TABLES_OPTION in current_selection
        and not is_select_all_currently_checked
    ):
        st.session_state.ms_tables_to_refresh = [_SELECT_ALL_TABLES_OPTION] + TABLES
        st.session_state.all_tables_selected_for_refresh_flag = True
    # Case 2: "Select All" was just unchecked
    elif (
        _SELECT_ALL_TABLES_OPTION not in current_selection
        and is_select_all_currently_checked
    ):
        st.session_state.ms_tables_to_refresh = []
        st.session_state.all_tables_selected_for_refresh_flag = False
    # Case 3: Normal selection change
    else:
        st.session_state.ms_tables_to_refresh = current_selection


def display_sidebar(
    db_path_arg: Path,
    exports_arg: dict,
    connect_func_arg: callable,
    get_db_table_list_func_arg: callable,
    get_table_columns_func_arg: callable,
    get_filter_options_func_arg: callable,
    faction_display_map_arg: dict,
    ui_logger_arg: logging.Logger,
    format_exc_func_arg: callable,
):
    """Renders all sidebar components."""
    st.sidebar.header("🔄 Data Refresh Controls")
    options_for_multiselect = [_SELECT_ALL_TABLES_OPTION] + TABLES

    st.sidebar.multiselect(
        label="Select tables to refresh/fetch:",
        options=options_for_multiselect,
        default=st.session_state.get("ms_tables_to_refresh", []),
        key="ms_tables_to_refresh_widget",
        on_change=_handle_multiselect_change,
    )
    _handle_multiselect_change()  # Call it once to initialize state correctly

    if st.sidebar.button("🔄 Refresh Selected Data", key="btn_refresh_data"):
        _handle_data_refresh_button_click(
            db_path_arg, ui_logger_arg, format_exc_func_arg
        )

    st.sidebar.divider()
    st.sidebar.header("🔎 Predefined Queries")
    query_names_options = [""] + list(exports_arg.keys())
    st.session_state.selected_query_name = st.sidebar.selectbox(
        "Select a predefined query:",
        options=query_names_options,
        index=query_names_options.index(st.session_state.selected_query_name)
        if st.session_state.selected_query_name in query_names_options
        else 0,
        key="sb_selected_query_name",
    )
    if st.sidebar.button(
        "▶️ Run Selected Query",
        disabled=(not st.session_state.selected_query_name),
        key="btn_run_query",
    ):
        _handle_run_query_button_click(
            exports_arg,
            db_path_arg,
            connect_func_arg,
            ui_logger_arg,
            format_exc_func_arg,
            faction_display_map_arg,
        )

    st.sidebar.divider()
    st.sidebar.header("🔬 Interactive Table Explorer")
    db_tables_list_for_explorer = [""] + get_db_table_list_func_arg()
    st.session_state.selected_table_for_explorer = st.sidebar.selectbox(
        "Select a table to explore:",
        options=db_tables_list_for_explorer,
        index=db_tables_list_for_explorer.index(
            st.session_state.selected_table_for_explorer
        )
        if st.session_state.selected_table_for_explorer in db_tables_list_for_explorer
        else 0,
        key="sb_selected_table_explorer",
    )
    if st.sidebar.button(
        "🔍 Explore Selected Table",
        disabled=(not st.session_state.selected_table_for_explorer),
        key="btn_explore_table",
    ):
        _handle_explore_table_button_click(
            db_path_arg,
            connect_func_arg,
            get_db_table_list_func_arg,
            get_table_columns_func_arg,
            ui_logger_arg,
            format_exc_func_arg,
            faction_display_map_arg,
        )

    st.sidebar.divider()
    st.sidebar.header("📊 Filters (Apply to Queries, Explorer & Plots)")
    knesset_nums_options_filters, _ = (
        get_filter_options_func_arg()
    )  # We only need knesset_nums_options here

    # Create filter widgets - Streamlit automatically stores values in session state using the key
    st.sidebar.multiselect(
        "Knesset Number(s):",
        options=knesset_nums_options_filters,  # Use the fetched options
        key="ms_knesset_filter",  # This becomes the session state variable name
    )
    st.sidebar.multiselect(
        "Faction(s) (by Knesset):",
        options=list(faction_display_map_arg.keys()),  # Use the passed map
        help="Select factions. The Knesset number in parentheses provides context.",
        key="ms_faction_filter",  # This becomes the session state variable name
    )
