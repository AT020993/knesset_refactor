"""
Query execution logic with filtering and session state management.

This module handles the execution of predefined queries with dynamic filtering
and manages the results in Streamlit session state.
"""

import logging
import re
from pathlib import Path
from typing import List, Optional, Callable, Dict, Any, Tuple

import pandas as pd
import streamlit as st

from .predefined_queries import get_query_definition, get_query_sql, get_query_filters


class QueryExecutor:
    """Handles execution of predefined queries with filtering capabilities."""
    
    def __init__(self, db_path: Path, connect_func: Callable, logger: logging.Logger):
        """
        Initialize the query executor.
        
        Args:
            db_path: Path to the database
            connect_func: Function to create database connections
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.connect_func = connect_func
        self.logger = logger

    def execute_query_with_filters(
        self,
        query_name: str,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[int]] = None,
        safe_execute_func: Optional[Callable] = None
    ) -> Tuple[pd.DataFrame, str, List[str]]:
        """
        Execute a predefined query with optional filters.
        
        Args:
            query_name: Name of the predefined query to execute
            knesset_filter: List of Knesset numbers to filter by
            faction_filter: List of faction IDs to filter by
            safe_execute_func: Function to safely execute SQL queries
            
        Returns:
            Tuple of (results_df, executed_sql, applied_filters_info)
        """
        query_def = get_query_definition(query_name)
        if not query_def:
            self.logger.error(f"Query '{query_name}' not found in predefined queries")
            return pd.DataFrame(), "", ["Error: Query not found"]
        
        base_sql = get_query_sql(query_name)
        filter_columns = get_query_filters(query_name)
        
        # Build dynamic filters
        where_conditions = []
        applied_filters_info = []
        
        # Add Knesset filter
        if knesset_filter and filter_columns.get("knesset_filter_column"):
            if len(knesset_filter) == 1:
                where_conditions.append(f"{filter_columns['knesset_filter_column']} = {knesset_filter[0]}")
                applied_filters_info.append(f"Knesset(s): {knesset_filter[0]}")
            else:
                knesset_list = ", ".join(map(str, knesset_filter))
                where_conditions.append(f"{filter_columns['knesset_filter_column']} IN ({knesset_list})")
                applied_filters_info.append(f"Knesset(s): {knesset_list}")
        else:
            applied_filters_info.append("Knesset(s): All")
        
        # Add Faction filter
        if faction_filter and filter_columns.get("faction_filter_column"):
            if len(faction_filter) == 1:
                where_conditions.append(f"{filter_columns['faction_filter_column']} = {faction_filter[0]}")
            else:
                faction_list = ", ".join(map(str, faction_filter))
                where_conditions.append(f"{filter_columns['faction_filter_column']} IN ({faction_list})")
            applied_filters_info.append(f"Faction(s): {len(faction_filter)} selected")
        else:
            applied_filters_info.append("Faction(s): All")
        
        # Construct final SQL with filters
        if where_conditions:
            # Remove the existing LIMIT and add WHERE clause before ORDER BY
            sql_parts = base_sql.rsplit("ORDER BY", 1)
            if len(sql_parts) == 2:
                main_query = sql_parts[0].rstrip()
                order_clause = "ORDER BY " + sql_parts[1]
                
                # Remove LIMIT from order clause if present
                order_parts = order_clause.rsplit("LIMIT", 1)
                order_clause = order_parts[0].rstrip()
                limit_clause = "LIMIT " + order_parts[1].strip() if len(order_parts) > 1 else "LIMIT 1000"
                
                where_clause = " WHERE " + " AND ".join(where_conditions)
                final_sql = main_query + where_clause + " " + order_clause + " " + limit_clause
            else:
                # Fallback: append WHERE conditions at the end
                final_sql = base_sql.rstrip()
                if final_sql.endswith(";"):
                    final_sql = final_sql[:-1]
                final_sql += " WHERE " + " AND ".join(where_conditions) + ";"
        else:
            final_sql = base_sql
        
        # Execute the query
        con = None
        try:
            con = self.connect_func(read_only=True)
            if safe_execute_func:
                results_df = safe_execute_func(con, final_sql, _logger_obj=self.logger)
            else:
                results_df = pd.read_sql_query(final_sql, con)
            
            self.logger.info(f"Query '{query_name}' executed successfully, returned {len(results_df)} rows")
            return results_df, final_sql, applied_filters_info
            
        except Exception as e:
            self.logger.error(f"Error executing query '{query_name}': {e}", exc_info=True)
            return pd.DataFrame(), final_sql, applied_filters_info + [f"Error: {str(e)}"]
        finally:
            if con:
                con.close()

    def update_session_state_with_results(
        self,
        query_name: str,
        results_df: pd.DataFrame,
        executed_sql: str,
        applied_filters_info: List[str]
    ) -> None:
        """
        Update Streamlit session state with query results.
        
        Args:
            query_name: Name of the executed query
            results_df: Query results dataframe
            executed_sql: The SQL that was executed
            applied_filters_info: List of filter descriptions
        """
        st.session_state.executed_query_name = query_name
        st.session_state.query_results_df = results_df
        st.session_state.last_executed_sql = executed_sql
        st.session_state.applied_filters_info_query = applied_filters_info
        st.session_state.show_query_results = True

    def execute_table_exploration(
        self,
        table_name: str,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[int]] = None,
        faction_display_map: Optional[Dict[str, int]] = None,
        safe_execute_func: Optional[Callable] = None
    ) -> Tuple[pd.DataFrame, bool]:
        """
        Execute table exploration with filters.
        
        Args:
            table_name: Name of the table to explore
            knesset_filter: List of Knesset numbers to filter by
            faction_filter: List of faction names (display format) to filter by
            faction_display_map: Mapping from display names to faction IDs
            safe_execute_func: Function to safely execute SQL queries
            
        Returns:
            Tuple of (results_df, success_flag)
        """
        try:
            # Build base query
            base_query = f"SELECT * FROM {table_name}"
            where_conditions = []
            
            # Add filters if the table has relevant columns
            con = self.connect_func(read_only=True)
            
            # Check for KnessetNum column
            if knesset_filter:
                try:
                    # Test if KnessetNum column exists
                    test_query = f"SELECT KnessetNum FROM {table_name} LIMIT 1"
                    if safe_execute_func:
                        safe_execute_func(con, test_query, _logger_obj=self.logger)
                    else:
                        pd.read_sql_query(test_query, con)
                    
                    if len(knesset_filter) == 1:
                        where_conditions.append(f"KnessetNum = {knesset_filter[0]}")
                    else:
                        knesset_list = ", ".join(map(str, knesset_filter))
                        where_conditions.append(f"KnessetNum IN ({knesset_list})")
                except:
                    # KnessetNum column doesn't exist, skip this filter
                    pass
            
            # Add filters for faction if applicable
            if faction_filter and faction_display_map:
                faction_ids = [faction_display_map[name] for name in faction_filter if name in faction_display_map]
                if faction_ids:
                    try:
                        # Test if FactionID column exists
                        test_query = f"SELECT FactionID FROM {table_name} LIMIT 1"
                        if safe_execute_func:
                            safe_execute_func(con, test_query, _logger_obj=self.logger)
                        else:
                            pd.read_sql_query(test_query, con)
                        
                        if len(faction_ids) == 1:
                            where_conditions.append(f"FactionID = {faction_ids[0]}")
                        else:
                            faction_list = ", ".join(map(str, faction_ids))
                            where_conditions.append(f"FactionID IN ({faction_list})")
                    except:
                        # FactionID column doesn't exist, skip this filter
                        pass
            
            # Construct final query
            if where_conditions:
                final_query = base_query + " WHERE " + " AND ".join(where_conditions) + " LIMIT 1000"
            else:
                final_query = base_query + " LIMIT 1000"
            
            # Execute query
            if safe_execute_func:
                results_df = safe_execute_func(con, final_query, _logger_obj=self.logger)
            else:
                results_df = pd.read_sql_query(final_query, con)
            
            con.close()
            self.logger.info(f"Table exploration for '{table_name}' returned {len(results_df)} rows")
            return results_df, True
            
        except Exception as e:
            self.logger.error(f"Error exploring table '{table_name}': {e}", exc_info=True)
            if 'con' in locals() and con:
                con.close()
            return pd.DataFrame(), False

    def update_session_state_with_table_results(
        self,
        table_name: str,
        results_df: pd.DataFrame
    ) -> None:
        """
        Update Streamlit session state with table exploration results.
        
        Args:
            table_name: Name of the explored table
            results_df: Table exploration results dataframe
        """
        st.session_state.executed_table_explorer_name = table_name
        st.session_state.table_explorer_df = results_df
        st.session_state.show_table_explorer_results = True