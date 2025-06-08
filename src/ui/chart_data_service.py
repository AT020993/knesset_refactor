"""
Chart Data Service - Handles SQL queries and data fetching for chart builder
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

from . import ui_utils


class ChartDataService:
    """Service for handling chart data operations and SQL queries"""
    
    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj
    
    def get_table_data_for_filters(
        self, 
        table_name: str, 
        max_rows: int,
        sidebar_knesset_filter: List[int],
        sidebar_faction_filter_names: List[str],
        faction_display_map: Dict[str, int]
    ) -> pd.DataFrame:
        """
        Fetch data from selected table with global sidebar filters applied.
        Used to populate chart-specific filter options.
        """
        if not table_name:
            return pd.DataFrame()
            
        try:
            all_cols, _, _ = ui_utils.get_table_columns(self.db_path, table_name, self.logger)
            
            con = ui_utils.connect_db(self.db_path, read_only=True, _logger_obj=self.logger)
            try:
                query = f'SELECT * FROM "{table_name}"'
                where_clauses = []
                
                # Apply Knesset filter
                actual_knesset_col = next((col for col in all_cols if col.lower() == "knessetnum"), None)
                if actual_knesset_col and sidebar_knesset_filter:
                    where_clauses.append(f'"{actual_knesset_col}" IN ({", ".join(map(str, sidebar_knesset_filter))})')
                
                # Apply Faction filter
                actual_faction_col = next((col for col in all_cols if col.lower() == "factionid"), None)
                if actual_faction_col and sidebar_faction_filter_names:
                    sidebar_faction_ids = [
                        faction_display_map[name] 
                        for name in sidebar_faction_filter_names 
                        if name in faction_display_map
                    ]
                    if sidebar_faction_ids:
                        where_clauses.append(f'"{actual_faction_col}" IN ({", ".join(map(str, sidebar_faction_ids))})')
                
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                query += f" LIMIT {max_rows}"
                
                result_df = ui_utils.safe_execute_query(con, query, self.logger)
                self.logger.info(f"Fetched {len(result_df)} rows for chart-specific filter population from table '{table_name}'")
                return result_df
                
            finally:
                con.close()
                
        except Exception as e:
            self.logger.error(f"Error fetching data for chart-specific filter options: {e}", exc_info=True)
            return pd.DataFrame()
    
    def apply_chart_specific_filters(
        self, 
        df: pd.DataFrame, 
        knesset_filter: List[int], 
        faction_filter_ids: List[int]
    ) -> pd.DataFrame:
        """Apply chart-specific filters to the dataframe"""
        filtered_df = df.copy()
        
        # Apply Knesset filter
        if knesset_filter and 'KnessetNum' in filtered_df.columns:
            # Ensure KnessetNum is integer type
            if not pd.api.types.is_integer_dtype(filtered_df['KnessetNum']) and filtered_df['KnessetNum'].notna().any():
                try:
                    filtered_df['KnessetNum'] = pd.to_numeric(filtered_df['KnessetNum'], errors='coerce').fillna(-1).astype(int)
                except Exception as e:
                    self.logger.warning(f"Could not convert KnessetNum to int for chart-specific filtering: {e}")
            
            filtered_df = filtered_df[filtered_df['KnessetNum'].isin(knesset_filter)]
            self.logger.info(f"After chart-specific Knesset filter: {len(filtered_df)} rows.")
        
        # Apply Faction filter
        if faction_filter_ids and 'FactionID' in filtered_df.columns:
            # Ensure FactionID is integer type
            if not pd.api.types.is_integer_dtype(filtered_df['FactionID']) and filtered_df['FactionID'].notna().any():
                try:
                    filtered_df['FactionID'] = pd.to_numeric(filtered_df['FactionID'], errors='coerce').fillna(-1).astype(int)
                except Exception as e:
                    self.logger.warning(f"Could not convert FactionID to int for chart-specific filtering: {e}")
            
            filtered_df = filtered_df[filtered_df['FactionID'].isin(faction_filter_ids)]
            self.logger.info(f"After chart-specific Faction filter: {len(filtered_df)} rows.")
        
        return filtered_df
    
    def get_unique_values_for_filter(self, df: pd.DataFrame, column: str, sort_reverse: bool = False) -> List:
        """Get unique values from a column for filter options"""
        if df.empty or column not in df.columns:
            return []
        
        unique_values = df[column].dropna().unique()
        
        # Convert to appropriate type and sort
        if column.lower() == 'knessetnum':
            unique_values = sorted(unique_values.astype(int), reverse=sort_reverse)
        else:
            unique_values = sorted(unique_values)
        
        return list(unique_values)
    
    def get_faction_filter_options(self, df: pd.DataFrame, faction_display_map: Dict[str, int]) -> List[str]:
        """Get faction filter options sorted alphabetically"""
        if df.empty or 'FactionID' not in df.columns:
            return []
        
        unique_faction_ids = df['FactionID'].dropna().unique()
        faction_options = [
            display_name
            for display_name, f_id in faction_display_map.items()
            if f_id in unique_faction_ids
        ]
        return sorted(faction_options)