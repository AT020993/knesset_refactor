"""Base chart class for all chart types."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
import logging
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import duckdb

from backend.connection_manager import get_db_connection, safe_execute_query
from config.charts import ChartConfig


class BaseChart(ABC):
    """Base class for all chart generators."""
    
    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj
        self.config = ChartConfig()
    
    def check_database_exists(self) -> bool:
        """Check if database file exists."""
        if not self.db_path.exists():
            st.error("Database not found. Cannot generate visualization.")
            self.logger.error(f"Database not found: {self.db_path}")
            return False
        return True
    
    def check_tables_exist(self, con: duckdb.DuckDBPyConnection, required_tables: List[str]) -> bool:
        """Check if all required tables exist in the database."""
        try:
            db_tables_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main';").df()
            db_tables_list = db_tables_df['table_name'].str.lower().tolist()
            missing_tables = [table for table in required_tables if table.lower() not in db_tables_list]
            
            if missing_tables:
                st.warning(f"Visualization skipped: Required table(s) '{', '.join(missing_tables)}' not found. Please refresh data.")
                self.logger.warning(f"Required table(s) '{', '.join(missing_tables)}' not found for visualization.")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}", exc_info=True)
            st.error(f"Error checking table existence: {e}")
            return False
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute a query safely and return the result."""
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                return safe_execute_query(con, query, self.logger)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}", exc_info=True)
            st.error(f"Error executing query: {e}")
            return None
    
    def build_filters(self, knesset_filter: Optional[List[int]] = None, 
                     faction_filter: Optional[List[str]] = None,
                     table_prefix: str = "",
                     **kwargs) -> Dict[str, Any]:
        """Build common filter conditions."""
        filters = {}
        
        # Add table prefix with dot if provided
        prefix = f"{table_prefix}." if table_prefix else ""
        
        if knesset_filter:
            filters['knesset_condition'] = f"{prefix}KnessetNum IN ({','.join(map(str, knesset_filter))})"
            if len(knesset_filter) == 1:
                filters['is_single_knesset'] = True
                filters['knesset_title'] = f"Knesset {knesset_filter[0]}"
            else:
                filters['is_single_knesset'] = False
                filters['knesset_title'] = f"Knessets: {', '.join(map(str, knesset_filter))}"
        else:
            filters['knesset_condition'] = "1=1"  # No filter
            filters['is_single_knesset'] = False
            filters['knesset_title'] = "All Knessets"
            
        if faction_filter:
            faction_list = "', '".join(faction_filter)
            filters['faction_condition'] = f"FactionName IN ('{faction_list}')"
        else:
            filters['faction_condition'] = "1=1"  # No filter
            
        # Add advanced filters
        self._add_advanced_filters(filters, prefix, **kwargs)
        
        return filters
        
    def _add_advanced_filters(self, filters: Dict[str, Any], prefix: str = "", **kwargs) -> None:
        """Add advanced filter conditions based on provided kwargs."""
        
        # Query-specific filters
        query_type_filter = kwargs.get('query_type_filter', [])
        if query_type_filter:
            type_list = "', '".join(query_type_filter)
            filters['query_type_condition'] = f"{prefix}TypeDesc IN ('{type_list}')"
        else:
            filters['query_type_condition'] = "1=1"
            
        query_status_filter = kwargs.get('query_status_filter', [])
        if query_status_filter:
            status_list = "', '".join(query_status_filter)
            filters['query_status_condition'] = f"{prefix}StatusDesc IN ('{status_list}')"
        else:
            filters['query_status_condition'] = "1=1"
            
        # Agenda-specific filters
        session_type_filter = kwargs.get('session_type_filter', [])
        if session_type_filter:
            session_list = "', '".join(session_type_filter)
            filters['session_type_condition'] = f"{prefix}SessionType IN ('{session_list}')"
        else:
            filters['session_type_condition'] = "1=1"
            
        agenda_status_filter = kwargs.get('agenda_status_filter', [])
        if agenda_status_filter:
            agenda_status_list = "', '".join(agenda_status_filter)
            filters['agenda_status_condition'] = f"{prefix}StatusDesc IN ('{agenda_status_list}')"
        else:
            filters['agenda_status_condition'] = "1=1"
            
        # Bill-specific filters
        bill_type_filter = kwargs.get('bill_type_filter', [])
        if bill_type_filter:
            bill_type_list = "', '".join(bill_type_filter)
            filters['bill_type_condition'] = f"{prefix}BillTypeDesc IN ('{bill_type_list}')"
        else:
            filters['bill_type_condition'] = "1=1"
            
        bill_status_filter = kwargs.get('bill_status_filter', [])
        if bill_status_filter:
            bill_status_list = "', '".join(bill_status_filter)
            filters['bill_status_condition'] = f"{prefix}StatusDesc IN ('{bill_status_list}')"
        else:
            filters['bill_status_condition'] = "1=1"
            
        # Date filters
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        
        if start_date:
            filters['start_date_condition'] = f"{prefix}SubmitDate >= '{start_date}'"
        else:
            filters['start_date_condition'] = "1=1"
            
        if end_date:
            filters['end_date_condition'] = f"{prefix}SubmitDate <= '{end_date}'"
        else:
            filters['end_date_condition'] = "1=1"
    
    @abstractmethod
    def generate(self, **kwargs) -> Optional[go.Figure]:
        """Generate the chart. Must be implemented by subclasses."""
        pass