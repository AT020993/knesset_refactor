"""Distribution and categorical chart generators."""

from pathlib import Path
from typing import Optional, List
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from .base import BaseChart
from backend.connection_manager import get_db_connection, safe_execute_query


class DistributionCharts(BaseChart):
    """Distribution analysis charts (pie, histogram, etc.)."""
    
    def plot_query_types_distribution(self, knesset_filter: Optional[List[int]] = None, faction_filter: Optional[List[str]] = None, **kwargs) -> Optional[go.Figure]:
        """Generate query types distribution chart."""
        if not self.check_database_exists():
            return None
        
        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q")
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if not self.check_tables_exist(con, ["KNS_Query"]):
                    return None
                
                query = f"""
                    SELECT
                        COALESCE(q.TypeDesc, 'Unknown') AS QueryType,
                        COUNT(q.QueryID) AS Count
                    FROM KNS_Query q
                    WHERE q.KnessetNum IS NOT NULL
                        AND {filters['knesset_condition']}
                    GROUP BY q.TypeDesc
                    ORDER BY Count DESC
                """
                
                df = safe_execute_query(con, query, self.logger)
                
                if df.empty:
                    st.info(f"No query type data found for '{filters['knesset_title']}'.")
                    return None
                
                fig = px.pie(
                    df,
                    values="Count",
                    names="QueryType",
                    title=f"<b>Query Types Distribution for {filters['knesset_title']}</b>"
                )
                
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(title_x=0.5)
                
                return fig
                
        except Exception as e:
            self.logger.error(f"Error generating query types distribution chart: {e}", exc_info=True)
            st.error(f"Could not generate query types chart: {e}")
            return None
    
    def plot_agenda_classifications_pie(self, knesset_filter: Optional[List[int]] = None, faction_filter: Optional[List[str]] = None, **kwargs) -> Optional[go.Figure]:
        """Generate agenda classifications pie chart."""
        if not self.check_database_exists():
            return None
        
        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a")
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if not self.check_tables_exist(con, ["KNS_Agenda"]):
                    return None
                
                query = f"""
                    SELECT
                        COALESCE(a.ClassificationDesc, 'Unknown') AS Classification,
                        COUNT(a.AgendaID) AS Count
                    FROM KNS_Agenda a
                    WHERE a.KnessetNum IS NOT NULL
                        AND {filters['knesset_condition']}
                    GROUP BY a.ClassificationDesc
                    ORDER BY Count DESC
                """
                
                df = safe_execute_query(con, query, self.logger)
                
                if df.empty:
                    st.info(f"No agenda classification data found for '{filters['knesset_title']}'.")
                    return None
                
                fig = px.pie(
                    df,
                    values="Count",
                    names="Classification",
                    title=f"<b>Agenda Classifications Distribution for {filters['knesset_title']}</b>"
                )
                
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(title_x=0.5)
                
                return fig
                
        except Exception as e:
            self.logger.error(f"Error generating agenda classifications pie chart: {e}", exc_info=True)
            st.error(f"Could not generate agenda classifications chart: {e}")
            return None
    
    def plot_query_status_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Generate query status distribution chart."""
        # TODO: Implement from original plot_generators.py
        pass
    
    def plot_agenda_status_distribution(self, knesset_filter: Optional[List[int]] = None, faction_filter: Optional[List[str]] = None, **kwargs) -> Optional[go.Figure]:
        """Generate agenda status distribution chart."""
        if not self.check_database_exists():
            return None
        
        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="a")
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if not self.check_tables_exist(con, ["KNS_Agenda", "KNS_Status"]):
                    return None
                
                query = f"""
                    SELECT
                        COALESCE(s."Desc", 'Unknown') AS Status,
                        COUNT(a.AgendaID) AS Count
                    FROM KNS_Agenda a
                    LEFT JOIN KNS_Status s ON a.StatusID = s.StatusID
                    WHERE a.KnessetNum IS NOT NULL
                        AND {filters['knesset_condition']}
                    GROUP BY s."Desc"
                    ORDER BY Count DESC
                """
                
                df = safe_execute_query(con, query, self.logger)
                
                if df.empty:
                    st.info(f"No agenda status data found for '{filters['knesset_title']}'.")
                    return None
                
                fig = px.pie(
                    df,
                    values="Count",
                    names="Status",
                    title=f"<b>Agenda Status Distribution for {filters['knesset_title']}</b>"
                )
                
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(title_x=0.5)
                
                return fig
                
        except Exception as e:
            self.logger.error(f"Error generating agenda status distribution chart: {e}", exc_info=True)
            st.error(f"Could not generate agenda status chart: {e}")
            return None
    
    def plot_bill_status_distribution(self, knesset_filter: Optional[List[int]] = None, faction_filter: Optional[List[str]] = None, **kwargs) -> Optional[go.Figure]:
        """Generate bill status distribution chart."""
        if not self.check_database_exists():
            return None
        
        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b")
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_Status"]):
                    return None
                
                query = f"""
                    SELECT
                        COALESCE(s."Desc", 'Unknown') AS Status,
                        COUNT(b.BillID) AS Count
                    FROM KNS_Bill b
                    LEFT JOIN KNS_Status s ON b.StatusID = s.StatusID
                    WHERE b.KnessetNum IS NOT NULL
                        AND {filters['knesset_condition']}
                    GROUP BY s."Desc"
                    ORDER BY Count DESC
                """
                
                df = safe_execute_query(con, query, self.logger)
                
                if df.empty:
                    st.info(f"No bill status data found for '{filters['knesset_title']}'.")
                    return None
                
                fig = px.pie(
                    df,
                    values="Count",
                    names="Status",
                    title=f"<b>Bill Status Distribution for {filters['knesset_title']}</b>"
                )
                
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(title_x=0.5)
                
                return fig
                
        except Exception as e:
            self.logger.error(f"Error generating bill status distribution chart: {e}", exc_info=True)
            st.error(f"Could not generate bill status chart: {e}")
            return None
    
    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested distribution chart."""
        chart_methods = {
            "query_types_distribution": self.plot_query_types_distribution,
            "agenda_classifications_pie": self.plot_agenda_classifications_pie,
            "query_status_distribution": self.plot_query_status_distribution,
            "agenda_status_distribution": self.plot_agenda_status_distribution,
            "bill_status_distribution": self.plot_bill_status_distribution,
        }
        
        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown distribution chart type: {chart_type}")
            return None