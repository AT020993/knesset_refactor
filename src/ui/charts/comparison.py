"""Comparison and faction analysis chart generators."""

from pathlib import Path
from typing import Optional, List
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from .base import BaseChart
from backend.connection_manager import get_db_connection, safe_execute_query


class ComparisonCharts(BaseChart):
    """Comparison charts for factions, ministries, etc."""
    
    def plot_queries_per_faction(self, knesset_filter: Optional[List[int]] = None, faction_filter: Optional[List[str]] = None, **kwargs) -> Optional[go.Figure]:
        """Generate queries per faction chart."""
        if not self.check_database_exists():
            return None
        
        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="q")
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if not self.check_tables_exist(con, ["KNS_Query", "KNS_PersonToPosition"]):
                    return None
                
                query = f"""
                    SELECT
                        COALESCE(p2p.FactionName, 'Unknown') AS FactionName,
                        COUNT(q.QueryID) AS QueryCount
                    FROM KNS_Query q
                    LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID 
                        AND q.KnessetNum = p2p.KnessetNum
                    WHERE q.KnessetNum IS NOT NULL
                        AND {filters['knesset_condition']}
                    GROUP BY p2p.FactionName
                    ORDER BY QueryCount DESC
                    LIMIT 20
                """
                
                df = safe_execute_query(con, query, self.logger)
                
                if df.empty:
                    st.info(f"No faction query data found for '{filters['knesset_title']}'.")
                    return None
                
                fig = px.bar(
                    df,
                    x="FactionName",
                    y="QueryCount",
                    title=f"<b>Queries per Faction for {filters['knesset_title']}</b>",
                    labels={
                        "FactionName": "Faction",
                        "QueryCount": "Number of Queries"
                    },
                    color_discrete_sequence=self.config.KNESSET_COLOR_SEQUENCE
                )
                
                fig.update_layout(
                    xaxis_title="Faction",
                    yaxis_title="Number of Queries",
                    title_x=0.5,
                    xaxis_tickangle=-45
                )
                
                return fig
                
        except Exception as e:
            self.logger.error(f"Error generating queries per faction chart: {e}", exc_info=True)
            st.error(f"Could not generate queries per faction chart: {e}")
            return None
    
    def plot_queries_by_coalition_status(self, **kwargs) -> Optional[go.Figure]:
        """Generate queries by coalition/opposition status chart."""
        # TODO: Implement from original plot_generators.py
        pass
    
    def plot_queries_by_ministry(self, **kwargs) -> Optional[go.Figure]:
        """Generate queries by ministry chart."""
        # TODO: Implement from original plot_generators.py
        pass
    
    
    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested comparison chart."""
        chart_methods = {
            "queries_per_faction": self.plot_queries_per_faction,
            "queries_by_coalition_status": self.plot_queries_by_coalition_status,
            "queries_by_ministry": self.plot_queries_by_ministry,
        }
        
        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown comparison chart type: {chart_type}")
            return None