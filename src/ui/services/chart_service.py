"""Chart service for UI layer."""

from pathlib import Path
from typing import Optional, Dict, Any
import logging
import plotly.graph_objects as go

from ui.charts.factory import ChartFactory
from config.settings import Settings


class ChartService:
    """Service layer for chart generation in the UI."""
    
    def __init__(self, db_path: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)
        self.chart_factory = ChartFactory(self.db_path, self.logger)
    
    def create_chart(self, chart_category: str, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Create a chart using the chart factory."""
        return self.chart_factory.create_chart(chart_category, chart_type, **kwargs)
    
    def get_available_charts(self) -> Dict[str, list]:
        """Get available chart types."""
        return self.chart_factory.get_available_charts()
    
    # Legacy compatibility methods for existing UI code
    def plot_queries_by_time_period(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for queries by time period."""
        return self.chart_factory.plot_queries_by_time_period(**kwargs)
    
    def plot_query_types_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for query types distribution."""
        return self.chart_factory.plot_query_types_distribution(**kwargs)
    
    def plot_queries_per_faction_in_knesset(self, **kwargs) -> Optional[go.Figure]:
        """Legacy method for queries per faction."""
        return self.chart_factory.plot_queries_per_faction_in_knesset(**kwargs)