"""Chart factory for creating different chart types."""

from pathlib import Path
from typing import Optional, Dict, Any
import logging
import plotly.graph_objects as go

from .time_series import TimeSeriesCharts
from .distribution import DistributionCharts
from .comparison import ComparisonCharts
from .network import NetworkCharts
from .timeline import TimelineCharts


class ChartFactory:
    """Factory class for creating different types of charts."""
    
    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj
        
        # Initialize chart generators
        self._generators = {
            'time_series': TimeSeriesCharts(db_path, logger_obj),
            'distribution': DistributionCharts(db_path, logger_obj),
            'comparison': ComparisonCharts(db_path, logger_obj),
            'network': NetworkCharts(db_path, logger_obj),
            'timeline': TimelineCharts(db_path, logger_obj),
        }
    
    def create_chart(self, chart_category: str, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """
        Create a chart of the specified type.
        
        Args:
            chart_category: Category of chart (time_series, distribution, etc.)
            chart_type: Specific chart type within the category
            **kwargs: Chart-specific parameters
            
        Returns:
            Plotly figure or None if error
        """
        generator = self._generators.get(chart_category)
        if not generator:
            self.logger.warning(f"Unknown chart category: {chart_category}")
            return None
        
        try:
            return generator.generate(chart_type, **kwargs)
        except Exception as e:
            self.logger.error(f"Error creating chart {chart_category}.{chart_type}: {e}", exc_info=True)
            return None
    
    def get_available_charts(self) -> Dict[str, list]:
        """Get list of available chart types by category."""
        return {
            'time_series': [
                'queries_by_time',
                'agendas_by_time',
                'parliamentary_activity_heatmap'
            ],
            'distribution': [
                'query_types_distribution',
                'agenda_classifications_pie',
                'query_status_distribution',
                'agenda_status_distribution',
                'bill_status_distribution'
            ],
            'comparison': [
                'queries_per_faction',
                'queries_by_coalition_status',
                'queries_by_ministry'
            ],
            'network': [
                'mk_collaboration_network'
            ],
            'timeline': [
                'coalition_timeline_gantt',
                'mk_tenure_gantt',
                'ministry_leadership_timeline',
                'query_response_times'
            ]
        }
    
    # Legacy compatibility methods for existing code
    def plot_queries_by_time_period(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart('time_series', 'queries_by_time', **kwargs)
    
    def plot_query_types_distribution(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart('distribution', 'query_types_distribution', **kwargs)
    
    def plot_queries_per_faction_in_knesset(self, **kwargs) -> Optional[go.Figure]:
        """Legacy compatibility method."""
        return self.create_chart('comparison', 'queries_per_faction', **kwargs)