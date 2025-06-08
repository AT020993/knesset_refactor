"""
Legacy compatibility layer for plot_generators.

This module provides backward compatibility for existing code that imports
from the old plot_generators module. All functionality has been moved to
the new modular chart system.

For new code, use:
- ui.charts.factory.ChartFactory
- ui.services.chart_service.ChartService
"""

import warnings
from pathlib import Path
from typing import Optional, List, Callable, Any
import logging

from ui.services.chart_service import ChartService

# Deprecation warning
warnings.warn(
    "plot_generators module is deprecated. Use ui.charts.factory.ChartFactory or ui.services.chart_service.ChartService instead.",
    DeprecationWarning,
    stacklevel=2
)

# Legacy color constants for backward compatibility
from config.charts import ChartConfig
KNESSET_COLOR_SEQUENCE = ChartConfig.KNESSET_COLOR_SEQUENCE
COALITION_OPPOSITION_COLORS = ChartConfig.COALITION_OPPOSITION_COLORS
ANSWER_STATUS_COLORS = ChartConfig.ANSWER_STATUS_COLORS
GENERAL_STATUS_COLORS = ChartConfig.GENERAL_STATUS_COLORS
QUERY_TYPE_COLORS = ChartConfig.QUERY_TYPE_COLORS


def check_tables_exist(con, required_tables: list[str], logger_obj: logging.Logger) -> bool:
    """Legacy compatibility function."""
    from ui.charts.base import BaseChart
    base_chart = BaseChart(Path(), logger_obj)
    return base_chart.check_tables_exist(con, required_tables)


# Legacy function wrappers
def plot_queries_by_time_period(
    db_path: Path,
    connect_func: Callable,  # Ignored in new implementation
    logger_obj: logging.Logger,
    **kwargs
) -> Optional[Any]:
    """Legacy wrapper for queries by time period chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_queries_by_time_period(**kwargs)


def plot_query_types_distribution(
    db_path: Path,
    connect_func: Callable,  # Ignored in new implementation
    logger_obj: logging.Logger,
    **kwargs
) -> Optional[Any]:
    """Legacy wrapper for query types distribution chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_query_types_distribution(**kwargs)


def plot_queries_per_faction_in_knesset(
    db_path: Path,
    connect_func: Callable,  # Ignored in new implementation
    logger_obj: logging.Logger,
    **kwargs
) -> Optional[Any]:
    """Legacy wrapper for queries per faction chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_queries_per_faction_in_knesset(**kwargs)


# Add more legacy function wrappers as needed
def plot_parliamentary_activity_heatmap(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_ministry_workload_sunburst(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_mk_collaboration_network(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_agendas_by_time_period(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_agenda_classifications_pie(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_query_status_by_faction(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_agenda_status_distribution(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_queries_by_coalition_and_answer_status(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_queries_by_ministry_and_status(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_agendas_per_faction_in_knesset(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_agendas_by_coalition_and_status(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_coalition_timeline_gantt(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_mk_tenure_gantt(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_ministry_leadership_timeline(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None


def plot_query_response_times(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper - implementation pending in new system."""
    warnings.warn("This chart type needs to be implemented in the new chart system", UserWarning)
    return None