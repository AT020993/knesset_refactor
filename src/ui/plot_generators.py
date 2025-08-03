"""
Legacy compatibility layer for plot_generators.

This module provides backward compatibility for existing code that imports
from the old plot_generators module. All functionality has been moved to
the new modular chart system.

For new code, use:
- ui.charts.factory.ChartFactory
- ui.services.chart_service.ChartService
"""

import logging
import warnings
from pathlib import Path
from typing import Any, Callable, List, Optional

from ui.services.chart_service import ChartService

# Deprecation warning
warnings.warn(
    "plot_generators module is deprecated. Use ui.charts.factory.ChartFactory or ui.services.chart_service.ChartService instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Legacy color constants for backward compatibility
from config.charts import ChartConfig

KNESSET_COLOR_SEQUENCE = ChartConfig.KNESSET_COLOR_SEQUENCE
COALITION_OPPOSITION_COLORS = ChartConfig.COALITION_OPPOSITION_COLORS
ANSWER_STATUS_COLORS = ChartConfig.ANSWER_STATUS_COLORS
GENERAL_STATUS_COLORS = ChartConfig.GENERAL_STATUS_COLORS
QUERY_TYPE_COLORS = ChartConfig.QUERY_TYPE_COLORS


def check_tables_exist(
    con, required_tables: list[str], logger_obj: logging.Logger
) -> bool:
    """Legacy compatibility function."""
    from ui.charts.base import BaseChart

    base_chart = BaseChart(Path(), logger_obj)
    return base_chart.check_tables_exist(con, required_tables)


# Legacy function wrappers
def plot_queries_by_time_period(
    db_path: Path,
    connect_func: Callable,  # Ignored in new implementation
    logger_obj: logging.Logger,
    **kwargs,
) -> Optional[Any]:
    """Legacy wrapper for queries by time period chart."""
    try:
        logger_obj.info(
            f"plot_queries_by_time_period called with db_path={db_path}, kwargs={kwargs}"
        )
        chart_service = ChartService(db_path, logger_obj)
        logger_obj.info("ChartService created successfully")
        result = chart_service.plot_queries_by_time_period(**kwargs)
        logger_obj.info(
            f"Chart result: {type(result)} {'(figure)' if result else '(None)'}"
        )
        return result
    except Exception as e:
        logger_obj.error(f"Error in plot_queries_by_time_period: {e}", exc_info=True)
        import streamlit as st

        st.error(f"Chart generation failed: {e}")
        return None


def plot_query_types_distribution(
    db_path: Path,
    connect_func: Callable,  # Ignored in new implementation
    logger_obj: logging.Logger,
    **kwargs,
) -> Optional[Any]:
    """Legacy wrapper for query types distribution chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_query_types_distribution(**kwargs)


def plot_queries_per_faction_in_knesset(
    db_path: Path,
    connect_func: Callable,  # Ignored in new implementation
    logger_obj: logging.Logger,
    **kwargs,
) -> Optional[Any]:
    """Legacy wrapper for queries per faction chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_queries_per_faction_in_knesset(**kwargs)


def plot_query_status_by_faction(
    db_path: Path,
    connect_func: Callable,  # Ignored in new implementation
    logger_obj: logging.Logger,
    **kwargs,
) -> Optional[Any]:
    """Legacy wrapper for query status by faction chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_query_status_by_faction(**kwargs)


# Add more legacy function wrappers as needed


def plot_agendas_by_time_period(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for agendas by time period chart."""
    try:
        logger_obj.info(
            f"plot_agendas_by_time_period called with db_path={db_path}, kwargs={kwargs}"
        )
        chart_service = ChartService(db_path, logger_obj)
        logger_obj.info("ChartService created for agendas")
        result = chart_service.plot_agendas_by_time_period(**kwargs)
        logger_obj.info(
            f"Agenda chart result: {type(result)} {'(figure)' if result else '(None)'}"
        )
        return result
    except Exception as e:
        logger_obj.error(f"Error in plot_agendas_by_time_period: {e}", exc_info=True)
        import streamlit as st

        st.error(f"Agenda chart generation failed: {e}")
        return None


def plot_agenda_classifications_pie(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for agenda classifications pie chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_agenda_classifications_pie(**kwargs)


def plot_agenda_status_distribution(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for agenda status distribution chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_agenda_status_distribution(**kwargs)


def plot_agendas_per_faction(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for agendas per faction chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_agendas_per_faction(**kwargs)


def plot_agendas_by_coalition_status(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for agendas by coalition status chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_agendas_by_coalition_status(**kwargs)


def plot_bill_status_distribution(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for bill status distribution chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_bill_status_distribution(**kwargs)

def plot_bills_by_time_period(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for bills by time period chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_bills_by_time_period(**kwargs)

def plot_bill_subtype_distribution(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for bill subtype distribution chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_bill_subtype_distribution(**kwargs)

def plot_bills_per_faction(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for bills per faction chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_bills_per_faction(**kwargs)

def plot_bills_by_coalition_status(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for bills by coalition status chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_bills_by_coalition_status(**kwargs)


def plot_top_bill_initiators(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for top bill initiators chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_top_bill_initiators(**kwargs)


def plot_bill_initiators_by_faction(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for bill initiators by faction chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_bill_initiators_by_faction(**kwargs)


def plot_total_bills_per_faction(db_path, connect_func, logger_obj, **kwargs):
    """Legacy wrapper for total bills per faction chart."""
    chart_service = ChartService(db_path, logger_obj)
    return chart_service.plot_total_bills_per_faction(**kwargs)


def get_available_plots():
    """Return available plot categories and their functions for the UI."""
    return {
        "Query Analytics": {
            "Queries by Time Period": plot_queries_by_time_period,
            "Query Types Distribution": plot_query_types_distribution,
            "Queries per Faction": plot_queries_per_faction_in_knesset,
            "Query Status Description with Faction Breakdown (Single Knesset)": plot_query_status_by_faction,
        },
        "Agenda Analytics": {
            "Agendas by Time Period": plot_agendas_by_time_period,
            "Agenda Classifications": plot_agenda_classifications_pie,
            "Agenda Status Distribution": plot_agenda_status_distribution,
            "Agendas per Faction": plot_agendas_per_faction,
            "Agendas by Coalition Status": plot_agendas_by_coalition_status,
        },
        "Bills Analytics": {
            "Bill Status Distribution": plot_bill_status_distribution,
            "Bills by Time Period": plot_bills_by_time_period,
            "Bill SubType Distribution": plot_bill_subtype_distribution,
            "Bills per Faction": plot_bills_per_faction,
            "Bills by Coalition Status": plot_bills_by_coalition_status,
            "Top 10 Bill Initiators": plot_top_bill_initiators,
            "Bill Initiators by Faction": plot_bill_initiators_by_faction,
            "Total Bills per Faction": plot_total_bills_per_faction,
        },
    }
