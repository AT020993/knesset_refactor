"""Typed contracts for grouped Streamlit session state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class QueryState:
    """Query execution state contract."""

    selected_query_name: str | None
    executed_query_name: str | None
    executed_sql_string: str
    query_results_df: pd.DataFrame
    show_query_results: bool
    applied_knesset_filter_to_query: list[Any]
    last_executed_sql: str
    applied_filters_info_query: list[str]
    last_query_params: list[Any]


@dataclass(frozen=True)
class PlotState:
    """Plot configuration and generated-output state contract."""

    selected_plot_topic: str
    selected_plot_name_from_topic: str
    generated_plot_figure: Any
    plot_main_knesset_selection: str
    plot_aggregation_level: str
    plot_show_average_line: bool
    plot_start_date: date | None
    plot_end_date: date | None
    plot_query_type_filter: list[str]
    plot_query_status_filter: list[str]
    plot_session_type_filter: list[str]
    plot_agenda_status_filter: list[str]
    plot_bill_type_filter: list[str]
    plot_bill_status_filter: list[str]


@dataclass(frozen=True)
class TableExplorerState:
    """Table explorer state contract."""

    selected_table_for_explorer: str | None
    executed_table_explorer_name: str | None
    table_explorer_df: pd.DataFrame
    show_table_explorer_results: bool
    builder_selected_table: str | None
    builder_selected_table_previous_run: str | None


@dataclass(frozen=True)
class FilterState:
    """Global filter state contract."""

    ms_knesset_filter: list[int]
    ms_faction_filter: list[str]

