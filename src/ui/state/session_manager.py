"""
Centralized session state management for Streamlit application.

This module provides a clean interface for managing all session state
variables, reducing the scattered initialization throughout the UI code.
"""

from datetime import date
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import streamlit as st


class SessionStateManager:
    """Manages Streamlit session state with type-safe accessors."""

    # Session state key definitions for type safety
    QUERY_KEYS: Dict[str, Union[None, str, bool, Callable]] = {
        "selected_query_name": None,
        "executed_query_name": None,
        "executed_sql_string": "",
        "query_results_df": lambda: pd.DataFrame(),
        "show_query_results": False,
        "applied_knesset_filter_to_query": lambda: [],
        "last_executed_sql": "",
        "applied_filters_info_query": lambda: [],
    }

    TABLE_EXPLORER_KEYS = {
        "selected_table_for_explorer": None,
        "executed_table_explorer_name": None,
        "table_explorer_df": lambda: pd.DataFrame(),
        "show_table_explorer_results": False,
    }

    FILTER_KEYS: Dict[str, Callable] = {
        "ms_knesset_filter": lambda: [],
        "ms_faction_filter": lambda: [],
    }

    PLOT_KEYS: Dict[str, Union[str, None, bool, Callable]] = {
        "selected_plot_topic": "",
        "selected_plot_name_from_topic": "",
        "generated_plot_figure": None,
        "plot_main_knesset_selection": "",
        "plot_aggregation_level": "Yearly",
        "plot_show_average_line": False,
        "plot_start_date": None,
        "plot_end_date": None,
        # Advanced filter keys
        "plot_query_type_filter": lambda: [],
        "plot_query_status_filter": lambda: [],
        "plot_session_type_filter": lambda: [],
        "plot_agenda_status_filter": lambda: [],
        "plot_bill_type_filter": lambda: [],
        "plot_bill_status_filter": lambda: [],
        # Available options for filters (populated dynamically)
        "available_query_types": lambda: [],
        "available_query_statuses": lambda: [],
        "available_session_types": lambda: [],
        "available_agenda_statuses": lambda: [],
        "available_bill_types": lambda: [],
        "available_bill_statuses": lambda: [],
    }

    CHART_BUILDER_KEYS = {
        "builder_selected_table": None,
        "builder_selected_table_previous_run": None,
    }

    @classmethod
    def initialize_all_session_state(cls) -> None:
        """Initialize all session state variables with their default values."""
        all_keys = {
            **cls.QUERY_KEYS,
            **cls.TABLE_EXPLORER_KEYS,
            **cls.FILTER_KEYS,
            **cls.PLOT_KEYS,
            **cls.CHART_BUILDER_KEYS,
        }

        for key, default_value in all_keys.items():
            if key not in st.session_state:
                if callable(default_value):
                    st.session_state[key] = default_value()
                else:
                    st.session_state[key] = default_value

    @classmethod
    def reset_query_state(cls) -> None:
        """Reset all query-related session state."""
        for key, default_value in cls.QUERY_KEYS.items():
            if callable(default_value):
                st.session_state[key] = default_value()
            else:
                st.session_state[key] = default_value

    @classmethod
    def reset_plot_state(cls, keep_topic: bool = False) -> None:
        """
        Reset plot-related session state.

        Args:
            keep_topic: If True, keeps the selected plot topic
        """
        for key, default_value in cls.PLOT_KEYS.items():
            if keep_topic and key == "selected_plot_topic":
                continue
            if callable(default_value):
                st.session_state[key] = default_value()
            else:
                st.session_state[key] = default_value

    @classmethod
    def reset_table_explorer_state(cls) -> None:
        """Reset table explorer-related session state."""
        for key, default_value in cls.TABLE_EXPLORER_KEYS.items():
            if callable(default_value):
                st.session_state[key] = default_value()
            else:
                st.session_state[key] = default_value

    # Type-safe getters
    @classmethod
    def get_selected_query_name(cls) -> Optional[str]:
        """Get the currently selected query name."""
        return st.session_state.get("selected_query_name")

    @classmethod
    def get_executed_query_name(cls) -> Optional[str]:
        """Get the name of the last executed query."""
        return st.session_state.get("executed_query_name")

    @classmethod
    def get_query_results_df(cls) -> pd.DataFrame:
        """Get the query results dataframe."""
        return st.session_state.get("query_results_df", pd.DataFrame())

    @classmethod
    def get_show_query_results(cls) -> bool:
        """Check if query results should be shown."""
        return st.session_state.get("show_query_results", False)

    @classmethod
    def get_last_executed_sql(cls) -> str:
        """Get the last executed SQL string."""
        return st.session_state.get("last_executed_sql", "")

    @classmethod
    def get_applied_filters_info_query(cls) -> List[str]:
        """Get the applied filters information for queries."""
        return st.session_state.get("applied_filters_info_query", [])

    @classmethod
    def get_knesset_filter(cls) -> List[int]:
        """Get the current Knesset filter."""
        return st.session_state.get("ms_knesset_filter", [])

    @classmethod
    def get_faction_filter(cls) -> List[str]:
        """Get the current faction filter."""
        return st.session_state.get("ms_faction_filter", [])

    @classmethod
    def get_selected_plot_topic(cls) -> str:
        """Get the selected plot topic."""
        return st.session_state.get("selected_plot_topic", "")

    @classmethod
    def get_selected_plot_name(cls) -> str:
        """Get the selected plot name within the topic."""
        return st.session_state.get("selected_plot_name_from_topic", "")

    @classmethod
    def get_plot_main_knesset_selection(cls) -> str:
        """Get the main Knesset selection for plots."""
        return st.session_state.get("plot_main_knesset_selection", "")

    @classmethod
    def get_plot_aggregation_level(cls) -> str:
        """Get the plot aggregation level."""
        return st.session_state.get("plot_aggregation_level", "Yearly")

    @classmethod
    def get_plot_show_average_line(cls) -> bool:
        """Check if average line should be shown in plots."""
        return st.session_state.get("plot_show_average_line", False)

    @classmethod
    def get_plot_start_date(cls) -> Optional[date]:
        """Get the plot start date filter."""
        return st.session_state.get("plot_start_date")

    @classmethod
    def get_plot_end_date(cls) -> Optional[date]:
        """Get the plot end date filter."""
        return st.session_state.get("plot_end_date")

    @classmethod
    def get_table_explorer_df(cls) -> pd.DataFrame:
        """Get the table explorer results dataframe."""
        return st.session_state.get("table_explorer_df", pd.DataFrame())

    @classmethod
    def get_executed_table_explorer_name(cls) -> Optional[str]:
        """Get the name of the last explored table."""
        return st.session_state.get("executed_table_explorer_name")

    @classmethod
    def get_show_table_explorer_results(cls) -> bool:
        """Check if table explorer results should be shown."""
        return st.session_state.get("show_table_explorer_results", False)

    @classmethod
    def get_builder_selected_table(cls) -> Optional[str]:
        """Get the selected table for chart builder."""
        return st.session_state.get("builder_selected_table")

    # Type-safe setters
    @classmethod
    def set_query_results(
        cls, query_name: str, results_df: pd.DataFrame, executed_sql: str, applied_filters: List[str]
    ) -> None:
        """Set query results in session state."""
        st.session_state.executed_query_name = query_name
        st.session_state.query_results_df = results_df
        st.session_state.last_executed_sql = executed_sql
        st.session_state.applied_filters_info_query = applied_filters
        st.session_state.show_query_results = True

    @classmethod
    def set_table_explorer_results(cls, table_name: str, results_df: pd.DataFrame) -> None:
        """Set table explorer results in session state."""
        st.session_state.executed_table_explorer_name = table_name
        st.session_state.table_explorer_df = results_df
        st.session_state.show_table_explorer_results = True

    @classmethod
    def set_plot_selection(cls, topic: str, plot_name: str) -> None:
        """Set plot selection in session state."""
        st.session_state.selected_plot_topic = topic
        st.session_state.selected_plot_name_from_topic = plot_name

    @classmethod
    def set_plot_knesset_selection(cls, knesset_selection: str) -> None:
        """Set the main Knesset selection for plots."""
        st.session_state.plot_main_knesset_selection = knesset_selection

    @classmethod
    def set_plot_figure(cls, figure: Any) -> None:
        """Set the generated plot figure."""
        st.session_state.generated_plot_figure = figure

    @classmethod
    def set_filters(cls, knesset_filter: List[int], faction_filter: List[str]) -> None:
        """Set the global filters."""
        st.session_state.ms_knesset_filter = knesset_filter
        st.session_state.ms_faction_filter = faction_filter
