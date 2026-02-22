"""Plot generation operations for plots page."""

from __future__ import annotations

import sys
from typing import Any

import streamlit as st

import ui.ui_utils as ui_utils
from ui.state.session_manager import SessionStateManager
from utils.performance_utils import reduce_plotly_figure_size


def get_final_knesset_filter(renderer: Any, selected_chart: str) -> list[int] | None:
    """Resolve final Knesset filter from current selection."""
    current_selection = SessionStateManager.get_plot_main_knesset_selection()
    can_show_all_knessets = selected_chart in [
        "Queries Over Time",
        "Queries by Time Period",
        "Agendas Over Time",
        "Agenda Items by Time Period",
        "Bills Over Time",
        "Bills by Time Period",
    ]

    if current_selection == "All Knessets (Color Coded)" and can_show_all_knessets:
        renderer.logger.info(
            f"Plot '{selected_chart}': Showing all Knessets (color coded)."
        )
        return None

    if current_selection and current_selection != "":
        try:
            final_knesset_filter = [int(current_selection)]
            renderer.logger.info(
                f"Plot '{selected_chart}': Using main area Knesset selection: {final_knesset_filter}"
            )
            return final_knesset_filter
        except ValueError:
            st.error(f"Invalid Knesset number selected: {current_selection}")
            return []

    return []


def build_plot_arguments(
    renderer: Any,
    final_knesset_filter: list[int] | None,
    faction_display_map: dict[str, int],
    connect_func,
    selected_chart: str,
) -> dict[str, Any]:
    """Build plot function keyword arguments from current state."""
    faction_filter = SessionStateManager.get_faction_filter()
    plot_args: dict[str, Any] = {
        "db_path": renderer.db_path,
        "connect_func": connect_func,
        "logger_obj": renderer.logger,
        "knesset_filter": final_knesset_filter,
        "faction_filter": [
            faction_display_map[name]
            for name in faction_filter
            if name in faction_display_map
        ],
    }

    if selected_chart in [
        "Queries by Time Period",
        "Agenda Items by Time Period",
        "Bills by Time Period",
    ]:
        plot_args["aggregation_level"] = SessionStateManager.get_plot_aggregation_level()
        plot_args["show_average_line"] = SessionStateManager.get_plot_show_average_line()
    elif selected_chart in [
        "Query Status Description with Faction Breakdown (Single Knesset)",
        "Query Types Distribution",
        "Query Types Breakdown",
        "Query Status by Faction",
        "Query Status Distribution",
        "Queries by Coalition Status",
    ]:
        start_date = SessionStateManager.get_plot_start_date()
        end_date = SessionStateManager.get_plot_end_date()
        plot_args["start_date"] = start_date.strftime("%Y-%m-%d") if start_date else None
        plot_args["end_date"] = end_date.strftime("%Y-%m-%d") if end_date else None

    if "Query" in selected_chart or "Queries" in selected_chart:
        plot_args["query_type_filter"] = st.session_state.get("plot_query_type_filter", [])
        plot_args["query_status_filter"] = st.session_state.get("plot_query_status_filter", [])
    elif "Agenda" in selected_chart or "Agendas" in selected_chart:
        plot_args["session_type_filter"] = st.session_state.get("plot_session_type_filter", [])
        plot_args["agenda_status_filter"] = st.session_state.get(
            "plot_agenda_status_filter", []
        )
    elif "Bill" in selected_chart or "Bills" in selected_chart:
        plot_args["bill_origin_filter"] = st.session_state.get(
            "plot_bill_origin_filter", "All Bills"
        )
        if "Major Topic" in selected_chart or "Minor Topic" in selected_chart:
            plot_args["show_percentage"] = st.session_state.get(
                "plot_show_percentage", False
            )
    elif "Collaboration" in selected_chart or "Network" in selected_chart:
        if "Faction Collaboration Network" not in selected_chart:
            plot_args["min_collaborations"] = st.session_state.get(
                "plot_min_collaborations", 3
            )
        if "Faction Collaboration Matrix" in selected_chart:
            plot_args["show_solo_bills"] = st.session_state.get(
                "plot_show_solo_bills", True
            )
            plot_args["min_total_bills"] = st.session_state.get(
                "plot_min_total_bills", 1
            )

    return plot_args


def generate_and_display_plot(
    renderer: Any,
    available_plots,
    selected_topic: str,
    selected_chart: str,
    faction_display_map: dict[str, int],
    connect_func,
) -> None:
    """Generate and render selected plot with current options."""
    final_knesset_filter = get_final_knesset_filter(renderer, selected_chart)
    can_generate_plot = selected_chart and (
        final_knesset_filter is None or len(final_knesset_filter) > 0
    )

    if not can_generate_plot:
        multi_knesset_charts = [
            "Queries Over Time",
            "Queries by Time Period",
            "Agendas Over Time",
            "Agenda Items by Time Period",
            "Bills Over Time",
            "Bills by Time Period",
        ]
        requires_single_knesset = selected_chart not in multi_knesset_charts
        if requires_single_knesset:
            st.info(
                f"ℹ️ Please select a single Knesset for '{selected_chart}'. "
                "This chart requires a specific Knesset selection."
            )
        return

    plot_function = available_plots[selected_topic][selected_chart]
    plot_args = build_plot_arguments(
        renderer,
        final_knesset_filter,
        faction_display_map,
        connect_func,
        selected_chart,
    )

    spinner_messages = {
        "Queries Over Time": "Loading query data and generating time series...",
        "Queries by Time Period": "Loading query data and generating time series...",
        "Agendas Over Time": "Loading agenda data and generating time series...",
        "Agendas by Time Period": "Loading agenda data and generating time series...",
        "Bills Over Time": "Loading bill data and generating time series...",
        "Bills by Time Period": "Loading bill data and generating time series...",
        "Legislator Collaboration Network": "Analyzing collaboration patterns (this may take a moment)...",
        "MK Collaboration Network": "Analyzing collaboration patterns (this may take a moment)...",
        "Faction Collaboration Network": "Computing faction relationships...",
        "Cross-Party Collaboration Matrix": "Building collaboration matrix...",
        "Faction Collaboration Matrix": "Building collaboration matrix...",
    }
    spinner_msg = spinner_messages.get(
        selected_chart,
        f"Generating '{selected_chart}'...",
    )

    with st.spinner(spinner_msg):
        try:
            figure = plot_function(**plot_args)
            if figure:
                # Optimize large figures for faster rendering
                if any(
                    len(x) > 500
                    for trace in figure.data
                    if (x := getattr(trace, 'x', None)) is not None
                ):
                    figure = reduce_plotly_figure_size(figure)
                st.plotly_chart(
                    figure,
                    use_container_width=True,
                    config={
                        "displayModeBar": True,
                        "displaylogo": False,
                        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
                        "toImageButtonOptions": {
                            "format": "png",
                            "filename": f"knesset_{selected_chart.replace(' ', '_')}",
                            "height": 800,
                            "width": 1400,
                            "scale": 2,
                        },
                    },
                )
                SessionStateManager.set_plot_figure(figure)

                if "Network" in selected_chart and "Matrix" not in selected_chart:
                    from ui.charts.network import NetworkCharts

                    with st.expander(
                        "📐 How Distance is Calculated in This Chart",
                        expanded=False,
                    ):
                        st.markdown(NetworkCharts.get_layout_explanation())
        except Exception as exc:
            renderer.logger.error(
                f"Error displaying plot '{selected_chart}': {exc}",
                exc_info=True,
            )
            st.error(
                f"An error occurred while generating the plot: {ui_utils.format_exception_for_ui(sys.exc_info())}"
            )
