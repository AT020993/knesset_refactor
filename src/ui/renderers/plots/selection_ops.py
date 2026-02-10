"""Selection and option rendering operations for plots page."""

from __future__ import annotations

from typing import Any

import streamlit as st

from ui.state.session_manager import SessionStateManager


def render_topic_selection(renderer: Any, available_plots: dict[str, dict[str, Any]]) -> str:
    """Render plot topic selection and return selected topic."""
    plot_topic_options = [""] + list(available_plots.keys())
    current_selected_topic = SessionStateManager.get_selected_plot_topic()
    topic_select_default_index = (
        plot_topic_options.index(current_selected_topic)
        if current_selected_topic in plot_topic_options
        else 0
    )

    st.selectbox(
        "1. Choose Plot Topic:",
        options=plot_topic_options,
        index=topic_select_default_index,
        key="sb_selected_plot_topic_widget",
        on_change=renderer._on_topic_selection_change,
    )

    return SessionStateManager.get_selected_plot_topic()


def on_topic_selection_change() -> None:
    """Handle topic change callback."""
    new_topic = st.session_state.get("sb_selected_plot_topic_widget", "")
    SessionStateManager.reset_plot_state(keep_topic=False)
    SessionStateManager.set_plot_selection(new_topic, "")


def render_chart_selection(
    renderer: Any,
    available_plots: dict[str, dict[str, Any]],
    selected_topic: str,
) -> str:
    """Render chart selection and return selected chart."""
    if not selected_topic:
        return ""

    charts_in_topic = available_plots[selected_topic]
    chart_options_for_topic = [""] + list(charts_in_topic.keys())
    current_selected_chart = SessionStateManager.get_selected_plot_name()
    chart_select_default_index = (
        chart_options_for_topic.index(current_selected_chart)
        if current_selected_chart in chart_options_for_topic
        else 0
    )

    widget_key = f"sb_selected_chart_for_topic_{selected_topic.replace(' ', '_')}"
    st.selectbox(
        f"2. Choose Visualization for '{selected_topic}':",
        options=chart_options_for_topic,
        index=chart_select_default_index,
        key=widget_key,
        on_change=renderer._on_chart_selection_change,
        kwargs={"widget_key": widget_key, "topic": selected_topic},
    )

    return SessionStateManager.get_selected_plot_name()


def on_chart_selection_change(topic: str, widget_key: str) -> None:
    """Handle chart selection callback."""
    new_chart = st.session_state.get(widget_key, "")

    st.session_state.plot_aggregation_level = "Yearly"
    st.session_state.plot_show_average_line = False
    st.session_state.plot_start_date = None
    st.session_state.plot_end_date = None

    SessionStateManager.set_plot_selection(topic, new_chart)


def render_plot_options(
    renderer: Any,
    selected_chart: str,
    knesset_options: list[str],
) -> None:
    """Render chart-specific option controls."""
    if not selected_chart:
        return

    plot_knesset_options = [""] + knesset_options
    can_show_all_knessets = selected_chart in [
        "Queries by Time Period",
        "Agenda Items by Time Period",
    ]

    if (
        can_show_all_knessets
        and "All Knessets (Color Coded)" not in plot_knesset_options
    ):
        plot_knesset_options.insert(1, "All Knessets (Color Coded)")

    current_main_knesset_selection = SessionStateManager.get_plot_main_knesset_selection()
    if current_main_knesset_selection not in plot_knesset_options:
        current_main_knesset_selection = ""
        SessionStateManager.set_plot_knesset_selection("")

    if selected_chart in ["Queries by Time Period", "Agenda Items by Time Period"]:
        renderer._render_time_period_plot_options(
            selected_chart,
            plot_knesset_options,
            current_main_knesset_selection,
        )
    else:
        renderer._render_single_knesset_plot_options(
            selected_chart,
            plot_knesset_options,
            current_main_knesset_selection,
            can_show_all_knessets,
        )

    if selected_chart in [
        "Query Status Description with Faction Breakdown (Single Knesset)",
        "Query Types Distribution",
        "Query Types Breakdown",
        "Query Status by Faction",
        "Query Status Distribution",
        "Queries by Coalition Status",
    ]:
        renderer._render_date_filter_options(selected_chart)

    renderer._filter_panels.populate_filter_options()
    renderer._filter_panels.render_advanced_filters(selected_chart)


def render_time_period_plot_options(
    renderer: Any,
    selected_chart: str,
    plot_knesset_options: list[str],
    current_selection: str,
) -> None:
    """Render Knesset and aggregation controls for time-period charts."""
    knesset_select_default_index = (
        plot_knesset_options.index(current_selection)
        if current_selection in plot_knesset_options
        else 0
    )

    aggregation_level = SessionStateManager.get_plot_aggregation_level()
    show_average_line = SessionStateManager.get_plot_show_average_line()

    widget_key = f"plot_main_knesset_selector_tp_{selected_chart.replace(' ', '_')}"
    col_knesset_select, col_agg_select, col_avg_line = st.columns([2, 1, 1])

    with col_knesset_select:
        st.selectbox(
            "3. Select Knesset for Plot:",
            options=plot_knesset_options,
            index=knesset_select_default_index,
            key=widget_key,
            on_change=renderer._on_knesset_selection_change,
            kwargs={"widget_key": widget_key},
        )

    with col_agg_select:
        st.session_state.plot_aggregation_level = st.selectbox(
            "Aggregate:",
            options=["Yearly", "Monthly", "Quarterly"],
            index=["Yearly", "Monthly", "Quarterly"].index(aggregation_level),
            key=f"agg_level_{selected_chart.replace(' ', '_')}",
        )

    with col_avg_line:
        st.session_state.plot_show_average_line = st.checkbox(
            "Avg Line",
            value=show_average_line,
            key=f"avg_line_{selected_chart.replace(' ', '_')}",
        )


def render_single_knesset_plot_options(
    renderer: Any,
    selected_chart: str,
    plot_knesset_options: list[str],
    current_selection: str,
    can_show_all_knessets: bool,
) -> None:
    """Render Knesset selector for single-knesset charts."""
    _ = can_show_all_knessets
    options_for_single_knesset_plot = [
        opt
        for opt in plot_knesset_options
        if opt != "All Knessets (Color Coded)" and opt != ""
    ]

    if (
        current_selection not in options_for_single_knesset_plot
        and current_selection != ""
    ):
        current_selection = ""
        SessionStateManager.set_plot_knesset_selection("")

    effective_options_single = [""] + options_for_single_knesset_plot
    single_knesset_default_idx = (
        effective_options_single.index(current_selection)
        if current_selection in effective_options_single
        else 0
    )

    widget_key = (
        f"plot_main_knesset_selector_single_{selected_chart.replace(' ', '_')}"
    )
    st.selectbox(
        "3. Select Knesset for Plot:",
        options=effective_options_single,
        index=single_knesset_default_idx,
        key=widget_key,
        on_change=renderer._on_knesset_selection_change,
        kwargs={"widget_key": widget_key},
    )


def on_knesset_selection_change(widget_key: str) -> None:
    """Handle Knesset selectbox change callback."""
    new_knesset = st.session_state.get(widget_key, "")
    SessionStateManager.set_plot_knesset_selection(new_knesset)


def render_date_filter_options(selected_chart: str) -> None:
    """Render start/end date controls for eligible charts."""
    st.markdown("**Optional Date Range Filter:**")
    col_start_date, col_end_date = st.columns(2)

    with col_start_date:
        st.session_state.plot_start_date = st.date_input(
            "Start Date (optional)",
            value=SessionStateManager.get_plot_start_date(),
            key=f"start_date_{selected_chart.replace(' ', '_')}",
            help="Filter queries from this date onwards",
        )

    with col_end_date:
        st.session_state.plot_end_date = st.date_input(
            "End Date (optional)",
            value=SessionStateManager.get_plot_end_date(),
            key=f"end_date_{selected_chart.replace(' ', '_')}",
            help="Filter queries up to this date",
        )
