"""Predefined visualizations page orchestration.

This module keeps the PlotsPageRenderer public interface stable while
delegating selection and generation internals to focused operations modules.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

import streamlit as st

from ui.renderers.plots import generation_ops, selection_ops
from ui.renderers.plots.filter_panels import PlotFilterPanels


class PlotsPageRenderer:
    """Handles rendering of the predefined visualizations section."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger
        self._filter_panels = PlotFilterPanels(db_path, logger)

    def render_plots_section(
        self,
        available_plots: dict[str, dict[str, Callable]],
        knesset_options: list[str],
        faction_display_map: dict[str, int],
        connect_func: Callable,
    ) -> None:
        """Render the complete plots selection and generation interface."""
        st.divider()
        st.header("📈 Predefined Visualizations")

        if not self.db_path.exists():
            st.warning(
                "Database not found. Visualizations cannot be generated. "
                "Please run a data refresh."
            )
            return

        selected_topic = self._render_topic_selection(available_plots)
        if not selected_topic:
            st.info("Select a plot topic to see available visualizations.")
            return

        selected_chart = self._render_chart_selection(available_plots, selected_topic)
        if not selected_chart:
            st.info("Please choose a specific visualization from the dropdown above.")
            return

        self._render_plot_options(selected_chart, knesset_options)
        self._generate_and_display_plot(
            available_plots,
            selected_topic,
            selected_chart,
            faction_display_map,
            connect_func,
        )

    def _render_topic_selection(
        self,
        available_plots: dict[str, dict[str, Callable]],
    ) -> str:
        return selection_ops.render_topic_selection(self, available_plots)

    def _on_topic_selection_change(self) -> None:
        selection_ops.on_topic_selection_change()

    def _render_chart_selection(
        self,
        available_plots: dict[str, dict[str, Callable]],
        selected_topic: str,
    ) -> str:
        return selection_ops.render_chart_selection(self, available_plots, selected_topic)

    def _on_chart_selection_change(self, widget_key: str, topic: str) -> None:
        selection_ops.on_chart_selection_change(topic=topic, widget_key=widget_key)

    def _render_plot_options(self, selected_chart: str, knesset_options: list[str]) -> None:
        selection_ops.render_plot_options(self, selected_chart, knesset_options)

    def _render_time_period_plot_options(
        self,
        selected_chart: str,
        plot_knesset_options: list[str],
        current_selection: str,
    ) -> None:
        selection_ops.render_time_period_plot_options(
            self,
            selected_chart,
            plot_knesset_options,
            current_selection,
        )

    def _render_single_knesset_plot_options(
        self,
        selected_chart: str,
        plot_knesset_options: list[str],
        current_selection: str,
        can_show_all_knessets: bool,
    ) -> None:
        selection_ops.render_single_knesset_plot_options(
            self,
            selected_chart,
            plot_knesset_options,
            current_selection,
            can_show_all_knessets,
        )

    def _on_knesset_selection_change(self, widget_key: str) -> None:
        selection_ops.on_knesset_selection_change(widget_key)

    def _render_date_filter_options(self, selected_chart: str) -> None:
        selection_ops.render_date_filter_options(selected_chart)

    def _generate_and_display_plot(
        self,
        available_plots: dict[str, dict[str, Callable]],
        selected_topic: str,
        selected_chart: str,
        faction_display_map: dict[str, int],
        connect_func: Callable,
    ) -> None:
        generation_ops.generate_and_display_plot(
            self,
            available_plots,
            selected_topic,
            selected_chart,
            faction_display_map,
            connect_func,
        )

    def _get_final_knesset_filter(self, selected_chart: str) -> list[int] | None:
        return generation_ops.get_final_knesset_filter(self, selected_chart)

    def _build_plot_arguments(
        self,
        final_knesset_filter: list[int] | None,
        faction_display_map: dict[str, int],
        connect_func: Callable,
        selected_chart: str,
    ) -> dict[str, Any]:
        return generation_ops.build_plot_arguments(
            self,
            final_knesset_filter,
            faction_display_map,
            connect_func,
            selected_chart,
        )
