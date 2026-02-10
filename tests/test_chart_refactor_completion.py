"""Targeted coverage for completed chart refactor items."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import streamlit as st

from ui.charts.comparison.query_charts import QueryComparisonCharts
from ui.charts.distribution import DistributionCharts
from ui.charts.factory import ChartFactory
from ui.renderers.plots import generation_ops


def test_queries_by_coalition_status_chart_generation(monkeypatch, tmp_path, mock_logger):
    """Queries-by-coalition chart should return a figure once data is available."""
    db_path = tmp_path / "warehouse.duckdb"
    db_path.touch()
    chart = QueryComparisonCharts(db_path, mock_logger)

    monkeypatch.setattr(chart, "check_database_exists", lambda: True)
    monkeypatch.setattr(chart, "check_tables_exist", lambda con, tables: True)
    monkeypatch.setattr(
        "ui.charts.comparison.query_charts.get_db_connection",
        lambda *args, **kwargs: nullcontext(mock.MagicMock()),
    )
    monkeypatch.setattr(
        "ui.charts.comparison.query_charts.safe_execute_query",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "CoalitionStatus": ["Coalition", "Opposition"],
                "QueryCount": [11, 7],
            }
        ),
    )

    fig = chart.plot_queries_by_coalition_status(
        knesset_filter=[25],
        faction_filter=[1, 2],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert fig is not None
    assert len(fig.data) == 1


def test_query_status_distribution_chart_generation(monkeypatch, tmp_path, mock_logger):
    """Query-status distribution chart should return a figure once data is available."""
    db_path = tmp_path / "warehouse.duckdb"
    db_path.touch()
    chart = DistributionCharts(db_path, mock_logger)

    monkeypatch.setattr(chart, "check_database_exists", lambda: True)
    monkeypatch.setattr(chart, "check_tables_exist", lambda con, tables: True)
    monkeypatch.setattr(
        "ui.charts.distribution.get_db_connection",
        lambda *args, **kwargs: nullcontext(mock.MagicMock()),
    )
    monkeypatch.setattr(
        "ui.charts.distribution.safe_execute_query",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "Status": ["Answered", "Not Answered"],
                "Count": [9, 4],
            }
        ),
    )

    fig = chart.plot_query_status_distribution(
        knesset_filter=[25],
        faction_filter=[1],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert fig is not None
    assert len(fig.data) == 1


def test_chart_factory_includes_completed_chart_types(tmp_path):
    """Factory chart registry should include newly implemented chart types."""
    db_path = tmp_path / "warehouse.duckdb"
    factory = ChartFactory(db_path, mock.MagicMock())
    available = factory.get_available_charts()

    assert "query_status_distribution" in available["distribution"]
    assert "queries_by_coalition_status" in available["comparison"]


def test_plot_args_include_date_filters_for_current_query_chart_labels(monkeypatch):
    """Date range wiring should match current query chart labels."""
    renderer = SimpleNamespace(db_path=Path("data/warehouse.duckdb"), logger=mock.MagicMock())
    st.session_state["plot_query_type_filter"] = ["רגילה"]
    st.session_state["plot_query_status_filter"] = ["התקבלה תשובה"]

    monkeypatch.setattr(
        generation_ops.SessionStateManager, "get_faction_filter", lambda: []
    )
    monkeypatch.setattr(
        generation_ops.SessionStateManager, "get_plot_start_date", lambda: date(2024, 1, 1)
    )
    monkeypatch.setattr(
        generation_ops.SessionStateManager, "get_plot_end_date", lambda: date(2024, 12, 31)
    )

    args_for_breakdown = generation_ops.build_plot_arguments(
        renderer=renderer,
        final_knesset_filter=[25],
        faction_display_map={},
        connect_func=mock.MagicMock(),
        selected_chart="Query Types Breakdown",
    )
    args_for_status = generation_ops.build_plot_arguments(
        renderer=renderer,
        final_knesset_filter=[25],
        faction_display_map={},
        connect_func=mock.MagicMock(),
        selected_chart="Query Status by Faction",
    )

    assert args_for_breakdown["start_date"] == "2024-01-01"
    assert args_for_breakdown["end_date"] == "2024-12-31"
    assert args_for_status["start_date"] == "2024-01-01"
    assert args_for_status["end_date"] == "2024-12-31"


@mock.patch("ui.plot_generators.ChartService")
def test_legacy_wrapper_for_queries_by_coalition_status(mock_chart_service):
    """Legacy wrapper should delegate to ChartService for coalition status queries."""
    from ui.plot_generators import plot_queries_by_coalition_status

    mock_instance = mock_chart_service.return_value
    mock_instance.plot_queries_by_coalition_status.return_value = object()

    result = plot_queries_by_coalition_status(
        Path("data/warehouse.duckdb"),
        mock.MagicMock(),
        mock.MagicMock(),
        knesset_filter=[25],
    )

    assert result is not None
    mock_instance.plot_queries_by_coalition_status.assert_called_once()
