"""Tests for secure query builder and predefined query registry."""

from __future__ import annotations

import pytest

from ui.queries.predefined_queries import PREDEFINED_QUERIES, get_all_query_names, get_query_definition
from utils.query_builder import QueryTemplate, SecureQueryBuilder


def test_predefined_query_registry_contains_expected_keys_in_order():
    """Registry should expose all query packs in stable order."""
    expected = [
        "Parliamentary Queries (Full Details)",
        "Agenda Motions (Full Details)",
        "Bills & Legislation (Full Details)",
    ]
    assert get_all_query_names() == expected
    assert list(PREDEFINED_QUERIES.keys()) == expected


def test_get_query_definition_returns_typed_values():
    """Typed query definition should preserve metadata from registry."""
    definition = get_query_definition("Parliamentary Queries (Full Details)")
    assert definition is not None
    assert definition.knesset_filter_column == "Q.KnessetNum"
    assert definition.faction_filter_column == "f.FactionID"
    assert "SELECT" in definition.sql


def test_build_time_series_query_uses_stable_named_time_unit_placeholder():
    """Time-series queries should not use positional placeholder hacks."""
    builder = SecureQueryBuilder()
    query = QueryTemplate.build_time_series_query(
        date_column="SubmitDate",
        metric_column="QueryID",
        table_name="KNS_Query",
        builder=builder,
        time_unit="month",
    )

    params = builder.get_parameters()
    assert params["time_unit"] == "month"
    assert "$time_unit" in query
    assert "GROUP BY DATE_TRUNC($time_unit" in query
    assert "$1" not in query
    assert "CURRENT_DATE + INTERVAL '1 year'" in query


def test_build_time_series_query_rejects_invalid_time_unit():
    """Only explicit time buckets should be accepted."""
    builder = SecureQueryBuilder()
    with pytest.raises(ValueError, match="Invalid time_unit"):
        QueryTemplate.build_time_series_query(
            date_column="SubmitDate",
            metric_column="QueryID",
            table_name="KNS_Query",
            builder=builder,
            time_unit="month; DROP TABLE KNS_Query",
        )


def test_build_faction_analysis_query_validates_metric_alias():
    """Metric alias must be a safe SQL identifier."""
    builder = SecureQueryBuilder()
    with pytest.raises(ValueError, match="Invalid metric name"):
        QueryTemplate.build_faction_analysis_query(
            metric_column="QueryID",
            metric_name="count(*)",
            table_name="KNS_Query",
            builder=builder,
        )

