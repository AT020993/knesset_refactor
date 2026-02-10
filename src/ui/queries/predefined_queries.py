"""Predefined SQL query registry and typed accessors."""

from __future__ import annotations

from typing import Any

from ui.queries.packs import build_predefined_queries
from ui.queries.types import QueryDefinition

# Query definitions with their SQL and metadata
PREDEFINED_QUERIES: dict[str, dict[str, Any]] = build_predefined_queries()


def get_query_sql(query_name: str) -> str:
    """Get the SQL for a specific query."""
    sql = PREDEFINED_QUERIES.get(query_name, {}).get("sql", "")
    return sql if isinstance(sql, str) else ""


def get_query_info(query_name: str) -> dict[str, Any]:
    """Get all information for a specific query."""
    return PREDEFINED_QUERIES.get(query_name, {})


def get_query_definition(query_name: str) -> QueryDefinition | None:
    """Get a typed query definition for a specific query."""
    query_info = get_query_info(query_name)
    if not query_info:
        return None

    return QueryDefinition(
        name=query_name,
        sql=query_info.get("sql", ""),
        knesset_filter_column=query_info.get("knesset_filter_column"),
        faction_filter_column=query_info.get("faction_filter_column"),
        description=query_info.get("description", ""),
    )


def get_all_query_names() -> list[str]:
    """Get list of all available query names."""
    return list(PREDEFINED_QUERIES.keys())


def get_filter_columns(query_name: str) -> tuple[str | None, str | None]:
    """Get the filter column names for a query."""
    query_info = PREDEFINED_QUERIES.get(query_name, {})
    return (
        query_info.get("knesset_filter_column"),
        query_info.get("faction_filter_column"),
    )
