"""Backward-compat stub. Canonical location: ``data.queries.predefined_queries``."""

from data.queries.predefined_queries import *  # noqa: F401,F403
from data.queries.predefined_queries import (  # noqa: F401
    PREDEFINED_QUERIES,
    get_all_query_names,
    get_filter_columns,
    get_query_definition,
    get_query_info,
    get_query_sql,
)

__all__ = [
    "PREDEFINED_QUERIES",
    "get_all_query_names",
    "get_filter_columns",
    "get_query_definition",
    "get_query_info",
    "get_query_sql",
]
