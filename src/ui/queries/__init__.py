"""Backward-compat shim. Canonical location: :mod:`data.queries`.

Reusable query building blocks (``SQLTemplates``, ``FilterBuilder``, typed
records, predefined query definitions) now live under :mod:`data.queries`.
This package still hosts :class:`QueryExecutor` (which depends on Streamlit
and is UI-layer only) and re-exports the moved symbols for legacy callers.

New code — especially anything that runs outside Streamlit (FastAPI,
exporters, scripts) — should import from :mod:`data.queries` directly.
"""

from data.queries.filter_builder import FilterBuilder
from data.queries.predefined_queries import (
    PREDEFINED_QUERIES,
    get_all_query_names,
    get_filter_columns,
    get_query_definition,
    get_query_info,
    get_query_sql,
)
from data.queries.sql_templates import SQLTemplates
from data.queries.types import (
    PaginationSpec,
    QueryDefinition,
    QueryExecutionResult,
    QueryRequest,
)

__all__ = [
    "FilterBuilder",
    "PREDEFINED_QUERIES",
    "PaginationSpec",
    "QueryDefinition",
    "QueryExecutionResult",
    "QueryRequest",
    "SQLTemplates",
    "get_all_query_names",
    "get_filter_columns",
    "get_query_definition",
    "get_query_info",
    "get_query_sql",
]
