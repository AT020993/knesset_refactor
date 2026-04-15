"""Reusable query layer (SQL templates, filter builder, predefined query packs).

Canonical location for query building blocks. The legacy ``ui.queries`` package
re-exports from here for backward compatibility with existing Streamlit code;
new callers (FastAPI, exporters, scripts) should import from ``data.queries``
directly.
"""

from .filter_builder import FilterBuilder
from .predefined_queries import (
    PREDEFINED_QUERIES,
    get_all_query_names,
    get_filter_columns,
    get_query_definition,
    get_query_info,
    get_query_sql,
)
from .sql_templates import SQLTemplates
from .types import (
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
