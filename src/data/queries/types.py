"""Typed contracts for predefined query execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import pandas as pd


@dataclass(frozen=True)
class PaginationSpec:
    """Pagination settings for query execution."""

    limit: int | None = None
    offset: int = 0


@dataclass(frozen=True)
class QueryDefinition:
    """Canonical typed representation of a predefined query."""

    name: str
    sql: str
    knesset_filter_column: str | None = None
    faction_filter_column: str | None = None
    description: str = ""


@dataclass(frozen=True)
class QueryRequest:
    """Input request for a query execution."""

    definition: QueryDefinition
    knesset_numbers: Sequence[int] = field(default_factory=tuple)
    faction_ids: Sequence[int] = field(default_factory=tuple)
    document_types: Sequence[str] = field(default_factory=tuple)
    pagination: PaginationSpec = field(default_factory=PaginationSpec)


@dataclass(frozen=True)
class QueryExecutionResult:
    """Result payload for query execution."""

    dataframe: pd.DataFrame
    executed_sql: str
    params: list[Any]
    applied_filters: list[str]
