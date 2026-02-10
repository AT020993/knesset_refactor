"""Registry composition for predefined query packs."""

from __future__ import annotations

from typing import Any

from .agenda import AGENDA_QUERIES
from .bills import BILLS_QUERIES
from .parliamentary import PARLIAMENTARY_QUERIES


def build_predefined_queries() -> dict[str, dict[str, Any]]:
    """Build predefined query registry in stable display order."""
    queries: dict[str, dict[str, Any]] = {}
    queries.update(PARLIAMENTARY_QUERIES)
    queries.update(AGENDA_QUERIES)
    queries.update(BILLS_QUERIES)
    return queries
