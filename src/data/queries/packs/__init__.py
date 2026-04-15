"""Query packs for predefined UI queries."""

from .agenda import AGENDA_QUERIES
from .bills import BILLS_QUERIES
from .parliamentary import PARLIAMENTARY_QUERIES
from .registry import build_predefined_queries

__all__ = [
    "PARLIAMENTARY_QUERIES",
    "AGENDA_QUERIES",
    "BILLS_QUERIES",
    "build_predefined_queries",
]
