"""Caching operations for CAP annotation repository."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from backend.connection_manager import get_db_connection


def _get_annotation_counts_impl(db_path_str: str) -> dict[int, int]:
    """Compute annotation counts for all bills."""
    try:
        db_path = Path(db_path_str)
        with get_db_connection(db_path, read_only=True) as conn:
            result = conn.execute(
                """
                SELECT BillID, COUNT(*) as total
                FROM UserBillCAP
                GROUP BY BillID
                """
            ).fetchdf()

            if result.empty:
                return {}

            return dict(zip(result["BillID"].tolist(), result["total"].tolist()))
    except Exception:
        return {}


try:
    _get_cached_annotation_counts = st.cache_data(ttl=600, show_spinner=False)(
        _get_annotation_counts_impl
    )
except Exception:
    _get_cached_annotation_counts = _get_annotation_counts_impl


def get_annotation_counts_cached(db_path_str: str) -> dict[int, int]:
    """Return cached annotation counts."""
    return _get_cached_annotation_counts(db_path_str)


def clear_annotation_counts_cache() -> None:
    """Clear cached annotation counts."""
    try:
        if hasattr(_get_cached_annotation_counts, "clear"):
            _get_cached_annotation_counts.clear()
    except Exception:
        pass

