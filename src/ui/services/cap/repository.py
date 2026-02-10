"""CAP Annotation Repository facade.

This module keeps CAPAnnotationRepository's public API stable while delegating
implementation details to focused read/write/cache operation modules.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from . import repository_command_ops as command_ops
from . import repository_query_ops as query_ops
from .repository_cache_ops import clear_annotation_counts_cache, get_annotation_counts_cached


class CAPAnnotationRepository:
    """Repository for CAP bill annotation CRUD operations."""

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)

    def get_annotation_counts(self) -> dict[int, int]:
        """Get cached annotation counts for all bills."""
        return get_annotation_counts_cached(str(self.db_path))

    def get_uncoded_bills(
        self,
        knesset_num: Optional[int] = None,
        limit: int = 100,
        search_term: Optional[str] = None,
        researcher_id: Optional[int] = None,
    ) -> pd.DataFrame:
        return query_ops.get_uncoded_bills(
            self,
            knesset_num=knesset_num,
            limit=limit,
            search_term=search_term,
            researcher_id=researcher_id,
        )

    def get_coded_bills(
        self,
        knesset_num: Optional[int] = None,
        cap_code: Optional[int] = None,
        limit: int = 100,
        researcher_id: Optional[int] = None,
    ) -> pd.DataFrame:
        return query_ops.get_coded_bills(
            self,
            knesset_num=knesset_num,
            cap_code=cap_code,
            limit=limit,
            researcher_id=researcher_id,
        )

    def get_recent_annotations(
        self,
        limit: int = 5,
        researcher_id: Optional[int] = None,
    ) -> pd.DataFrame:
        return query_ops.get_recent_annotations(
            self,
            limit=limit,
            researcher_id=researcher_id,
        )

    def get_bills_with_status(
        self,
        knesset_num: Optional[int] = None,
        limit: int = 100,
        search_term: Optional[str] = None,
        include_coded: bool = False,
        researcher_id: Optional[int] = None,
    ) -> pd.DataFrame:
        return query_ops.get_bills_with_status(
            self,
            knesset_num=knesset_num,
            limit=limit,
            search_term=search_term,
            include_coded=include_coded,
            researcher_id=researcher_id,
        )

    def get_annotation_by_bill_id(
        self,
        bill_id: int,
        researcher_id: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        return query_ops.get_annotation_by_bill_id(
            self,
            bill_id=bill_id,
            researcher_id=researcher_id,
        )

    def get_all_annotations_for_bill(self, bill_id: int) -> pd.DataFrame:
        return query_ops.get_all_annotations_for_bill(self, bill_id)

    def save_annotation(
        self,
        bill_id: int,
        cap_minor_code: int,
        researcher_id: int,
        confidence: str = "Medium",
        notes: str = "",
        source: str = "Database",
        submission_date: str = "",
    ) -> bool:
        return command_ops.save_annotation(
            self,
            bill_id=bill_id,
            cap_minor_code=cap_minor_code,
            researcher_id=researcher_id,
            confidence=confidence,
            notes=notes,
            source=source,
            submission_date=submission_date,
            clear_cache_callback=clear_annotation_counts_cache,
        )

    def delete_annotation(
        self,
        bill_id: int,
        researcher_id: Optional[int] = None,
    ) -> bool:
        return command_ops.delete_annotation(
            self,
            bill_id=bill_id,
            researcher_id=researcher_id,
            clear_cache_callback=clear_annotation_counts_cache,
        )

    def get_bill_documents(self, bill_id: int) -> pd.DataFrame:
        return query_ops.get_bill_documents(self, bill_id)

    def get_bills_not_in_database(
        self,
        api_bills: pd.DataFrame,
        limit: int = 100,
        researcher_id: Optional[int] = None,
    ) -> pd.DataFrame:
        return query_ops.get_bills_not_in_database(
            self,
            api_bills=api_bills,
            limit=limit,
            researcher_id=researcher_id,
        )


def get_annotation_repository(
    db_path: Path,
    logger_obj: Optional[logging.Logger] = None,
) -> CAPAnnotationRepository:
    """Factory function to get a CAP annotation repository instance."""
    return CAPAnnotationRepository(db_path, logger_obj)
