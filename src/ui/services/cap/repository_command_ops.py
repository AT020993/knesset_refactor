"""Write/command operations for CAP annotation repository."""

from __future__ import annotations

from typing import Any, Callable, Optional

from backend.connection_manager import get_db_connection


def save_annotation(
    repo: Any,
    bill_id: int,
    cap_minor_code: int,
    researcher_id: int,
    confidence: str = "Medium",
    notes: str = "",
    source: str = "Database",
    submission_date: str = "",
    clear_cache_callback: Optional[Callable[[], None]] = None,
) -> bool:
    """Create or update a researcher annotation for a bill."""
    if not isinstance(researcher_id, int):
        repo.logger.error(
            f"researcher_id must be int, got {type(researcher_id).__name__}: {researcher_id}. "
            "Did you pass cap_researcher_name instead of cap_user_id?"
        )
        return False

    if researcher_id <= 0:
        repo.logger.error(
            f"Invalid researcher_id: {researcher_id}. Must be a positive integer."
        )
        return False

    try:
        with get_db_connection(repo.db_path, read_only=False, logger_obj=repo.logger) as conn:
            researcher_exists = conn.execute(
                "SELECT 1 FROM UserResearchers WHERE ResearcherID = ? AND IsActive = TRUE",
                [researcher_id],
            ).fetchone()
            if not researcher_exists:
                repo.logger.error(
                    f"Researcher ID {researcher_id} not found or inactive. Cannot save annotation."
                )
                return False

            cap_exists = conn.execute(
                "SELECT 1 FROM UserCAPTaxonomy WHERE MinorCode = ?",
                [cap_minor_code],
            ).fetchone()
            if not cap_exists:
                repo.logger.error(
                    f"CAP Minor Code {cap_minor_code} not found in taxonomy. Cannot save annotation."
                )
                return False

            existing = conn.execute(
                "SELECT AnnotationID FROM UserBillCAP WHERE BillID = ? AND ResearcherID = ?",
                [bill_id, researcher_id],
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE UserBillCAP SET
                        CAPMinorCode = ?,
                        AssignedDate = CURRENT_TIMESTAMP,
                        Confidence = ?,
                        Notes = ?,
                        Source = ?,
                        SubmissionDate = ?
                    WHERE BillID = ? AND ResearcherID = ?
                    """,
                    [
                        cap_minor_code,
                        confidence,
                        notes,
                        source,
                        submission_date,
                        bill_id,
                        researcher_id,
                    ],
                )
                repo.logger.info(
                    f"Updated annotation for bill {bill_id} by researcher {researcher_id}"
                )
            else:
                conn.execute(
                    """
                    INSERT INTO UserBillCAP
                    (BillID, ResearcherID, CAPMinorCode, Confidence, Notes, Source, SubmissionDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        bill_id,
                        researcher_id,
                        cap_minor_code,
                        confidence,
                        notes,
                        source,
                        submission_date,
                    ],
                )
                repo.logger.info(
                    f"Created annotation for bill {bill_id} by researcher {researcher_id}"
                )

            if clear_cache_callback:
                clear_cache_callback()
            return True

    except Exception as exc:
        repo.logger.error(
            f"Error saving annotation for bill {bill_id}: {exc}", exc_info=True
        )
        return False


def delete_annotation(
    repo: Any,
    bill_id: int,
    researcher_id: Optional[int] = None,
    clear_cache_callback: Optional[Callable[[], None]] = None,
) -> bool:
    """Delete one annotation (or all annotations for bill when researcher unset)."""
    try:
        with get_db_connection(repo.db_path, read_only=False, logger_obj=repo.logger) as conn:
            if researcher_id is not None:
                conn.execute(
                    "DELETE FROM UserBillCAP WHERE BillID = ? AND ResearcherID = ?",
                    [bill_id, researcher_id],
                )
                repo.logger.info(
                    f"Deleted annotation for bill {bill_id} by researcher {researcher_id}"
                )
            else:
                conn.execute("DELETE FROM UserBillCAP WHERE BillID = ?", [bill_id])
                repo.logger.info(f"Deleted all annotations for bill {bill_id}")

            if clear_cache_callback:
                clear_cache_callback()
            return True

    except Exception as exc:
        repo.logger.error(
            f"Error deleting annotation for bill {bill_id}: {exc}", exc_info=True
        )
        return False

