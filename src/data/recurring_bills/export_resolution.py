"""Shared helpers for recurring-bill export normalization and enrichment."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any, cast

import pandas as pd

from data.recurring_bills.knesset_docs import (
    classify_recurrence_phrase,
    validate_submission_date,
)

_SOURCE_METADATA_NAME_METHODS = {
    "contextual_knesset_name_match",
    "same_knesset_name_fallback",
    "prior_knesset_name_fallback",
}
_SOURCE_METADATA_PRIVATE_NUMBER_METHODS = {
    "same_knesset_private_number_fallback",
    "prior_knesset_private_number_fallback",
}
SOURCE_METADATA_RESOLUTION_METHODS = (
    _SOURCE_METADATA_NAME_METHODS | _SOURCE_METADATA_PRIVATE_NUMBER_METHODS
)
LOW_CONFIDENCE_THRESHOLD = 0.75


def _is_missing(value: object) -> bool:
    return value is None or bool(pd.isna(cast(Any, value)))


def _to_int(value: object) -> int:
    return int(cast(Any, value))


def _truthy(value: object) -> bool:
    return False if _is_missing(value) else bool(value)


def _parse_reference_candidates(raw: object) -> list[dict]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if _is_missing(raw):
        return []
    if not isinstance(raw, str):
        return []
    text = raw.strip()
    if not text or text.lower() == "nan":
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return (
        [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, list)
        else []
    )


def _candidate_bill_id(candidate: dict) -> int | None:
    value = candidate.get("resolved_bill_id")
    if _is_missing(value):
        return None
    try:
        return _to_int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: object) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _source_doc_id(source_url: object) -> int | None:
    if _is_missing(source_url):
        return None
    text = str(source_url)
    match = re.search(r"_lst_(\d+)\.(?:docx?|pdf)(?:\?|$)", text)
    return int(match.group(1)) if match else None


def _status_for_resolution(
    *,
    reason: object,
    resolved_bill_id: object = None,
    private_number: object = None,
    target_url: object = None,
    suspicious_self: object = False,
    ambiguous: object = False,
) -> str:
    reason_text = None if _is_missing(reason) else str(reason)
    if _truthy(suspicious_self) or reason_text == "suspicious_self_reference":
        return "unresolved_suspicious_self_reference"
    if _truthy(ambiguous) or reason_text == "ambiguous_primary_reference_candidates":
        return "unresolved_ambiguous"
    if reason_text in _SOURCE_METADATA_NAME_METHODS:
        return "unresolved_no_link_or_number"
    if reason_text in _SOURCE_METADATA_PRIVATE_NUMBER_METHODS:
        return "unresolved_missing_target_knesset"
    if reason_text in {
        "unresolved_no_link_or_number",
        "no_reference_candidates_in_recurrence_context",
    }:
        return "unresolved_no_link_or_number"
    if reason_text == "unresolved_missing_target_knesset":
        return "unresolved_missing_target_knesset"
    if reason_text in {
        "explicit_reference_unresolved",
        "no_matching_bill_for_reference",
        "no_resolved_reference_candidates",
    }:
        return "unresolved_no_matching_bill"
    if not _is_missing(resolved_bill_id):
        return "resolved"
    if _is_missing(target_url) and _is_missing(private_number):
        return "unresolved_no_link_or_number"
    return "unresolved_no_matching_bill"


def _warning_for_status(status: str, confidence: float | None) -> str | None:
    warnings: list[str] = []
    if status == "unresolved_no_link_or_number":
        warnings.append(
            "No target URL or reliable target bill number; target left unresolved."
        )
    elif status == "unresolved_missing_target_knesset":
        warnings.append(
            "Target bill number appears without target Knesset; source Knesset was not inferred."
        )
    elif status == "unresolved_no_matching_bill":
        warnings.append("Extracted reference did not resolve to a warehouse bill.")
    elif status == "unresolved_ambiguous":
        warnings.append(
            "Multiple equally strong target candidates; no final target selected."
        )
    elif status == "unresolved_suspicious_self_reference":
        warnings.append(
            "Reference resolves only to the source bill; suppressed as suspicious."
        )

    if confidence is not None and confidence < LOW_CONFIDENCE_THRESHOLD:
        warnings.append(f"Low extraction/resolution confidence ({confidence:.2f}).")

    return " ".join(warnings) if warnings else None


def ensure_columns(df: pd.DataFrame, defaults: dict[str, Any]) -> pd.DataFrame:
    """Ensure optional export columns exist so older tables remain readable."""
    for column, default in defaults.items():
        if column not in df.columns:
            df[column] = cast(Any, default)
    return df


def apply_option_c_post_pass(
    df: pd.DataFrame,
    *,
    reason_for: Callable[[pd.Series], str],
) -> pd.DataFrame:
    """Flatten raw recurrence chains to a single effective ancestor.

    Recurrent rows with an untraceable ancestor are promoted to effective
    originals. ``reason_for`` is called only for those promoted rows.
    """
    df["is_recurring_upstream"] = df["is_original"].eq(False)
    df["effective_original_reason"] = pd.NA

    universe_ids = set(df["BillID"].dropna().astype(int))
    raw_parent = {
        _to_int(bill_id): (None if _is_missing(parent_id) else _to_int(parent_id))
        for bill_id, parent_id in zip(df["BillID"], df["original_bill_id"])
    }
    raw_is_original = {
        _to_int(bill_id): bool(value) if not _is_missing(value) else False
        for bill_id, value in zip(df["BillID"], df["is_original"])
    }

    def walk_to_original(bill_id: int) -> int | None:
        seen: set[int] = set()
        current = bill_id
        while current not in seen:
            seen.add(current)
            if current not in universe_ids:
                return None
            if raw_is_original.get(current, False):
                return current if current != bill_id else None
            parent = raw_parent.get(current)
            if parent is None or parent == current:
                return None
            current = parent
        return None

    recurring_rows = df.index[df["is_original"] == False]  # noqa: E712
    for idx in recurring_rows:
        bill_id = _to_int(df.at[idx, "BillID"])
        ancestor = walk_to_original(bill_id)
        if ancestor is not None:
            df.at[idx, "original_bill_id"] = ancestor
            continue

        df.at[idx, "is_original"] = True
        df.at[idx, "original_bill_id"] = bill_id
        df.at[idx, "effective_original_reason"] = reason_for(df.loc[idx])

    return df


def enrich_from_final_original_bill_id(
    df: pd.DataFrame,
    bill_ref: pd.DataFrame,
) -> pd.DataFrame:
    """Join ancestor metadata from the final, post-pass ``original_bill_id``."""
    for column in ("original_knesset_num", "original_private_number"):
        if column in df.columns:
            df = df.drop(columns=[column])

    return df.merge(
        bill_ref[
            ["original_bill_id", "original_knesset_num", "original_private_number"]
        ],
        on="original_bill_id",
        how="left",
    )


def add_reference_summary_columns(
    df: pd.DataFrame,
    bill_ref: pd.DataFrame,
) -> pd.DataFrame:
    """Add readable direct/all-reference columns from raw classification evidence."""
    lookup = {
        _to_int(row.original_bill_id): (
            (
                None
                if _is_missing(row.original_knesset_num)
                else _to_int(row.original_knesset_num)
            ),
            (
                None
                if _is_missing(row.original_private_number)
                else _to_int(row.original_private_number)
            ),
        )
        for row in bill_ref.itertuples(index=False)
        if not _is_missing(row.original_bill_id)
    }

    def _format_reference(bill_id: int) -> str | None:
        knesset_num, private_number = lookup.get(bill_id, (None, None))
        if knesset_num is None or private_number is None:
            return None
        return f"{knesset_num}/{private_number}"

    rows: list[dict[str, object]] = []
    for row in df.itertuples(index=False):
        bill_id = _to_int(getattr(row, "BillID"))
        raw_parent = getattr(row, "original_bill_id", None)
        candidates = _parse_reference_candidates(
            getattr(row, "reference_candidates", None)
        )

        resolved_candidates = []
        seen_ids: set[int] = set()
        for candidate in candidates:
            if bool(candidate.get("suspicious_self_resolution", False)):
                continue
            resolved_bill_id = _candidate_bill_id(candidate)
            if resolved_bill_id is None or resolved_bill_id in seen_ids:
                continue
            seen_ids.add(resolved_bill_id)
            resolved_candidates.append((resolved_bill_id, candidate))

        selected_ids = [
            resolved_bill_id
            for resolved_bill_id, candidate in resolved_candidates
            if bool(candidate.get("selected", False))
        ]
        direct_bill_id = selected_ids[0] if len(selected_ids) == 1 else None

        if direct_bill_id is None and not _is_missing(raw_parent):
            raw_parent_id = _to_int(raw_parent)
            if raw_parent_id != bill_id:
                direct_bill_id = raw_parent_id

        if direct_bill_id is None and len(resolved_candidates) == 1:
            direct_bill_id = resolved_candidates[0][0]

        direct_knesset = direct_private = None
        if direct_bill_id is not None:
            direct_knesset, direct_private = lookup.get(direct_bill_id, (None, None))

        cited_ids = [resolved_bill_id for resolved_bill_id, _ in resolved_candidates]
        cited_refs = [
            formatted
            for formatted in (
                _format_reference(resolved_bill_id) for resolved_bill_id in cited_ids
            )
            if formatted is not None
        ]

        rows.append(
            {
                "direct_reference_bill_id": direct_bill_id,
                "direct_reference_knesset_num": direct_knesset,
                "direct_reference_private_number": direct_private,
                "direct_reference": (
                    _format_reference(direct_bill_id)
                    if direct_bill_id is not None
                    else None
                ),
                "cited_reference_count": len(cited_ids),
                "cited_bill_ids": (
                    "; ".join(str(value) for value in cited_ids) if cited_ids else None
                ),
                "cited_references": "; ".join(cited_refs) if cited_refs else None,
            }
        )

    summary = pd.DataFrame(rows, index=df.index)
    return pd.concat([df, summary], axis=1)


def suppress_source_metadata_reference_resolutions(df: pd.DataFrame) -> pd.DataFrame:
    """Undo stale source-metadata target resolutions before export."""
    if "reference_resolution_reason" not in df.columns:
        return df

    df = df.copy()
    unsafe_rows = df["reference_resolution_reason"].isin(
        SOURCE_METADATA_RESOLUTION_METHODS
    )
    for idx in df.index:
        row_reason = df.at[idx, "reference_resolution_reason"]
        row_is_unsafe = bool(unsafe_rows.loc[idx])
        if row_is_unsafe:
            status = _status_for_resolution(reason=row_reason)
            df.at[idx, "original_bill_id"] = df.at[idx, "BillID"]
            df.at[idx, "method"] = "doc_pattern_unresolved"
            df.at[idx, "reference_resolution_reason"] = status
            df.at[idx, "reference_resolution_confidence"] = pd.NA

        candidates = _parse_reference_candidates(df.at[idx, "reference_candidates"])
        candidates_changed = False
        for candidate in candidates:
            candidate_reason = candidate.get("reference_resolution_reason")
            if candidate_reason in SOURCE_METADATA_RESOLUTION_METHODS or (
                row_is_unsafe and bool(candidate.get("selected", False))
            ):
                candidate_status = _status_for_resolution(reason=candidate_reason)
                candidate["resolved_bill_id"] = None
                candidate["reference_resolution_reason"] = candidate_status
                candidate["reference_resolution_confidence"] = None
                candidate["priority"] = 0
                candidate["selected"] = False
                candidate["selection_rank"] = None
                candidate["target_resolution_status"] = candidate_status
                candidate["source_metadata_resolution_suppressed"] = True
                candidates_changed = True
        if candidates_changed:
            df.at[idx, "reference_candidates"] = json.dumps(
                candidates,
                ensure_ascii=False,
                sort_keys=True,
            )

    return df


def add_source_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add source/target audit aliases with names that separate source from target."""
    df = df.copy()
    df["source_knesset"] = df["KnessetNum"]
    df["source_bill_id"] = df["BillID"]
    df["source_doc_id"] = (
        df["doc_url"].map(_source_doc_id) if "doc_url" in df.columns else None
    )
    df["source_url"] = df["doc_url"] if "doc_url" in df.columns else None
    df["explicit_relation_type"] = df["matched_phrase"].apply(classify_recurrence_type)
    df["final_relation_type"] = df["explicit_relation_type"]

    statuses: list[str] = []
    warnings: list[str | None] = []
    for row in df.itertuples(index=False):
        confidence = _safe_float(getattr(row, "reference_resolution_confidence", None))
        status = _status_for_resolution(
            reason=getattr(row, "reference_resolution_reason", None),
            resolved_bill_id=(
                getattr(row, "direct_reference_bill_id", None)
                if hasattr(row, "direct_reference_bill_id")
                else None
            ),
            suspicious_self=getattr(row, "suspicious_self_resolution", False),
            ambiguous=getattr(row, "ambiguous_reference_resolution", False),
        )
        if getattr(row, "matched_phrase", None) is None and getattr(
            row, "method", None
        ) in {
            "doc_no_pattern",
            "no_doc_url",
            "doc_fetch_failed",
        }:
            status = "not_applicable_no_recurring_phrase"
        statuses.append(status)
        warnings.append(_warning_for_status(status, confidence))

    df["target_resolution_status"] = statuses
    df["target_resolution_method"] = df["reference_resolution_reason"]
    df["target_resolution_confidence"] = df["reference_resolution_confidence"]
    df["warnings"] = warnings
    df["notes"] = df["target_resolution_status"].map(
        lambda status: (
            "Identity/similarity is based on explicit Hebrew phrases only; "
            "no normalized legal amendment text comparison was performed."
            if status != "not_applicable_no_recurring_phrase"
            else None
        )
    )
    return df


def build_reference_resolution_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Expand raw reference evidence to one row per reference/phrase."""
    rows: list[dict[str, object]] = []
    for row in df.itertuples(index=False):
        candidates = _parse_reference_candidates(
            getattr(row, "reference_candidates", None)
        )
        if not candidates and not _is_missing(getattr(row, "matched_phrase", None)):
            candidates = [
                {
                    "phrase_text": getattr(row, "matched_phrase", None),
                    "recurrence_type": getattr(row, "explicit_relation_type", None),
                    "context": None,
                    "reference_text": None,
                    "private_number": None,
                    "explicit_knesset": None,
                    "contextual_knesset": None,
                    "referenced_knesset": None,
                    "resolved_bill_id": None,
                    "reference_resolution_reason": getattr(
                        row,
                        "reference_resolution_reason",
                        None,
                    ),
                    "reference_resolution_confidence": getattr(
                        row,
                        "reference_resolution_confidence",
                        None,
                    ),
                }
            ]

        for reference_index, candidate in enumerate(candidates, start=1):
            confidence = _safe_float(candidate.get("reference_resolution_confidence"))
            status = _status_for_resolution(
                reason=candidate.get("reference_resolution_reason"),
                resolved_bill_id=candidate.get("resolved_bill_id"),
                private_number=candidate.get("private_number"),
                target_url=candidate.get("target_url"),
                suspicious_self=candidate.get("suspicious_self_resolution", False),
                ambiguous=(
                    bool(candidate.get("tied_for_best", False))
                    and bool(getattr(row, "ambiguous_reference_resolution", False))
                ),
            )
            if status != "resolved":
                confidence = None
            relation_type = (
                candidate.get("recurrence_type")
                or getattr(row, "explicit_relation_type", None)
                or "unknown"
            )
            evidence = (
                candidate.get("context")
                or candidate.get("reference_text")
                or candidate.get("phrase_text")
                or getattr(row, "matched_phrase", None)
            )
            note = None
            if candidate.get("source_metadata_resolution_suppressed"):
                note = "Suppressed stale source-metadata fallback resolution."
            rows.append(
                {
                    "source_knesset": getattr(row, "source_knesset", None),
                    "source_bill_id": getattr(row, "source_bill_id", None),
                    "source_doc_id": getattr(row, "source_doc_id", None),
                    "source_url": getattr(row, "source_url", None),
                    "reference_index": reference_index,
                    "reference_text_raw": evidence,
                    "target_knesset_extracted": (
                        candidate.get("explicit_knesset")
                        or candidate.get("contextual_knesset")
                        or candidate.get("referenced_knesset")
                    ),
                    "target_bill_number_extracted": candidate.get("private_number"),
                    "target_url_extracted": candidate.get("target_url"),
                    "target_resolution_status": status,
                    "target_resolution_method": candidate.get(
                        "reference_resolution_reason"
                    ),
                    "target_resolution_confidence": confidence,
                    "explicit_relation_type": relation_type,
                    "final_relation_type": relation_type,
                    "relation_evidence_text": evidence,
                    "notes": note,
                    "warnings": _warning_for_status(status, confidence),
                    "resolved_target_bill_id": candidate.get("resolved_bill_id"),
                    "source_metadata_resolution_suppressed": bool(
                        candidate.get("source_metadata_resolution_suppressed", False)
                    ),
                }
            )

    return pd.DataFrame(rows)


def verify_effective_originals(
    df: pd.DataFrame,
    *,
    outside_label: str,
    classification_mask: pd.Series | None = None,
) -> dict:
    """Return post-pass integrity counts. All values should be 0."""
    if classification_mask is None:
        classification_mask = pd.Series(True, index=df.index)

    scoped = df[classification_mask].copy()
    originals = scoped[scoped["is_original"] == True]  # noqa: E712
    recurring = scoped[scoped["is_original"] == False]  # noqa: E712

    all_ids = set(df["BillID"].dropna().astype(int))
    chain = dict(zip(scoped["BillID"].astype(int), scoped["is_original"].astype(bool)))

    return {
        "originals_not_self_referencing": int(
            (
                originals["original_bill_id"].astype("Int64")
                != originals["BillID"].astype("Int64")
            ).sum()
        ),
        "recurring_self_referencing": int(
            (
                recurring["original_bill_id"].astype("Int64")
                == recurring["BillID"].astype("Int64")
            ).sum()
        ),
        outside_label: int((~recurring["original_bill_id"].isin(all_ids)).sum()),
        "recurring_ancestor_also_recurring": int(
            recurring["original_bill_id"]
            .astype("Int64")
            .map(
                lambda value: (
                    False if pd.isna(value) else not chain.get(int(value), True)
                )
            )
            .sum()
        ),
    }


def classify_recurrence_type(matched_phrase: object) -> str | None:
    if _is_missing(matched_phrase):
        return None
    return classify_recurrence_phrase(matched_phrase)


def strip_timezone_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert timezone-aware datetime columns to naive datetimes for Excel."""
    for column in df.select_dtypes(include=["datetimetz"]).columns:
        df[column] = df[column].dt.tz_localize(None)
    return df


def sanitize_submission_dates(
    df: pd.DataFrame,
    *,
    knesset_col: str = "KnessetNum",
) -> pd.DataFrame:
    """Drop implausible submission dates before export."""
    if "submission_date" not in df.columns:
        return df

    def _sanitize_row(row: pd.Series) -> str | None:
        current_knesset = row.get(knesset_col)
        if _is_missing(current_knesset):
            current_knesset = None
        else:
            current_knesset = _to_int(current_knesset)
        return validate_submission_date(
            row.get("submission_date"),
            current_knesset=current_knesset,
        )

    df["submission_date"] = cast(Any, df.apply)(_sanitize_row, axis=1)
    return df
