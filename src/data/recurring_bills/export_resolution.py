"""Shared helpers for recurring-bill export normalization and enrichment."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast

import pandas as pd

from data.recurring_bills.knesset_docs import (
    classify_recurrence_phrase,
    validate_submission_date,
)


def _is_missing(value: object) -> bool:
    return value is None or bool(pd.isna(cast(Any, value)))


def _to_int(value: object) -> int:
    return int(cast(Any, value))


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
            None
            if _is_missing(row.original_knesset_num)
            else _to_int(row.original_knesset_num),
            None
            if _is_missing(row.original_private_number)
            else _to_int(row.original_private_number),
        )
        for row in bill_ref.itertuples(index=False)
        if not _is_missing(row.original_bill_id)
    }

    def _parse_candidates(raw: object) -> list[dict]:
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

    def _format_reference(bill_id: int) -> str | None:
        knesset_num, private_number = lookup.get(bill_id, (None, None))
        if knesset_num is None or private_number is None:
            return None
        return f"{knesset_num}/{private_number}"

    rows: list[dict[str, object]] = []
    for row in df.itertuples(index=False):
        bill_id = _to_int(getattr(row, "BillID"))
        raw_parent = getattr(row, "original_bill_id", None)
        candidates = _parse_candidates(getattr(row, "reference_candidates", None))

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
                "direct_reference": _format_reference(direct_bill_id)
                if direct_bill_id is not None
                else None,
                "cited_reference_count": len(cited_ids),
                "cited_bill_ids": "; ".join(str(value) for value in cited_ids)
                if cited_ids
                else None,
                "cited_references": "; ".join(cited_refs) if cited_refs else None,
            }
        )

    summary = pd.DataFrame(rows, index=df.index)
    return pd.concat([df, summary], axis=1)


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
