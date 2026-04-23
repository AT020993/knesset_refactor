"""Shared helpers for recurring-bill export normalization and enrichment."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd


def ensure_columns(df: pd.DataFrame, defaults: dict[str, object]) -> pd.DataFrame:
    """Ensure optional export columns exist so older tables remain readable."""
    for column, default in defaults.items():
        if column not in df.columns:
            df[column] = default
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
        int(bill_id): (None if pd.isna(parent_id) else int(parent_id))
        for bill_id, parent_id in zip(df["BillID"], df["original_bill_id"])
    }
    raw_is_original = {
        int(bill_id): bool(value) if pd.notna(value) else False
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
        bill_id = int(df.at[idx, "BillID"])
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
        bill_ref[["original_bill_id", "original_knesset_num", "original_private_number"]],
        on="original_bill_id",
        how="left",
    )


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
            (originals["original_bill_id"].astype("Int64") != originals["BillID"].astype("Int64")).sum()
        ),
        "recurring_self_referencing": int(
            (recurring["original_bill_id"].astype("Int64") == recurring["BillID"].astype("Int64")).sum()
        ),
        outside_label: int((~recurring["original_bill_id"].isin(all_ids)).sum()),
        "recurring_ancestor_also_recurring": int(
            recurring["original_bill_id"].astype("Int64").map(
                lambda value: False if pd.isna(value) else not chain.get(int(value), True)
            ).sum()
        ),
    }


def classify_recurrence_type(matched_phrase: object) -> str | None:
    if matched_phrase is None or (isinstance(matched_phrase, float) and pd.isna(matched_phrase)):
        return None

    phrase = str(matched_phrase)
    if "דומה" in phrase or "המשך" in phrase:
        return "similar"
    if (
        "זהה" in phrase
        or "חוזר" in phrase
        or phrase.startswith("הונחה")
        or phrase.startswith("הוגש")
        or phrase.startswith("ומספרה")
    ):
        return "identical"
    return None


def strip_timezone_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert timezone-aware datetime columns to naive datetimes for Excel."""
    for column in df.select_dtypes(include=["datetimetz"]).columns:
        df[column] = df[column].dt.tz_localize(None)
    return df
