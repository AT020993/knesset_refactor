#!/usr/bin/env python
"""Regenerate Amnon's Excel with recurring-bill classifications.

Joins ``data/Private.Bills.Final.091123.xlsx`` (the file Amnon sent us, with
bill IDs and document URLs) against the ``bill_classifications`` table in the
warehouse, and writes the augmented workbook to
``data/snapshots/Private.Bills.Final.091123_with_classification.xlsx``.

Per Amnon's reply (2026-04-19): he wants the original file back with
``is_original`` and, for recurring bills, ``original_bill_id`` pointing to
the patient-zero ancestor. His workflow is "code the original, apply to all
recurring", so every ``is_original=False`` row needs a codable ancestor row
in the same Excel.

Option C (hybrid) post-pass — keep both perspectives:
- ``is_original`` / ``original_bill_id`` drive Amnon's coding workflow.
  A bill whose factual ancestor is untraceable within this Excel
  (Tal cross-term with no pinpoint ancestor, doc-scan references outside
  Amnon's corpus, or unresolved ``פ/NNN``) is promoted to an EFFECTIVE
  original — ``is_original=True`` and ``original_bill_id=BillID``.
- ``is_recurring_upstream`` preserves the factual reprise signal from
  Tal / doc-scan, so the "recurring-bill noise" analysis Amnon mentioned
  still has a True/False column independent of our effective-original fix.
- ``effective_original_reason`` records WHY a row was promoted.
- Chains are resolved transitively: every non-effective recurring row's
  ``original_bill_id`` points to the deepest ``is_original=True`` ancestor,
  so depth-1 coding lookups always land on a codable bill.

Run:
    source .venv/bin/activate
    PYTHONPATH="./src" python scripts/export_amnon_classified_excel.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _reason_for_tal_self_loop(row: pd.Series) -> str:
    """Pick a specific effective-original reason for Tal self-loops."""
    cat = row.get("tal_category")
    if cat == "cross":
        return "tal_cross_term"
    if cat == "within":
        return "tal_within_term_dup"
    if cat == "self":
        return "tal_self_resubmission"
    return "tal_no_pinpoint_ancestor"


def apply_option_c_post_pass(merged: pd.DataFrame) -> pd.DataFrame:
    """Add is_recurring_upstream / effective_original_reason and resolve chains.

    Algorithm:
    1. Freeze the factual reprise signal (``is_recurring_upstream``) before edits.
    2. For every bill, walk up the raw-chain using (original_bill_id, is_original)
       as they came from the warehouse. The walk stops at:
         - an ancestor inside Amnon's Excel that is raw-original  →  traceable
         - self-loop (Tal cross-term encoding, unresolved doc ref) →  UNTRACEABLE
         - a parent missing from Amnon's Excel                    →  UNTRACEABLE
         - a cycle                                                →  UNTRACEABLE
    3. Traceable bills: ``is_original`` stays False,
       ``original_bill_id`` becomes the deepest raw-original found (chain flattened).
    4. Untraceable recurring bills: flip to effective-original
       (``is_original`` = True, ``original_bill_id`` = self) with a
       ``effective_original_reason`` explaining why.

    Mutates and returns the DataFrame.
    """
    merged["is_recurring_upstream"] = merged["is_original"].eq(False)
    merged["effective_original_reason"] = pd.NA

    excel_ids = set(merged["BillID"].dropna().astype(int))

    # Snapshot RAW state — the chain walker must see the original warehouse
    # labels, not the mutated post-pass values.
    raw_parent = {
        int(b): (None if pd.isna(p) else int(p))
        for b, p in zip(merged["BillID"], merged["original_bill_id"])
    }
    raw_is_orig = {
        int(b): bool(v) if pd.notna(v) else False
        for b, v in zip(merged["BillID"], merged["is_original"])
    }

    def walk_to_original(bid: int) -> int | None:
        """Return the BillID of the deepest raw-original ancestor inside
        Amnon's Excel, or None if untraceable (self-loop, off-corpus, cycle).
        """
        seen: set[int] = set()
        cur = bid
        while cur not in seen:
            seen.add(cur)
            if cur not in excel_ids:
                return None  # ancestor outside Amnon's Excel
            if raw_is_orig.get(cur, False):
                return cur if cur != bid else None
            parent = raw_parent.get(cur)
            if parent is None or parent == cur:
                return None  # self-loop / dead end
            cur = parent
        return None  # cycle

    def reason_for(row: pd.Series) -> str:
        """Pick the most specific reason for an untraceable bill."""
        src = row.get("classification_source")
        orig_id = row.get("original_bill_id")
        if src == "tal_alovitz" and not pd.isna(orig_id) and int(orig_id) == int(row["BillID"]):
            return _reason_for_tal_self_loop(row)
        if src == "doc_based_unresolved_k16_k18":
            return "doc_unresolved"
        return "ancestor_outside_excel"

    rec_rows = merged[merged["is_original"] == False]  # noqa: E712
    for idx in rec_rows.index:
        bid = int(merged.at[idx, "BillID"])
        ancestor = walk_to_original(bid)
        if ancestor is not None:
            # Traceable — keep recurring, point at deepest original (chain flattened)
            merged.at[idx, "original_bill_id"] = ancestor
        else:
            # Untraceable — promote to effective-original
            merged.at[idx, "is_original"] = True
            merged.at[idx, "original_bill_id"] = bid
            merged.at[idx, "effective_original_reason"] = reason_for(merged.loc[idx])

    return merged


def verify(merged: pd.DataFrame) -> dict:
    """Post-pass integrity check. Returns dict of violation counts (all should be 0)."""
    has_cls = merged[merged["classification_source"].notna()].copy()
    orig = has_cls[has_cls["is_original"] == True]  # noqa: E712
    rec = has_cls[has_cls["is_original"] == False]  # noqa: E712

    all_ids = set(merged["BillID"].dropna().astype(int))
    chain = dict(zip(has_cls["BillID"].astype(int), has_cls["is_original"].astype(bool)))

    violations = {
        "originals_not_self_referencing": int(
            ((orig["original_bill_id"].astype("Int64") != orig["BillID"].astype("Int64"))).sum()
        ),
        "recurring_self_referencing": int(
            ((rec["original_bill_id"].astype("Int64") == rec["BillID"].astype("Int64"))).sum()
        ),
        "recurring_ancestor_outside_excel": int(
            (~rec["original_bill_id"].isin(all_ids)).sum()
        ),
        "recurring_ancestor_also_recurring": int(
            rec["original_bill_id"].astype("Int64").map(
                lambda x: False if pd.isna(x) else not chain.get(int(x), True)
            ).sum()
        ),
    }
    return violations


def export(
    *,
    excel_path: Path,
    db_path: Path,
    output_path: Path,
) -> dict:
    xl = pd.read_excel(excel_path)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        cls = con.execute(
            """
            SELECT
                BillID,
                is_original,
                original_bill_id,
                tal_category,
                family_size,
                classification_source
            FROM bill_classifications
            """
        ).df()
        # Pull KnessetNum + PrivateNumber for every bill, so we can look up
        # the ancestor's K# + פ/NNN reference that appears in דברי ההסבר.
        bill_ref = con.execute(
            """
            SELECT BillID AS original_bill_id,
                   KnessetNum AS original_knesset_num,
                   PrivateNumber AS original_private_number
            FROM KNS_Bill
            WHERE PrivateNumber IS NOT NULL
            """
        ).df()
    finally:
        con.close()

    merged = xl.merge(cls, on="BillID", how="left")
    # Enrich with ancestor's K# + PrivateNumber. For self-referential originals
    # (is_original=True and original_bill_id=BillID) this yields the bill's own
    # K# + PN, which is redundant but harmless — the row's own KnessetNum /
    # PrivateNumber columns already carry that info.
    merged = merged.merge(bill_ref, on="original_bill_id", how="left")

    # Raw counts BEFORE post-pass (for before/after reporting)
    raw_originals = int((merged["is_original"] == True).sum())  # noqa: E712
    raw_recurring = int((merged["is_original"] == False).sum())  # noqa: E712

    merged = apply_option_c_post_pass(merged)
    violations = verify(merged)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_excel(output_path, index=False)

    return {
        "input_rows": len(xl),
        "output_rows": len(merged),
        "unmatched": int(merged["classification_source"].isna().sum()),
        "by_source": merged["classification_source"].value_counts(dropna=False).to_dict(),
        "raw_originals": raw_originals,
        "raw_recurring": raw_recurring,
        "effective_originals": int((merged["is_original"] == True).sum()),  # noqa: E712
        "effective_recurring": int((merged["is_original"] == False).sum()),  # noqa: E712
        "is_recurring_upstream_true": int(merged["is_recurring_upstream"].sum()),
        "promoted_to_effective_original": int(
            merged["effective_original_reason"].notna().sum()
        ),
        "by_reason": merged["effective_original_reason"]
        .value_counts(dropna=False)
        .to_dict(),
        "violations": violations,
        "output_path": str(output_path),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--excel", type=Path,
                   default=_REPO_ROOT / "data" / "Private.Bills.Final.091123.xlsx")
    p.add_argument("--db", type=Path,
                   default=_REPO_ROOT / "data" / "warehouse.duckdb")
    p.add_argument("--output", type=Path,
                   default=_REPO_ROOT / "data" / "snapshots"
                                      / "Private.Bills.Final.091123_with_classification.xlsx")
    args = p.parse_args()

    if not args.excel.exists():
        print(f"ERROR: input Excel not found: {args.excel}", file=sys.stderr)
        return 1
    if not args.db.exists():
        print(f"ERROR: warehouse not found: {args.db}", file=sys.stderr)
        return 1

    stats = export(excel_path=args.excel, db_path=args.db, output_path=args.output)

    print(f"Input rows:  {stats['input_rows']}")
    print(f"Output rows: {stats['output_rows']}")
    print(f"Unmatched:   {stats['unmatched']} (no row in bill_classifications)")
    print()
    print("Raw (from Tal + doc-scan):")
    print(f"  Originals: {stats['raw_originals']}")
    print(f"  Recurring: {stats['raw_recurring']}")
    print()
    print("Effective (after Option C post-pass — drives Amnon's coding workflow):")
    print(f"  Originals: {stats['effective_originals']}")
    print(f"  Recurring: {stats['effective_recurring']}")
    print()
    print(f"is_recurring_upstream=True (factual reprise signal): {stats['is_recurring_upstream_true']}")
    print(f"Promoted to effective-original: {stats['promoted_to_effective_original']}")
    print()
    print("By classification_source:")
    for src, count in stats["by_source"].items():
        print(f"  {src}: {count}")
    print()
    print("effective_original_reason:")
    for reason, count in stats["by_reason"].items():
        print(f"  {reason}: {count}")
    print()
    print("Integrity violations (all must be 0):")
    all_zero = True
    for k, v in stats["violations"].items():
        marker = "OK" if v == 0 else "FAIL"
        print(f"  [{marker}] {k}: {v}")
        if v != 0:
            all_zero = False
    print()
    print(f"Wrote: {stats['output_path']}")
    if not all_zero:
        print("WARNING: integrity violations detected — inspect before sending", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
