#!/usr/bin/env python
"""Export the COMPLETE K1-K25 private-bill classification — warehouse-backed.

Per Prof. Amnon's 2026-04-21 follow-up: he disowns his original Excel
(wasn't involved in collecting it, doubts reliability) and wants a
complete dataset backed by the Knesset OData warehouse (``KNS_Bill``)
rather than his file.

This script produces ``data/snapshots/All_Private_Bills_K1_K25_classified.xlsx``
covering EVERY private bill in the warehouse (51,673 rows across K1-K25)
with:

- Bill identity: BillID, KnessetNum, Name, PrivateNumber, SubTypeDesc
- Document link: doc_url (the fs.knesset.gov.il link our classifier read)
- Classification: is_original, original_bill_id, original_knesset_num,
  original_private_number
- Method provenance: method, matched_phrase, classification_source
- Option-C presentation: is_recurring_upstream, effective_original_reason

Option-C post-pass keeps the coding workflow intact: every recurring
bill's ``original_bill_id`` is transitively flattened to the deepest
raw-original ancestor inside the 51,673-row universe. The only bills
promoted to "effective original" are those with no resolvable ancestor
via our regex (``doc_fetch_failed``, ``no_doc_url``, or truly never
cited a predecessor).

Run:
    source .venv/bin/activate
    PYTHONPATH="./src" python scripts/export_all_bills_classified.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from data.recurring_bills.export_resolution import (  # noqa: E402
    apply_option_c_post_pass,
    classify_recurrence_type,
    enrich_from_final_original_bill_id,
    ensure_columns,
    strip_timezone_columns,
    verify_effective_originals,
)


def _reason_for_doc_row(row: pd.Series) -> str:
    method = row.get("method")
    if method == "doc_fetch_failed":
        return "doc_fetch_failed"
    if method == "no_doc_url":
        return "no_doc_url"
    if bool(row.get("ambiguous_reference_resolution", False)):
        return "ambiguous_doc_reference"
    if bool(row.get("suspicious_self_resolution", False)):
        return "suspicious_self_reference_only"
    orig_id = row.get("original_bill_id")
    if not pd.isna(orig_id) and int(orig_id) == int(row["BillID"]):
        return "doc_no_ancestor_found"
    return "ancestor_outside_universe"


def export(*, db_path: Path, output_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        cls = con.execute("SELECT * FROM bill_classifications_doc_full").df()
        bill_meta = con.execute(
            """
            SELECT
                BillID,
                SubTypeDesc
            FROM KNS_Bill
            """
        ).df()
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

    cls = ensure_columns(
        cls,
        {
            "reference_candidates": "[]",
            "reference_candidate_count": 0,
            "reference_resolution_reason": None,
            "reference_resolution_confidence": None,
            "multiple_references_detected": False,
            "submission_date": None,
            "suspicious_self_resolution": False,
            "ambiguous_reference_resolution": False,
            "ambiguous_reference_reason": None,
        },
    )
    df = cls.merge(bill_meta, on="BillID", how="left").sort_values(["KnessetNum", "BillID"], kind="stable")

    raw_originals = int((df["is_original"] == True).sum())  # noqa: E712
    raw_recurring = int((df["is_original"] == False).sum())  # noqa: E712

    df["recurrence_type"] = df["matched_phrase"].apply(classify_recurrence_type)

    df = apply_option_c_post_pass(df, reason_for=_reason_for_doc_row)
    df = enrich_from_final_original_bill_id(df, bill_ref)
    violations = verify_effective_originals(
        df,
        outside_label="recurring_ancestor_outside_universe",
    )

    # Column ordering — most useful columns first for Amnon
    col_order = [
        "BillID", "KnessetNum", "Name", "PrivateNumber", "SubTypeDesc",
        "doc_url",
        "is_original", "original_bill_id",
        "original_knesset_num", "original_private_number",
        "is_recurring_upstream", "recurrence_type", "effective_original_reason",
        "method", "matched_phrase", "classification_source",
        "reference_candidate_count", "reference_resolution_reason",
        "reference_resolution_confidence", "multiple_references_detected",
        "suspicious_self_resolution", "ambiguous_reference_resolution",
        "ambiguous_reference_reason", "submission_date", "reference_candidates",
    ]
    df = df[col_order]
    df = strip_timezone_columns(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)

    return {
        "total_rows": len(df),
        "raw_originals": raw_originals,
        "raw_recurring": raw_recurring,
        "effective_originals": int((df["is_original"] == True).sum()),  # noqa: E712
        "effective_recurring": int((df["is_original"] == False).sum()),  # noqa: E712
        "is_recurring_upstream_true": int(df["is_recurring_upstream"].sum()),
        "promoted_to_effective_original": int(df["effective_original_reason"].notna().sum()),
        "by_method": df["method"].value_counts(dropna=False).to_dict(),
        "by_recurrence_type": df["recurrence_type"].value_counts(dropna=False).to_dict(),
        "by_reason": df["effective_original_reason"].value_counts(dropna=False).to_dict(),
        "per_knesset": df.groupby("KnessetNum").agg(
            total=("BillID", "count"),
            recurring=("is_recurring_upstream", "sum"),
        ).reset_index().to_dict("records"),
        "violations": violations,
        "output_path": str(output_path),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path,
                   default=_REPO_ROOT / "data" / "warehouse.duckdb")
    p.add_argument("--output", type=Path,
                   default=_REPO_ROOT / "data" / "snapshots"
                                      / "All_Private_Bills_K1_K25_classified.xlsx")
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: warehouse not found: {args.db}", file=sys.stderr)
        return 1

    stats = export(db_path=args.db, output_path=args.output)

    print(f"Total rows (all K1-K25 private bills): {stats['total_rows']}")
    print()
    print("Raw (from our doc-scan):")
    print(f"  Originals: {stats['raw_originals']}")
    print(f"  Recurring: {stats['raw_recurring']}  ({100*stats['raw_recurring']/stats['total_rows']:.2f}%)")
    print()
    print("Effective (after Option-C post-pass):")
    print(f"  Originals: {stats['effective_originals']}")
    print(f"  Recurring: {stats['effective_recurring']}")
    print(f"  Promoted to effective-original: {stats['promoted_to_effective_original']}")
    print()
    print("Per-Knesset recurrence rates:")
    for r in stats["per_knesset"]:
        pct = 100 * r["recurring"] / r["total"] if r["total"] else 0
        print(f"  K{int(r['KnessetNum']):<3}  {r['total']:>6}  recurring={r['recurring']:>5}  ({pct:5.2f}%)")
    print()
    print("Recurrence type (identical vs similar):")
    for t, n in stats["by_recurrence_type"].items():
        print(f"  {t}: {n}")
    print()
    print("Method distribution:")
    for m, n in sorted(stats["by_method"].items(), key=lambda kv: -kv[1] if kv[1] is not pd.NA else 0):
        print(f"  {m}: {n}")
    print()
    print("Effective-original reasons:")
    for r, n in stats["by_reason"].items():
        print(f"  {r}: {n}")
    print()
    print("Integrity (all must be 0):")
    all_ok = True
    for k, v in stats["violations"].items():
        mark = "OK" if v == 0 else "FAIL"
        print(f"  [{mark}] {k}: {v}")
        if v != 0:
            all_ok = False
    print()
    print(f"Wrote: {stats['output_path']}")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
