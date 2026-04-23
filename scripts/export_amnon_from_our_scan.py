#!/usr/bin/env python
"""Regenerate Amnon's Excel using OUR independent doc-based scan.

Joins ``data/Private.Bills.Final.091123.xlsx`` against the
``bill_classifications_doc_full`` table — our own regex-based scan of
every K1-K25 private bill's explanatory notes (via ``KNS_DocumentBill``),
independent of Tal Alovitz's ``pmb.teca-it.com`` API.

Why the separate export: per Prof. Amnon's 2026-04-20 reply ("I prefer
the data be *ours* so we can compare if needed"), this deliverable is
sourced entirely from fs.knesset.gov.il + our Hebrew-phrase regex, with
no Tal dependency. The earlier export (``export_amnon_classified_excel.py``)
blended Tal's K19-K25 labels with our K16-K18 doc-scan; this one uses our
scan across all Knessets uniformly.

Option C (hybrid) post-pass — identical semantics to the Tal-based export:
- ``is_original`` / ``original_bill_id`` drive Amnon's coding workflow.
  A bill whose factual ancestor is untraceable within this Excel (self-
  loop with no resolvable pinpoint, off-corpus ancestor, doc-fetch
  failure, or no doc URL at all) is promoted to an EFFECTIVE original —
  ``is_original=True``, ``original_bill_id=BillID``.
- ``is_recurring_upstream`` preserves the factual reprise signal from
  our doc-scan.
- ``effective_original_reason`` records WHY a row was promoted:
  ``doc_fetch_failed``, ``no_doc_url``, ``doc_no_ancestor_found``,
  ``ancestor_outside_excel``.
- Chains are resolved transitively so depth-1 coding lookups always land
  on a codable bill.

Run:
    source .venv/bin/activate
    PYTHONPATH="./src" python scripts/export_amnon_from_our_scan.py
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
    orig_id = row.get("original_bill_id")
    if not pd.isna(orig_id) and int(orig_id) == int(row["BillID"]):
        return "doc_no_ancestor_found"
    return "ancestor_outside_excel"


def export(
    *,
    excel_path: Path,
    db_path: Path,
    output_path: Path,
) -> dict:
    xl = pd.read_excel(excel_path)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        cls = con.execute("SELECT * FROM bill_classifications_doc_full").df()
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
            "matched_phrase": None,
            "method": None,
            "reference_candidates": "[]",
            "reference_candidate_count": 0,
            "reference_resolution_reason": None,
            "reference_resolution_confidence": None,
            "multiple_references_detected": False,
            "submission_date": None,
            "suspicious_self_resolution": False,
            "classification_source": None,
        },
    )
    merged = xl.merge(cls, on="BillID", how="left")

    raw_originals = int((merged["is_original"] == True).sum())  # noqa: E712
    raw_recurring = int((merged["is_original"] == False).sum())  # noqa: E712

    merged = apply_option_c_post_pass(merged, reason_for=_reason_for_doc_row)
    merged = enrich_from_final_original_bill_id(merged, bill_ref)
    violations = verify_effective_originals(
        merged,
        outside_label="recurring_ancestor_outside_excel",
        classification_mask=merged["classification_source"].notna(),
    )
    merged = strip_timezone_columns(merged)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_excel(output_path, index=False)

    return {
        "input_rows": len(xl),
        "output_rows": len(merged),
        "unmatched": int(merged["classification_source"].isna().sum()),
        "by_source": merged["classification_source"].value_counts(dropna=False).to_dict(),
        "by_method": merged["method"].value_counts(dropna=False).to_dict(),
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
                                      / "Private.Bills.Final.091123_with_our_classification.xlsx")
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
    print(f"Unmatched:   {stats['unmatched']} (no row in bill_classifications_doc_full)")
    print()
    print("Raw (from our doc-scan):")
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
    print("By scan method:")
    for m, count in stats["by_method"].items():
        print(f"  {m}: {count}")
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
