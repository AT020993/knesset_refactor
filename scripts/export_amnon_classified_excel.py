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
sys.path.insert(0, str(_REPO_ROOT / "src"))

from data.recurring_bills.export_resolution import (  # noqa: E402
    apply_option_c_post_pass,
    enrich_from_final_original_bill_id,
    ensure_columns,
    sanitize_submission_dates,
    strip_timezone_columns,
    suppress_source_metadata_reference_resolutions,
    verify_effective_originals,
)


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


def _reason_for_excel_row(row: pd.Series) -> str:
    src = row.get("classification_source")
    orig_id = row.get("original_bill_id")
    if (
        src == "tal_alovitz"
        and not pd.isna(orig_id)
        and int(orig_id) == int(row["BillID"])
    ):
        return _reason_for_tal_self_loop(row)
    if bool(row.get("ambiguous_reference_resolution", False)):
        return "ambiguous_doc_reference"
    if bool(row.get("suspicious_self_resolution", False)):
        return "suspicious_self_reference_only"
    if src == "doc_based_unresolved_k16_k18":
        return "doc_unresolved"
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
        cls = con.execute("SELECT * FROM bill_classifications").df()
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
            "tal_category": None,
            "family_size": None,
            "classification_source": None,
            "matched_phrase": None,
            "method": None,
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
    merged = xl.merge(cls, on="BillID", how="left")

    # Raw counts BEFORE post-pass (for before/after reporting)
    raw_originals = int((merged["is_original"] == True).sum())  # noqa: E712
    raw_recurring = int((merged["is_original"] == False).sum())  # noqa: E712

    merged = suppress_source_metadata_reference_resolutions(merged)
    merged = apply_option_c_post_pass(merged, reason_for=_reason_for_excel_row)
    merged = enrich_from_final_original_bill_id(merged, bill_ref)
    merged = sanitize_submission_dates(merged)
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
        "by_source": merged["classification_source"]
        .value_counts(dropna=False)
        .to_dict(),
        "raw_originals": raw_originals,
        "raw_recurring": raw_recurring,
        "effective_originals": int(merged["is_original"].eq(True).sum()),
        "effective_recurring": int(merged["is_original"].eq(False).sum()),
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
    p.add_argument(
        "--excel",
        type=Path,
        default=_REPO_ROOT / "data" / "Private.Bills.Final.091123.xlsx",
    )
    p.add_argument("--db", type=Path, default=_REPO_ROOT / "data" / "warehouse.duckdb")
    p.add_argument(
        "--output",
        type=Path,
        default=_REPO_ROOT
        / "data"
        / "snapshots"
        / "Private.Bills.Final.091123_with_classification.xlsx",
    )
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
    print(
        f"is_recurring_upstream=True (factual reprise signal): {stats['is_recurring_upstream_true']}"
    )
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
        print(
            "WARNING: integrity violations detected — inspect before sending",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
