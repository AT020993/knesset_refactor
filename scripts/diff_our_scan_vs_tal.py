#!/usr/bin/env python
"""Compare our independent doc-based scan against Tal Alovitz's classifications.

Run AFTER ``scripts/scan_all_bills.py`` finishes. Emits:

- Per-Knesset agreement rates (is_original match)
- Per-Knesset disagreement samples (for manual audit)
- Data/snapshots/our_scan_vs_tal_diff.xlsx — full bill-level diff table

Scope: only bills where BOTH Tal (``bill_classifications``) and our scan
(``bill_classifications_doc_full``) have a classification. For K1-K18
Tal has no coverage, so those rows are reported separately as
"our scan only".
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent


def compare(db_path: Path, output_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Full outer join of the two tables on BillID
        diff = con.execute(
            """
            SELECT
                COALESCE(t.BillID, o.BillID)                AS BillID,
                COALESCE(t.KnessetNum, o.KnessetNum)        AS KnessetNum,
                COALESCE(o.Name, t.Name)                    AS Name,
                t.is_original                                AS tal_is_original,
                t.original_bill_id                           AS tal_original_bill_id,
                t.tal_category,
                o.is_original                                AS our_is_original,
                o.original_bill_id                           AS our_original_bill_id,
                o.method                                     AS our_method,
                o.matched_phrase                             AS our_matched_phrase
            FROM bill_classifications t
            FULL OUTER JOIN bill_classifications_doc_full o USING (BillID)
            WHERE COALESCE(t.classification_source, 'x') = 'tal_alovitz'
               OR o.BillID IS NOT NULL
            """
        ).df()
    finally:
        con.close()

    # Categorize each row
    both = diff.dropna(subset=["tal_is_original", "our_is_original"])
    tal_only = diff[diff["tal_is_original"].notna() & diff["our_is_original"].isna()]
    our_only = diff[diff["tal_is_original"].isna() & diff["our_is_original"].notna()]

    both = both.copy()
    both["agree"] = both["tal_is_original"] == both["our_is_original"]

    # Per-Knesset agreement
    by_knesset = (
        both.groupby("KnessetNum")
        .agg(
            total=("BillID", "count"),
            agree=("agree", "sum"),
            disagree_tal_original_we_recurring=(
                "BillID",
                lambda s: int(
                    ((both.loc[s.index, "tal_is_original"] == True)
                     & (both.loc[s.index, "our_is_original"] == False)).sum()
                ),
            ),
            disagree_tal_recurring_we_original=(
                "BillID",
                lambda s: int(
                    ((both.loc[s.index, "tal_is_original"] == False)
                     & (both.loc[s.index, "our_is_original"] == True)).sum()
                ),
            ),
        )
        .reset_index()
    )
    by_knesset["agree_pct"] = (100.0 * by_knesset["agree"] / by_knesset["total"]).round(2)

    # Write the full diff workbook
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path) as w:
        by_knesset.to_excel(w, sheet_name="summary", index=False)
        both[~both["agree"]].to_excel(w, sheet_name="disagreements", index=False)
        our_only.to_excel(w, sheet_name="our_scan_only_K1_K18", index=False)
        if len(tal_only):
            tal_only.to_excel(w, sheet_name="tal_only", index=False)

    return {
        "both_classified": len(both),
        "agree": int(both["agree"].sum()),
        "disagree": int((~both["agree"]).sum()),
        "agree_pct": round(100.0 * both["agree"].sum() / max(len(both), 1), 2),
        "our_scan_only": len(our_only),
        "tal_only": len(tal_only),
        "per_knesset_summary": by_knesset.to_dict("records"),
        "output_path": str(output_path),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path,
                   default=_REPO_ROOT / "data" / "warehouse.duckdb")
    p.add_argument("--output", type=Path,
                   default=_REPO_ROOT / "data" / "snapshots" / "our_scan_vs_tal_diff.xlsx")
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: warehouse not found: {args.db}", file=sys.stderr)
        return 1

    stats = compare(args.db, args.output)
    print(f"Bills classified by BOTH: {stats['both_classified']}")
    print(f"  Agreement: {stats['agree']} ({stats['agree_pct']}%)")
    print(f"  Disagree:  {stats['disagree']}")
    print(f"Our scan only (K1-K18):  {stats['our_scan_only']}")
    print(f"Tal only (bills we didn't reach): {stats['tal_only']}")
    print()
    print("Per-Knesset agreement:")
    for r in stats["per_knesset_summary"]:
        print(f"  K{int(r['KnessetNum']):<3} {r['total']:>5} bills   "
              f"agree={r['agree']:>5} ({r['agree_pct']}%)   "
              f"Tal-orig/we-rec={r['disagree_tal_original_we_recurring']}   "
              f"Tal-rec/we-orig={r['disagree_tal_recurring_we_original']}")
    print()
    print(f"Wrote full diff workbook: {stats['output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
