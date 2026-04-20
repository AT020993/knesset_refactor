#!/usr/bin/env python
"""Produce the list of K16-K18 private bills that Amnon's Excel is missing.

Scope: bills that
  (a) exist in the warehouse ``KNS_Bill`` as private (``PrivateNumber IS NOT NULL``)
  (b) are referenced as ancestors by our K16-K18 doc-based classifier
      (i.e. something in Amnon's Excel cites them as its original)
  (c) are themselves NOT in Amnon's Excel

Scope is intentionally narrow: the warehouse has ~6,145 K16-K18 private
bills not in Amnon's Excel, but most of those were never referenced by a
K16-K18 reprise and may be intentionally excluded from his corpus. The
ones we emit are the ones with direct operational relevance — each is
the "original" that at least one bill in his Excel points to.

Output: ``data/snapshots/K16_K18_missing_bills_for_amnon.xlsx`` with:
  - BillID, KnessetNum, Name, PrivateNumber, SubTypeDesc
  - referenced_by_count: how many K16-K18 bills in Amnon's Excel cite it
  - referenced_by_billids: sample (up to 5) of those citing bills
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent


def build_missing_list(db_path: Path) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        referrers = con.execute(
            """
            SELECT
                CAST(original_bill_id AS BIGINT) AS ancestor_bill_id,
                BillID AS referrer_bill_id,
                KnessetNum AS referrer_knesset
            FROM bill_classifications
            WHERE classification_source = 'doc_based_k16_k18'
              AND is_original = FALSE
              AND original_bill_id IS NOT NULL
              AND original_bill_id NOT IN (SELECT BillID FROM bill_classifications)
            """
        ).df()

        if referrers.empty:
            return pd.DataFrame(columns=[
                "BillID", "KnessetNum", "Name", "PrivateNumber",
                "SubTypeDesc", "referenced_by_count", "referenced_by_billids"
            ])

        agg = (
            referrers.groupby("ancestor_bill_id")
            .agg(
                referenced_by_count=("referrer_bill_id", "nunique"),
                referenced_by_billids=(
                    "referrer_bill_id",
                    lambda s: ", ".join(str(x) for x in sorted(set(s))[:5])
                    + (" …" if len(set(s)) > 5 else ""),
                ),
            )
            .reset_index()
            .rename(columns={"ancestor_bill_id": "BillID"})
        )

        ids = agg["BillID"].astype(int).tolist()
        meta = con.execute(
            f"""
            SELECT BillID, KnessetNum, Name, PrivateNumber, SubTypeDesc
            FROM KNS_Bill
            WHERE BillID IN ({','.join('?' for _ in ids)})
            """,
            ids,
        ).df()

        merged = meta.merge(agg, on="BillID", how="right")
        # Flag whether the ancestor is within Amnon's corpus Knesset range
        # (K16-K18) — if yes, he could add it to his Excel; if no (K13-K15)
        # it's only useful as historical provenance.
        merged["in_amnon_knesset_scope"] = merged["KnessetNum"].between(16, 18)
        merged = merged.sort_values(
            ["in_amnon_knesset_scope", "KnessetNum", "referenced_by_count"],
            ascending=[False, True, False],
        ).reset_index(drop=True)
        return merged
    finally:
        con.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path,
                   default=_REPO_ROOT / "data" / "warehouse.duckdb")
    p.add_argument("--output", type=Path,
                   default=_REPO_ROOT / "data" / "snapshots"
                                      / "K16_K18_missing_bills_for_amnon.xlsx")
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: warehouse not found: {args.db}", file=sys.stderr)
        return 1

    df = build_missing_list(args.db)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(args.output, index=False)

    print(f"Total missing ancestors referenced by K16-K18 doc-scan: {len(df)}")
    print(f"By Knesset / SubType:")
    if not df.empty:
        print(df.groupby(["KnessetNum", "SubTypeDesc"]).size().to_string())
        print(f"\nReference-count distribution:")
        print(df["referenced_by_count"].describe().to_string())
    print(f"\nWrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
