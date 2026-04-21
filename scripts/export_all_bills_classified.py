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


def apply_option_c_post_pass(df: pd.DataFrame) -> pd.DataFrame:
    """Same semantics as scripts/export_amnon_from_our_scan.py, adapted to
    operate over the complete warehouse universe (not a subset).

    Algorithm:
    1. Capture factual reprise signal as ``is_recurring_upstream``.
    2. Walk each recurring bill up the chain. Stop at: an ancestor with
       is_original=True → traceable. Self-loop, off-universe parent, or
       cycle → untraceable.
    3. Traceable: keep is_original=False, flatten original_bill_id.
    4. Untraceable: flip to is_original=True, reason explains why.
    """
    df["is_recurring_upstream"] = df["is_original"].eq(False)
    df["effective_original_reason"] = pd.NA

    universe_ids = set(df["BillID"].dropna().astype(int))

    raw_parent = {
        int(b): (None if pd.isna(p) else int(p))
        for b, p in zip(df["BillID"], df["original_bill_id"])
    }
    raw_is_orig = {
        int(b): bool(v) if pd.notna(v) else False
        for b, v in zip(df["BillID"], df["is_original"])
    }

    def walk_to_original(bid: int) -> int | None:
        seen: set[int] = set()
        cur = bid
        while cur not in seen:
            seen.add(cur)
            if cur not in universe_ids:
                return None
            if raw_is_orig.get(cur, False):
                return cur if cur != bid else None
            parent = raw_parent.get(cur)
            if parent is None or parent == cur:
                return None
            cur = parent
        return None

    def reason_for(row: pd.Series) -> str:
        m = row.get("method")
        if m == "doc_fetch_failed":
            return "doc_fetch_failed"
        if m == "no_doc_url":
            return "no_doc_url"
        orig_id = row.get("original_bill_id")
        if not pd.isna(orig_id) and int(orig_id) == int(row["BillID"]):
            return "doc_no_ancestor_found"
        return "ancestor_outside_universe"

    rec_idx = df.index[df["is_original"] == False]  # noqa: E712
    for idx in rec_idx:
        bid = int(df.at[idx, "BillID"])
        ancestor = walk_to_original(bid)
        if ancestor is not None:
            df.at[idx, "original_bill_id"] = ancestor
        else:
            df.at[idx, "is_original"] = True
            df.at[idx, "original_bill_id"] = bid
            df.at[idx, "effective_original_reason"] = reason_for(df.loc[idx])

    return df


def verify(df: pd.DataFrame) -> dict:
    orig = df[df["is_original"] == True]  # noqa: E712
    rec = df[df["is_original"] == False]  # noqa: E712
    all_ids = set(df["BillID"].dropna().astype(int))
    chain = dict(zip(df["BillID"].astype(int), df["is_original"].astype(bool)))
    return {
        "originals_not_self_referencing": int(
            (orig["original_bill_id"].astype("Int64") != orig["BillID"].astype("Int64")).sum()
        ),
        "recurring_self_referencing": int(
            (rec["original_bill_id"].astype("Int64") == rec["BillID"].astype("Int64")).sum()
        ),
        "recurring_ancestor_outside_universe": int(
            (~rec["original_bill_id"].isin(all_ids)).sum()
        ),
        "recurring_ancestor_also_recurring": int(
            rec["original_bill_id"].astype("Int64").map(
                lambda x: False if pd.isna(x) else not chain.get(int(x), True)
            ).sum()
        ),
    }


def export(*, db_path: Path, output_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            """
            SELECT
                c.BillID,
                c.KnessetNum,
                c.Name,
                c.PrivateNumber,
                b.SubTypeDesc,
                c.doc_url,
                c.method,
                c.matched_phrase,
                c.is_original,
                c.original_bill_id,
                c.classification_source
            FROM bill_classifications_doc_full c
            LEFT JOIN KNS_Bill b ON c.BillID = b.BillID
            ORDER BY c.KnessetNum, c.BillID
            """
        ).df()
        # Ancestor K# + פ/NNN reference
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

    raw_originals = int((df["is_original"] == True).sum())  # noqa: E712
    raw_recurring = int((df["is_original"] == False).sum())  # noqa: E712

    # Derive recurrence_type from matched_phrase BEFORE the Option-C post-pass
    # (post-pass may flip untraceable recurring → effective-original, and those
    # rows should still carry the upstream recurrence_type for analysis).
    def _classify_type(phrase):
        if phrase is None or (isinstance(phrase, float) and pd.isna(phrase)):
            return None
        s = str(phrase)
        # "Similar"-family: explicit דומה or "building on" phrases
        if "דומה" in s or "המשך" in s:
            return "similar"
        # "Identical"-family: זהה, חוזר (re-submission), or the standard
        # bureaucratic tabling boilerplate which almost always accompanies
        # an identical re-submission.
        if "זהה" in s or "חוזר" in s or s.startswith("הונחה") or s.startswith("הוגש") or s.startswith("ומספרה"):
            return "identical"
        return None

    df["recurrence_type"] = df["matched_phrase"].apply(_classify_type)

    df = apply_option_c_post_pass(df)
    df = df.merge(bill_ref, on="original_bill_id", how="left")
    violations = verify(df)

    # Column ordering — most useful columns first for Amnon
    col_order = [
        "BillID", "KnessetNum", "Name", "PrivateNumber", "SubTypeDesc",
        "doc_url",
        "is_original", "original_bill_id",
        "original_knesset_num", "original_private_number",
        "is_recurring_upstream", "recurrence_type", "effective_original_reason",
        "method", "matched_phrase", "classification_source",
    ]
    df = df[col_order]

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
