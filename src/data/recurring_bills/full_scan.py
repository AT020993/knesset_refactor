"""Independent doc-based classification for ALL K1-K25 private bills.

Per Prof. Amnon's 2026-04-20 request: "do the scan like Tal did but on
everything — preferred that the data be *ours*". This module replicates
Tal's method (Hebrew-phrase regex on דברי ההסבר) across every private
bill in the Knesset warehouse, including:

- K1-K15 (which Tal's corpus does not cover)
- The ~6,146 private K16-K18 bills missing from Prof. Amnon's original Excel
- K19-K25 independently (so we can diff against Tal's `pmb.teca-it.com`)

URL source: `KNS_DocumentBill.FilePath`, filtered to the "הצעת חוק לדיון
מוקדם" doc type (the preliminary-discussion proposal with explanatory
notes — verified identical to the URLs in Prof. Amnon's Excel for
K16-K18). Falls back to "הצעת חוק לקריאה הראשונה" if no preliminary doc.

Writes results to ``bill_classifications_doc_full`` — a *separate* table
from ``bill_classifications``, preserving Tal's labels for comparison.

Resumable by virtue of the disk cache at ``data/external/knesset_docs/``
(shared with the K16-K18 pipeline). Per-bill exception isolation means
one bad bill doesn't poison the run.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from data.recurring_bills.knesset_docs import classify_bill_from_doc

log = logging.getLogger(__name__)

_DOC_TYPE_PREFERENCE = [
    "הצעת חוק לדיון מוקדם",
    "הצעת חוק לקריאה הראשונה",
]


def _pick_best_doc_url(con: duckdb.DuckDBPyConnection, bill_id: int) -> str | None:
    """Return the highest-priority doc URL for a BillID, or None if none exists."""
    for doc_type in _DOC_TYPE_PREFERENCE:
        row = con.execute(
            """
            SELECT FilePath
            FROM KNS_DocumentBill
            WHERE BillID = ? AND GroupTypeDesc = ? AND FilePath IS NOT NULL
            ORDER BY LastUpdatedDate ASC
            LIMIT 1
            """,
            [bill_id, doc_type],
        ).fetchone()
        if row and row[0]:
            return row[0]
    return None


def build_doc_based_full(
    *,
    warehouse_path: Path,
    cache_dir: Path,
    knessets: list[int] | None = None,
    delay_s: float = 0.3,
    progress_cb=None,
) -> pd.DataFrame:
    """Classify every private bill in the warehouse via doc-based method.

    Args:
        warehouse_path: DuckDB warehouse file (read-only).
        cache_dir: Doc cache. Shared with K16-K18 pipeline.
        knessets: Optional filter (e.g. ``[1, 2, 3]``). Default: all K1-K25.
        delay_s: Polite HTTP delay between fresh downloads.
        progress_cb: Optional ``cb(i, total, bill_id, method)`` for TUIs.

    Returns:
        DataFrame with columns:
        BillID, KnessetNum, Name, PrivateNumber,
        is_original, original_bill_id, matched_phrase, method,
        doc_url, classification_source, last_updated.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        where_k = ""
        params: list = []
        if knessets:
            where_k = f"AND KnessetNum IN ({','.join('?' for _ in knessets)})"
            params = list(knessets)

        bills = con.execute(
            f"""
            SELECT BillID, KnessetNum, Name, PrivateNumber
            FROM KNS_Bill
            WHERE PrivateNumber IS NOT NULL
              {where_k}
            ORDER BY KnessetNum, BillID
            """,
            params,
        ).df()

        log.info(
            "Full doc-based scan: %d private bills across K%s-K%s",
            len(bills),
            int(bills["KnessetNum"].min()) if len(bills) else "?",
            int(bills["KnessetNum"].max()) if len(bills) else "?",
        )

        results: list[dict] = []
        total = len(bills)
        now = datetime.now(timezone.utc)

        for i, row in bills.iterrows():
            bid = int(row["BillID"])
            kn = int(row["KnessetNum"])

            doc_url = _pick_best_doc_url(con, bid)
            if not doc_url:
                results.append({
                    "BillID": bid,
                    "KnessetNum": kn,
                    "Name": row["Name"],
                    "PrivateNumber": row["PrivateNumber"],
                    "is_original": True,
                    "original_bill_id": bid,
                    "matched_phrase": None,
                    "method": "no_doc_url",
                    "doc_url": None,
                })
            else:
                r = classify_bill_from_doc(
                    bill_id=bid,
                    current_knesset=kn,
                    doc_url=doc_url,
                    cache_dir=cache_dir,
                    warehouse_con=con,
                    delay_s=delay_s,
                )
                is_rec = r["is_recurring"]
                results.append({
                    "BillID": bid,
                    "KnessetNum": kn,
                    "Name": row["Name"],
                    "PrivateNumber": row["PrivateNumber"],
                    "is_original": not is_rec,
                    "original_bill_id": r["original_bill_id"] if r["original_bill_id"] else bid,
                    "matched_phrase": r["matched_phrase"],
                    "method": r["method"],
                    "doc_url": doc_url,
                })

            if progress_cb:
                progress_cb(int(i) + 1, total, bid, results[-1]["method"])
    finally:
        con.close()

    df = pd.DataFrame(results)
    df["classification_source"] = "doc_based_full"
    df["last_updated"] = now
    return df


def write_full_scan_table(df: pd.DataFrame, *, db_path: Path) -> None:
    """Upsert results into ``bill_classifications_doc_full`` table.

    Separate from ``bill_classifications`` — preserves Tal's data for
    comparison. Creates the table on first run.
    """
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS bill_classifications_doc_full (
                BillID BIGINT PRIMARY KEY,
                KnessetNum BIGINT,
                Name VARCHAR,
                PrivateNumber DOUBLE,
                is_original BOOLEAN,
                original_bill_id BIGINT,
                matched_phrase VARCHAR,
                method VARCHAR,
                doc_url VARCHAR,
                classification_source VARCHAR,
                last_updated TIMESTAMP
            )
            """
        )
        con.register("incoming", df)
        con.execute(
            """
            DELETE FROM bill_classifications_doc_full
            WHERE BillID IN (SELECT BillID FROM incoming)
            """
        )
        con.execute("INSERT INTO bill_classifications_doc_full SELECT * FROM incoming")
        n = con.execute("SELECT count(*) FROM bill_classifications_doc_full").fetchone()[0]
        log.info("Wrote %d rows; bill_classifications_doc_full now has %d total", len(df), n)
    finally:
        con.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _default_progress_cb(i: int, total: int, bid: int, method: str) -> None:
    if i % 100 == 0 or i == total:
        pct = 100.0 * i / total if total else 0
        log.info("%s  [%5d/%5d  %5.1f%%]  last: bill=%d method=%s",
                 _now_iso(), i, total, pct, bid, method)
