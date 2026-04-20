#!/usr/bin/env python
"""CLI for full K1-K25 doc-based classification — independent from Tal.

Runs the doc-based Hebrew-phrase scanner (same logic as K16-K18 pipeline)
against every private bill in the warehouse, pulling doc URLs from
``KNS_DocumentBill``. Writes to ``bill_classifications_doc_full`` table —
kept separate from Tal-based ``bill_classifications`` so we can diff.

Per-Knesset incremental writes: each Knesset is committed to the table
as it finishes, so a kill mid-run keeps partial progress. Re-runs skip
Knessets already fully covered (unless --force).

Usage:
    source .venv/bin/activate
    PYTHONPATH="./src" python scripts/scan_all_bills.py         # K1-K25
    PYTHONPATH="./src" python scripts/scan_all_bills.py --knessets 1 2 3
    PYTHONPATH="./src" python scripts/scan_all_bills.py --force # re-scan all
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

import duckdb  # noqa: E402

from data.recurring_bills.full_scan import (  # noqa: E402
    _default_progress_cb,
    build_doc_based_full,
    write_full_scan_table,
)


def _already_covered(db_path: Path, knesset: int) -> bool:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        in_full = con.execute(
            """
            SELECT count(*) FROM information_schema.tables
            WHERE table_name = 'bill_classifications_doc_full'
            """
        ).fetchone()[0]
        if not in_full:
            return False
        scanned = con.execute(
            "SELECT count(*) FROM bill_classifications_doc_full WHERE KnessetNum = ?",
            [knesset],
        ).fetchone()[0]
        total = con.execute(
            "SELECT count(*) FROM KNS_Bill WHERE KnessetNum = ? AND PrivateNumber IS NOT NULL",
            [knesset],
        ).fetchone()[0]
        return scanned >= total
    finally:
        con.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path,
                   default=_REPO_ROOT / "data" / "warehouse.duckdb")
    p.add_argument("--cache-dir", type=Path,
                   default=_REPO_ROOT / "data" / "external" / "knesset_docs")
    p.add_argument("--knessets", type=int, nargs="+",
                   default=list(range(1, 26)),
                   help="Knessets to scan (default: 1-25)")
    p.add_argument("--delay", type=float, default=0.3,
                   help="Seconds between fresh downloads")
    p.add_argument("--force", action="store_true",
                   help="Re-scan Knessets already fully covered")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("scan_all_bills")

    total_start = time.time()
    total_rows = 0

    for k in args.knessets:
        if (not args.force) and _already_covered(args.db, k):
            log.info("K%d already fully covered in bill_classifications_doc_full — skip (use --force to rescan)", k)
            continue

        log.info("=" * 60)
        log.info("K%d: starting doc-based scan", k)
        log.info("=" * 60)
        start = time.time()

        df = build_doc_based_full(
            warehouse_path=args.db,
            cache_dir=args.cache_dir,
            knessets=[k],
            delay_s=args.delay,
            progress_cb=_default_progress_cb,
        )
        write_full_scan_table(df, db_path=args.db)

        elapsed = time.time() - start
        recurring = int((df["is_original"] == False).sum())  # noqa: E712
        originals = int((df["is_original"] == True).sum())  # noqa: E712
        log.info("K%d done: %d bills  (%d originals, %d recurring)  in %.0fs",
                 k, len(df), originals, recurring, elapsed)
        total_rows += len(df)

    total_elapsed = time.time() - total_start
    log.info("=" * 60)
    log.info("FULL SCAN COMPLETE: %d bills across %d Knessets in %.0f min",
             total_rows, len(args.knessets), total_elapsed / 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
