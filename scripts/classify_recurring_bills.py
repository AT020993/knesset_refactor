#!/usr/bin/env python
"""CLI wrapper for src/data/recurring_bills/pipeline.py.

Run with:
    PYTHONPATH="./src" python scripts/classify_recurring_bills.py <mode> [options]

Modes:
    refresh   Pull latest from pmb.teca-it.com, rebuild outputs.
    rebuild   Rebuild outputs from existing cache (no network).
    report    Recompute coverage_report.md only.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure src/ is on the path if the user forgot PYTHONPATH
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from data.recurring_bills.pipeline import run_pipeline  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["refresh", "rebuild", "report"])
    parser.add_argument("--excel", type=Path,
                        default=_REPO_ROOT / "data" / "Private.Bills.Final.091123.xlsx")
    parser.add_argument("--cache-dir", type=Path,
                        default=_REPO_ROOT / "data" / "external" / "tal_bill_details")
    parser.add_argument("--bulk-csv", type=Path,
                        default=_REPO_ROOT / "data" / "external" / "tal_alovitz_bills.csv")
    parser.add_argument("--db", type=Path,
                        default=_REPO_ROOT / "data" / "warehouse.duckdb")
    parser.add_argument("--parquet", type=Path,
                        default=_REPO_ROOT / "data" / "snapshots" / "bill_classifications.parquet")
    parser.add_argument("--report", type=Path,
                        default=_REPO_ROOT / "data" / "recurring_bills" / "coverage_report.md")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Seconds between HTTP requests (refresh mode only)")
    parser.add_argument("--force-refresh", action="store_true",
                        help="Ignore detail cache, re-fetch every bill")
    parser.add_argument("--k16-k18-method", choices=["doc", "name"], default="doc",
                        help="How to classify K16-K18 (Tal doesn't cover these). "
                             "'doc' = download + parse Knesset docs (~30min first run, accurate). "
                             "'name' = fast name-matching fallback.")
    parser.add_argument("--knesset-docs-cache", type=Path,
                        default=_REPO_ROOT / "data" / "external" / "knesset_docs",
                        help="Cache dir for K16-K18 Knesset documents (.doc/.docx/.pdf)")
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    stats = run_pipeline(
        mode=args.mode,
        excel_path=args.excel,
        cache_dir=args.cache_dir,
        bulk_csv=args.bulk_csv,
        db_path=args.db,
        parquet_path=args.parquet,
        report_path=args.report,
        delay_s=args.delay,
        force_refresh=args.force_refresh,
        k16_k18_method=args.k16_k18_method,
        knesset_docs_cache_dir=args.knesset_docs_cache,
    )

    print(f"\nClassified {stats['total']} bills:")
    print(f"  Originals: {stats['originals']}")
    print(f"  Recurring: {stats['recurring']}")
    print(f"  Coverage report: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
