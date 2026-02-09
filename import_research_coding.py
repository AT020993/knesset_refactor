#!/usr/bin/env python3
"""
CLI script for importing research coding data into the dashboard database.

Usage:
    PYTHONPATH="./src" python import_research_coding.py \
        --bills all_bills_final_updated.xlsx \
        --queries parliamentary_queries_coded_KN17_24_Feb2026.xlsx \
        --agendas motions_agenda_coded_KN19_20_23_24.xlsx

Each flag is optional — import only the data types you need.
Gap analysis reports are saved to data/gap_analysis/.
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure src is on path
_project_root = Path(__file__).resolve().parent
_src_dir = _project_root / "src"
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))

from config.settings import Settings
from utils.research_coding_importer import ResearchCodingImporter, ImportResult


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("research_coding_import")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


def print_import_result(result: ImportResult) -> None:
    """Pretty-print import results."""
    print(f"\n{'='*60}")
    print(f"  Import Results: {result.data_type.upper()}")
    print(f"{'='*60}")
    print(f"  Total rows in file:    {result.total_rows_in_file:,}")
    print(f"  Rows inserted:         {result.rows_imported:,}")
    print(f"  Rows updated:          {result.rows_updated:,}")
    print(f"  Rows skipped (no ID):  {result.rows_skipped_no_match:,}")
    print(f"  Rows with errors:      {result.rows_skipped_error:,}")

    if result.match_method_counts:
        print(f"\n  Match method breakdown:")
        for method, count in sorted(result.match_method_counts.items()):
            print(f"    {method:20s}: {count:,}")

    if result.errors:
        print(f"\n  Errors ({len(result.errors)}):")
        for err in result.errors[:10]:
            print(f"    - {err}")
        if len(result.errors) > 10:
            print(f"    ... and {len(result.errors) - 10} more")


def save_gap_analysis(importer: ResearchCodingImporter, data_type: str, output_dir: Path) -> None:
    """Run gap analysis and save CSVs."""
    gap = importer.generate_gap_analysis(data_type)
    if gap is None:
        print(f"  Could not generate gap analysis for {data_type}")
        return

    print(f"\n  Gap Analysis: {data_type}")
    print(f"    Dashboard items:     {gap.total_in_dashboard:,}")
    print(f"    Coded items:         {gap.total_coded:,}")
    print(f"    Coded & matched:     {gap.coded_and_matched:,}")
    if gap.total_in_dashboard > 0:
        pct = round(100.0 * gap.coded_and_matched / gap.total_in_dashboard, 1)
        print(f"    Coverage:            {pct}%")

    coded_orphans = len(gap.coded_not_in_dashboard) if not gap.coded_not_in_dashboard.empty else 0
    print(f"    Coded not in DB:     {coded_orphans:,}")

    if not gap.coverage_by_knesset.empty:
        print(f"\n    Per-Knesset coverage:")
        for _, row in gap.coverage_by_knesset.iterrows():
            kn = int(row.get("KnessetNum", 0))
            total = int(row.get("TotalInDashboard", 0))
            coded = int(row.get("TotalCoded", 0))
            pct = row.get("CoveragePct", 0.0)
            print(f"      K{kn:2d}: {coded:>6,} / {total:>6,}  ({pct:.1f}%)")

    # Save CSVs
    output_dir.mkdir(parents=True, exist_ok=True)
    if not gap.coverage_by_knesset.empty:
        path = output_dir / f"{data_type}_coverage_by_knesset.csv"
        gap.coverage_by_knesset.to_csv(path, index=False)
        print(f"\n    Saved: {path}")

    if not gap.coded_not_in_dashboard.empty:
        path = output_dir / f"{data_type}_coded_not_in_dashboard.csv"
        gap.coded_not_in_dashboard.to_csv(path, index=False)
        print(f"    Saved: {path}")

    if not gap.uncoded_in_dashboard.empty:
        path = output_dir / f"{data_type}_uncoded_in_dashboard.csv"
        gap.uncoded_in_dashboard.to_csv(path, index=False)
        print(f"    Saved: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import research coding data into the Knesset dashboard database."
    )
    parser.add_argument("--bills", type=Path, help="Path to bills coding file (xlsx/csv)")
    parser.add_argument("--queries", type=Path, help="Path to queries coding file (xlsx/csv)")
    parser.add_argument("--agendas", type=Path, help="Path to agendas coding file (xlsx/csv)")
    parser.add_argument(
        "--db", type=Path, default=Settings.DEFAULT_DB_PATH,
        help=f"Database path (default: {Settings.DEFAULT_DB_PATH})"
    )
    parser.add_argument("--gap-only", action="store_true", help="Only run gap analysis, skip import")
    args = parser.parse_args()

    if not args.gap_only and not any([args.bills, args.queries, args.agendas]):
        parser.error("Provide at least one of --bills, --queries, --agendas (or use --gap-only)")

    logger = setup_logger()
    importer = ResearchCodingImporter(db_path=args.db, logger=logger)
    output_dir = _project_root / "data" / "gap_analysis"

    if not args.gap_only:
        if args.bills:
            print(f"\nImporting bills from: {args.bills}")
            result = importer.import_bill_coding(args.bills)
            print_import_result(result)
            if result.unmatched_items is not None and not result.unmatched_items.empty:
                path = output_dir / "bills_unmatched.csv"
                output_dir.mkdir(parents=True, exist_ok=True)
                result.unmatched_items.to_csv(path, index=False)
                print(f"  Unmatched items saved: {path}")

        if args.queries:
            print(f"\nImporting queries from: {args.queries}")
            result = importer.import_query_coding(args.queries)
            print_import_result(result)

        if args.agendas:
            print(f"\nImporting agendas from: {args.agendas}")
            result = importer.import_agenda_coding(args.agendas)
            print_import_result(result)
            if result.unmatched_items is not None and not result.unmatched_items.empty:
                path = output_dir / "agendas_unmatched.csv"
                output_dir.mkdir(parents=True, exist_ok=True)
                result.unmatched_items.to_csv(path, index=False)
                print(f"  Unmatched items saved: {path}")

    # Gap analysis for all imported types
    print(f"\n{'='*60}")
    print("  GAP ANALYSIS")
    print(f"{'='*60}")

    stats = importer.get_coding_statistics()
    for dtype in ("bills", "queries", "agendas"):
        if stats.get(dtype, 0) > 0:
            save_gap_analysis(importer, dtype, output_dir)
        else:
            print(f"\n  {dtype}: No coding data loaded, skipping gap analysis")

    print(f"\n{'='*60}")
    print(f"  Summary: {stats}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
