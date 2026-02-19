#!/usr/bin/env python3
"""
CLI script for importing government bill coding data into the dashboard database.

Imports coding for government bills (K10-20 and K23-24) which were not included
in the initial Round 1 import (private bills only). Uses upsert semantics so
overlapping BillIDs get their coding updated.

Usage:
    PYTHONPATH="./src" python import_government_bills.py
    PYTHONPATH="./src" python import_government_bills.py --k10-20 docs/GovernmentBills10_20_sorted.xlsx
    PYTHONPATH="./src" python import_government_bills.py --k23-24 docs/govbills_23_24.xlsx
    PYTHONPATH="./src" python import_government_bills.py --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent

try:
    from config.settings import Settings
    from utils.research_coding_importer import ResearchCodingImporter, ImportResult
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Import failed. Run with `PYTHONPATH=./src python import_government_bills.py ...`."
    ) from exc

# Reuse helpers from the existing import script
from import_research_coding import print_import_result, save_gap_analysis


DEFAULT_K10_20 = _project_root / "docs" / "GovernmentBills10_20_sorted.xlsx"
DEFAULT_K23_24 = _project_root / "docs" / "govbills_23_24.xlsx"


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("gov_bills_import")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


def import_k10_20(
    importer: ResearchCodingImporter, filepath: Path, dry_run: bool = False
) -> ImportResult:
    """
    Import K10-20 government bills.

    The source file uses 'KnessetID' for the bill ID column — we rename it
    to 'BILLID' so the existing BILL_COLUMN_MAP picks it up automatically.
    """
    result = ImportResult(
        data_type="bills (K10-20 gov)",
        total_rows_in_file=0,
        rows_imported=0,
        rows_updated=0,
        rows_skipped_no_match=0,
        rows_skipped_error=0,
        errors=[],
    )

    df, error = importer.read_file(filepath)
    if error:
        result.errors.append(error)
        return result
    if df is None:
        result.errors.append("No data loaded from file")
        return result

    result.total_rows_in_file = len(df)

    # Rename KnessetID → BILLID so BILL_COLUMN_MAP maps it to BillID
    if "KnessetID" in df.columns:
        df = df.rename(columns={"KnessetID": "BILLID"})
    elif "BILLID" not in df.columns and "billid" not in [c.lower() for c in df.columns]:
        result.errors.append(
            f"Missing required column: KnessetID or BILLID. Found: {list(df.columns)}"
        )
        return result

    # Map and clean using existing infrastructure
    mapped_df, mapped_cols, missing = importer._map_columns(df, importer.BILL_COLUMN_MAP)
    if "BillID" not in mapped_cols:
        result.errors.append(f"Column mapping failed for BillID. Found: {list(df.columns)}")
        return result
    if missing:
        print(f"  Note: missing optional columns: {missing}")

    int_cols = [c for c in mapped_cols if c != "BillID"]
    mapped_df = importer._clean_coding_values(mapped_df, int_cols)

    # Drop rows without BillID and deduplicate
    mapped_df = mapped_df.dropna(subset=["BillID"])
    mapped_df["BillID"] = mapped_df["BillID"].astype(int)
    mapped_df = mapped_df.drop_duplicates(subset=["BillID"], keep="last")

    print(f"  Prepared {len(mapped_df)} rows for import (from {result.total_rows_in_file} in file)")

    if dry_run:
        print("  [DRY RUN] Skipping database write")
        result.rows_imported = len(mapped_df)
        return result

    importer.ensure_tables_exist()
    result = importer._bulk_upsert(mapped_df, "UserBillCoding", "BillID", result)
    result.data_type = "bills (K10-20 gov)"
    return result


def import_k23_24(
    importer: ResearchCodingImporter, filepath: Path, dry_run: bool = False
) -> ImportResult:
    """
    Import K23-24 government bills.

    The source file already has BILLID. Rows with null BILLID are skipped
    with a warning. Only MAJORIL/MINORIL are populated (other coding columns
    are NULL in the source) — they're still imported so NULLs are preserved.
    """
    result = ImportResult(
        data_type="bills (K23-24 gov)",
        total_rows_in_file=0,
        rows_imported=0,
        rows_updated=0,
        rows_skipped_no_match=0,
        rows_skipped_error=0,
        errors=[],
    )

    df, error = importer.read_file(filepath)
    if error:
        result.errors.append(error)
        return result
    if df is None:
        result.errors.append("No data loaded from file")
        return result

    result.total_rows_in_file = len(df)

    # Count and report null BILLIDs
    null_count = df["BILLID"].isna().sum() if "BILLID" in df.columns else 0
    if null_count > 0:
        print(f"  Warning: {null_count} rows with null BILLID will be skipped")

    # Use existing import_bill_coding logic (handles mapping, cleaning, dedup)
    # But we call it directly since the file's BILLID column maps correctly
    mapped_df, mapped_cols, missing = importer._map_columns(df, importer.BILL_COLUMN_MAP)
    if "BillID" not in mapped_cols:
        result.errors.append(f"Missing required column: BILLID. Found: {list(df.columns)}")
        return result
    if missing:
        print(f"  Note: missing optional columns: {missing}")

    int_cols = [c for c in mapped_cols if c != "BillID"]
    mapped_df = importer._clean_coding_values(mapped_df, int_cols)

    mapped_df = mapped_df.dropna(subset=["BillID"])
    mapped_df["BillID"] = mapped_df["BillID"].astype(int)
    mapped_df = mapped_df.drop_duplicates(subset=["BillID"], keep="last")

    result.rows_skipped_no_match = null_count
    print(f"  Prepared {len(mapped_df)} rows for import (from {result.total_rows_in_file} in file)")

    if dry_run:
        print("  [DRY RUN] Skipping database write")
        result.rows_imported = len(mapped_df)
        return result

    importer.ensure_tables_exist()
    result = importer._bulk_upsert(mapped_df, "UserBillCoding", "BillID", result)
    result.data_type = "bills (K23-24 gov)"
    result.rows_skipped_no_match = null_count
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import government bill coding data into the Knesset dashboard."
    )
    parser.add_argument(
        "--k10-20", type=Path, default=DEFAULT_K10_20,
        help=f"Path to K10-20 government bills file (default: {DEFAULT_K10_20})"
    )
    parser.add_argument(
        "--k23-24", type=Path, default=DEFAULT_K23_24,
        help=f"Path to K23-24 government bills file (default: {DEFAULT_K23_24})"
    )
    parser.add_argument(
        "--db", type=Path, default=Settings.DEFAULT_DB_PATH,
        help=f"Database path (default: {Settings.DEFAULT_DB_PATH})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and validate files without writing to database"
    )
    parser.add_argument(
        "--skip-k10-20", action="store_true",
        help="Skip K10-20 import (only import K23-24)"
    )
    parser.add_argument(
        "--skip-k23-24", action="store_true",
        help="Skip K23-24 import (only import K10-20)"
    )
    args = parser.parse_args()

    logger = setup_logger()
    importer = ResearchCodingImporter(db_path=args.db, logger=logger)
    output_dir = _project_root / "data" / "gap_analysis"

    print(f"\n{'='*60}")
    print("  Government Bill Coding Import")
    print(f"{'='*60}")
    print(f"  Database: {args.db}")
    if args.dry_run:
        print("  Mode: DRY RUN (no database writes)")
    print()

    # Get pre-import stats
    pre_stats = importer.get_coding_statistics()
    print(f"  Pre-import bill coding rows: {pre_stats.get('bills', 0):,}")
    print()

    # Import K10-20
    if not args.skip_k10_20:
        print(f"Importing K10-20 government bills from: {args.k10_20}")
        result_k10_20 = import_k10_20(importer, args.k10_20, dry_run=args.dry_run)
        print_import_result(result_k10_20)
    else:
        print("Skipping K10-20 import")

    # Import K23-24
    if not args.skip_k23_24:
        print(f"\nImporting K23-24 government bills from: {args.k23_24}")
        result_k23_24 = import_k23_24(importer, args.k23_24, dry_run=args.dry_run)
        print_import_result(result_k23_24)
    else:
        print("Skipping K23-24 import")

    # Post-import stats and gap analysis
    if not args.dry_run:
        post_stats = importer.get_coding_statistics()
        print(f"\n  Post-import bill coding rows: {post_stats.get('bills', 0):,}")
        print(f"  Net change: {post_stats.get('bills', 0) - pre_stats.get('bills', 0):+,}")

        print(f"\n{'='*60}")
        print("  GAP ANALYSIS")
        print(f"{'='*60}")
        save_gap_analysis(importer, "bills", output_dir)

    print(f"\n{'='*60}")
    print("  Done!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
