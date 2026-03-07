#!/usr/bin/env python3
"""
CLI script for importing filtered coding data from Shahaf's CSVs.

Handles:
- ATI-873: 619 coded private bills from filtered_Bill_פרטית_with_coding.csv
- ATI-874: 173 coded committee bills from filtered_Bill_ועדה_with_coding.csv
- ATI-875: 547 coded government bills from filtered_Bill_ממשלתית_with_coding.csv
- ATI-876: 1,742 coded agenda items from filtered_Agenda_with_coding.csv
- ATI-877: 6 manually coded K24 queries

Uses GAP-FILL semantics: only fills NULL MajorIL/MinorIL values,
never overwrites existing codings.

Usage:
    PYTHONPATH="./src" python import_filtered_coding.py
    PYTHONPATH="./src" python import_filtered_coding.py --dry-run
    PYTHONPATH="./src" python import_filtered_coding.py --skip-bills --skip-agendas
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).resolve().parent

try:
    from config.settings import Settings
    from utils.research_coding_importer import ResearchCodingImporter, ImportResult
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Import failed. Run with `PYTHONPATH=./src python import_filtered_coding.py ...`."
    ) from exc

# Reuse helpers from the existing import script
from import_research_coding import print_import_result, save_gap_analysis


# --- Bill CSV files ---
BILL_FILES = [
    ("filtered_Bill_פרטית_with_coding.csv", "private (ATI-873)"),
    ("filtered_Bill_ועדה_with_coding.csv", "committee (ATI-874)"),
    ("filtered_Bill_ממשלתית_with_coding.csv", "government (ATI-875)"),
]

# --- K24 manual query codings (ATI-877) ---
MANUAL_QUERY_CODINGS = [
    {"QueryID": 2163246, "MajorIL": 20, "MinorIL": 2000},
    {"QueryID": 2161306, "MajorIL": 7, "MinorIL": 799},
    {"QueryID": 2160786, "MajorIL": 13, "MinorIL": 1304},
    {"QueryID": 2158621, "MajorIL": 12, "MinorIL": 1205},
    {"QueryID": 2158553, "MajorIL": 6, "MinorIL": 601},
    {"QueryID": 2157754, "MajorIL": 7, "MinorIL": 799},
]


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("filtered_coding_import")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


def _filter_coded_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame to rows where MAJORIL is not NA/empty."""
    mask = df["MAJORIL"].notna() & (df["MAJORIL"].astype(str).str.strip() != "NA")
    return df[mask].copy()


def import_bill_csv(
    importer: ResearchCodingImporter,
    filepath: Path,
    label: str,
    dry_run: bool = False,
) -> ImportResult:
    """Import a filtered bill CSV with gap-fill semantics."""
    result = ImportResult(
        data_type=f"bills ({label})",
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

    result.total_rows_in_file = len(df)
    coded = _filter_coded_rows(df)
    print(f"  {filepath.name}: {len(coded)} coded rows out of {len(df)} total")

    if coded.empty:
        result.errors.append("No coded rows found")
        return result

    # Rename ItemID -> BillID
    if "ItemID" in coded.columns:
        coded = coded.rename(columns={"ItemID": "BillID"})

    # Keep only the columns we need for gap-filling
    coded["BillID"] = pd.to_numeric(coded["BillID"], errors="coerce").astype("Int64")
    coded["MajorIL"] = pd.to_numeric(coded["MAJORIL"], errors="coerce").astype("Int64")
    coded["MinorIL"] = pd.to_numeric(coded["MINORIL"], errors="coerce").astype("Int64")

    import_df = coded[["BillID", "MajorIL", "MinorIL"]].copy()
    import_df = import_df.dropna(subset=["BillID"])
    import_df = import_df.drop_duplicates(subset=["BillID"], keep="last")

    if dry_run:
        result.rows_imported = len(import_df)
        print(f"  [DRY RUN] Would import {len(import_df)} rows")
        return result

    result = importer._bulk_upsert(
        import_df, "UserBillCoding", "BillID", result, gap_fill=True
    )
    result.data_type = f"bills ({label})"
    return result


def import_agenda_csv(
    importer: ResearchCodingImporter,
    filepath: Path,
    dry_run: bool = False,
) -> ImportResult:
    """Import filtered agenda CSV with gap-fill semantics."""
    result = ImportResult(
        data_type="agendas (ATI-876)",
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

    result.total_rows_in_file = len(df)
    coded = _filter_coded_rows(df)
    print(f"  {filepath.name}: {len(coded)} coded rows out of {len(df)} total")

    if coded.empty:
        result.errors.append("No coded rows found")
        return result

    # Rename ItemID -> AgendaID
    if "ItemID" in coded.columns:
        coded = coded.rename(columns={"ItemID": "AgendaID"})

    # Keep only the columns we need
    coded["AgendaID"] = pd.to_numeric(coded["AgendaID"], errors="coerce").astype("Int64")
    coded["MajorIL"] = pd.to_numeric(coded["MAJORIL"], errors="coerce").astype("Int64")
    coded["MinorIL"] = pd.to_numeric(coded["MINORIL"], errors="coerce").astype("Int64")

    import_df = coded[["AgendaID", "MajorIL", "MinorIL"]].copy()
    import_df = import_df.dropna(subset=["AgendaID"])
    import_df = import_df.drop_duplicates(subset=["AgendaID"], keep="last")

    if dry_run:
        result.rows_imported = len(import_df)
        print(f"  [DRY RUN] Would import {len(import_df)} rows")
        return result

    result = importer._bulk_upsert(
        import_df, "UserAgendaCoding", "AgendaID", result, gap_fill=True
    )
    return result


def insert_manual_query_codings(
    importer: ResearchCodingImporter,
    dry_run: bool = False,
) -> ImportResult:
    """Insert 6 manually coded K24 queries (ATI-877)."""
    result = ImportResult(
        data_type="queries (K24 manual, ATI-877)",
        total_rows_in_file=6,
        rows_imported=0,
        rows_updated=0,
        rows_skipped_no_match=0,
        rows_skipped_error=0,
        errors=[],
    )

    df = pd.DataFrame(MANUAL_QUERY_CODINGS)
    df["QueryID"] = df["QueryID"].astype("Int64")
    df["MajorIL"] = df["MajorIL"].astype("Int64")
    df["MinorIL"] = df["MinorIL"].astype("Int64")

    print(f"  Manual K24 query codings: {len(df)} rows")

    if dry_run:
        result.rows_imported = len(df)
        print(f"  [DRY RUN] Would import {len(df)} rows")
        return result

    result = importer._bulk_upsert(
        df, "UserQueryCoding", "QueryID", result, gap_fill=True
    )
    return result


def get_table_stats(importer: ResearchCodingImporter) -> dict:
    """Get current row counts for coding tables."""
    from backend.connection_manager import get_db_connection

    stats = {}
    try:
        with get_db_connection(importer.db_path, read_only=True, logger_obj=importer.logger) as conn:
            for table in ["UserBillCoding", "UserQueryCoding", "UserAgendaCoding"]:
                try:
                    total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    coded = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE MajorIL IS NOT NULL"
                    ).fetchone()[0]
                    stats[table] = {"total": total, "with_majoril": coded}
                except Exception:
                    stats[table] = {"total": 0, "with_majoril": 0}
    except Exception as e:
        print(f"  Warning: could not get stats: {e}")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import filtered coding data from Shahaf's CSVs (gap-fill mode)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse files without writing to DB")
    parser.add_argument("--skip-bills", action="store_true", help="Skip bill imports (ATI-873/874/875)")
    parser.add_argument("--skip-agendas", action="store_true", help="Skip agenda import (ATI-876)")
    parser.add_argument("--skip-queries", action="store_true", help="Skip query inserts (ATI-877)")
    args = parser.parse_args()

    logger = setup_logger()
    settings = Settings()
    db_path = settings.db_path
    importer = ResearchCodingImporter(db_path, logger)

    print(f"\n{'#'*60}")
    print(f"  Filtered Coding Import (Gap-Fill Mode)")
    print(f"  Database: {db_path}")
    print(f"{'#'*60}")

    # Ensure tables exist
    if not args.dry_run:
        importer.ensure_tables_exist()

    # Pre-import stats
    print("\n--- Pre-import statistics ---")
    pre_stats = get_table_stats(importer)
    for table, s in pre_stats.items():
        print(f"  {table}: {s['total']} total, {s['with_majoril']} with MajorIL")

    results = []

    # Phase 1: Bills (ATI-873, ATI-874, ATI-875)
    if not args.skip_bills:
        print(f"\n{'='*60}")
        print("  BILL IMPORTS")
        print(f"{'='*60}")
        for filename, label in BILL_FILES:
            filepath = _project_root / filename
            if not filepath.exists():
                print(f"  WARNING: {filename} not found, skipping")
                continue
            result = import_bill_csv(importer, filepath, label, args.dry_run)
            print_import_result(result)
            results.append(result)

    # Phase 2: Agendas (ATI-876)
    if not args.skip_agendas:
        print(f"\n{'='*60}")
        print("  AGENDA IMPORT")
        print(f"{'='*60}")
        agenda_file = _project_root / "filtered_Agenda_with_coding.csv"
        if agenda_file.exists():
            result = import_agenda_csv(importer, agenda_file, args.dry_run)
            print_import_result(result)
            results.append(result)
        else:
            print(f"  WARNING: {agenda_file.name} not found, skipping")

    # Phase 3: Manual queries (ATI-877)
    if not args.skip_queries:
        print(f"\n{'='*60}")
        print("  QUERY INSERTS (K24 manual)")
        print(f"{'='*60}")
        result = insert_manual_query_codings(importer, args.dry_run)
        print_import_result(result)
        results.append(result)

    # Post-import stats
    if not args.dry_run:
        print("\n--- Post-import statistics ---")
        post_stats = get_table_stats(importer)
        for table, s in post_stats.items():
            pre = pre_stats.get(table, {"total": 0, "with_majoril": 0})
            delta_total = s["total"] - pre["total"]
            delta_coded = s["with_majoril"] - pre["with_majoril"]
            print(
                f"  {table}: {s['total']} total (+{delta_total}), "
                f"{s['with_majoril']} with MajorIL (+{delta_coded})"
            )

        # Gap analysis
        output_dir = _project_root / "data" / "gap_analysis"
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n--- Gap Analysis ---")
        for data_type in ["bills", "queries", "agendas"]:
            save_gap_analysis(importer, data_type, output_dir)

    # Summary
    total_imported = sum(r.rows_imported for r in results)
    total_updated = sum(r.rows_updated for r in results)
    total_errors = sum(len(r.errors) for r in results)
    print(f"\n{'#'*60}")
    print(f"  SUMMARY: {total_imported} inserted, {total_updated} updated, {total_errors} errors")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
