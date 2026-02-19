#!/usr/bin/env python3
"""
CLI script for merging researcher faction CSV files into a comprehensive
coalition/opposition status file covering K1-25.

Merges 3 researcher-provided CSVs (from bills, queries, agendas datasets),
deduplicates on (KnessetNum, FactionID), parses mid-term change dates,
and outputs a single CSV ready for the dashboard.

Usage:
    PYTHONPATH="./src" python import_coalition_data.py
    PYTHONPATH="./src" python import_coalition_data.py --dry-run
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).resolve().parent

# Source files (bills is primary — most comprehensive at 378 rows)
DEFAULT_FILES = [
    _project_root / "docs" / "unique_factions_bills (1).csv",
    _project_root / "docs" / "unique_factions_queries (1).csv",
    _project_root / "docs" / "unique_factions_agendas (1).csv",
]

DEFAULT_OUTPUT = _project_root / "data" / "faction_coalition_status_all_knessets.csv"

# Regex to parse "from DD/MM/YYYY in (the )?(Coalition|Opposition)"
CHANGE_PATTERN = re.compile(
    r"from\s+(\d{1,2}/\d{1,2}/\d{4})\s+in\s+(?:the\s+)?(Coalition|Opposition)",
    re.IGNORECASE,
)


def read_faction_csv(filepath: Path) -> pd.DataFrame:
    """Read a faction CSV with Hebrew encoding fallback."""
    for encoding in ("utf-8-sig", "utf-8", "windows-1255"):
        try:
            return pd.read_csv(filepath, encoding=encoding)
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError(f"Could not decode {filepath} with any supported encoding")


def merge_faction_data(files: list[Path]) -> pd.DataFrame:
    """
    Merge multiple faction CSVs, deduplicate on (KnessetNum, FactionID).

    Files are processed in order — first file (bills) is primary. When
    duplicates exist, the first occurrence is kept.
    """
    frames = []
    for f in files:
        if not f.exists():
            print(f"  Warning: {f.name} not found, skipping")
            continue
        df = read_faction_csv(f)
        frames.append(df)
        print(f"  Read {len(df)} rows from {f.name}")

    if not frames:
        raise ValueError("No input files found")

    combined = pd.concat(frames, ignore_index=True)
    print(f"  Combined: {len(combined)} rows (before dedup)")

    # Deduplicate: keep first occurrence (bills file takes priority)
    before = len(combined)
    combined = combined.drop_duplicates(subset=["KnessetNum", "FactionID"], keep="first")
    print(f"  After dedup: {len(combined)} rows (removed {before - len(combined)} duplicates)")

    return combined


def parse_change_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse Change_coalition_opposition_from into DateJoinedCoalition / DateLeftCoalition.

    Logic:
    - CoalitionStatus = initial status at Knesset start
    - "from DD/MM/YYYY in Opposition" + initial="Coalition" → DateLeftCoalition
    - "from DD/MM/YYYY in Coalition" + initial="Opposition" → DateJoinedCoalition
    """
    df = df.copy()
    df["DateJoinedCoalition"] = pd.NaT
    df["DateLeftCoalition"] = pd.NaT

    change_col = "Change_coalition_opposition_from"
    if change_col not in df.columns:
        print("  Warning: No Change_coalition_opposition_from column found")
        return df

    changes_parsed = 0
    changes_skipped = 0

    for idx, row in df.iterrows():
        change_text = row.get(change_col)
        if pd.isna(change_text) or not str(change_text).strip():
            continue

        match = CHANGE_PATTERN.search(str(change_text))
        if not match:
            changes_skipped += 1
            print(f"    Could not parse: '{change_text}'")
            continue

        date_str = match.group(1)
        new_status = match.group(2).capitalize()  # "Coalition" or "Opposition"
        initial_status = str(row.get("CoalitionStatus", "")).strip()

        try:
            parsed_date = pd.to_datetime(date_str, format="%d/%m/%Y")
        except (ValueError, TypeError):
            changes_skipped += 1
            print(f"    Invalid date: '{date_str}'")
            continue

        if initial_status == "Coalition" and new_status == "Opposition":
            # Was in Coalition, moved to Opposition → left coalition
            df.at[idx, "DateLeftCoalition"] = parsed_date
            changes_parsed += 1
        elif initial_status == "Opposition" and new_status == "Coalition":
            # Was in Opposition, moved to Coalition → joined coalition
            df.at[idx, "DateJoinedCoalition"] = parsed_date
            changes_parsed += 1
        else:
            # Edge case: same status → skip (data anomaly)
            changes_skipped += 1
            print(
                f"    Ambiguous change: K{row.get('KnessetNum')} FID={row.get('FactionID')} "
                f"status={initial_status} → {new_status}"
            )

    print(f"  Parsed {changes_parsed} mid-term changes, skipped {changes_skipped}")
    return df


def format_output(df: pd.DataFrame) -> pd.DataFrame:
    """Select and order output columns for the final CSV."""
    output_cols = [
        "KnessetNum",
        "FactionID",
        "FactionName",
        "CoalitionStatus",
        "NewFactionName",
        "DateJoinedCoalition",
        "DateLeftCoalition",
    ]
    # Ensure all output columns exist
    for col in output_cols:
        if col not in df.columns:
            df[col] = pd.NA

    result = df[output_cols].copy()
    result = result.sort_values(["KnessetNum", "FactionID"]).reset_index(drop=True)

    # Format dates as YYYY-MM-DD strings (empty string for NaT)
    for date_col in ("DateJoinedCoalition", "DateLeftCoalition"):
        result[date_col] = result[date_col].dt.strftime("%Y-%m-%d").fillna("")

    return result


def print_summary(df: pd.DataFrame) -> None:
    """Print summary statistics."""
    print(f"\n{'='*60}")
    print("  Coalition Data Summary")
    print(f"{'='*60}")
    print(f"  Total faction-Knesset rows: {len(df)}")
    print(f"  Knessets covered: {sorted(df['KnessetNum'].unique())}")
    print(f"  Unique factions: {df['FactionID'].nunique()}")

    # Status breakdown
    status_counts = df["CoalitionStatus"].value_counts(dropna=False)
    print(f"\n  Status breakdown:")
    for status, count in status_counts.items():
        label = status if pd.notna(status) else "NaN/Unknown"
        print(f"    {label:15s}: {count:4d}")

    # Mid-term changes
    joined = (df["DateJoinedCoalition"] != "").sum()
    left = (df["DateLeftCoalition"] != "").sum()
    print(f"\n  Mid-term changes:")
    print(f"    Joined coalition:  {joined}")
    print(f"    Left coalition:    {left}")

    # Per-Knesset breakdown
    print(f"\n  Factions per Knesset:")
    per_knesset = df.groupby("KnessetNum").agg(
        total=("FactionID", "count"),
        coalition=("CoalitionStatus", lambda x: (x == "Coalition").sum()),
        opposition=("CoalitionStatus", lambda x: (x == "Opposition").sum()),
        unknown=("CoalitionStatus", lambda x: x.isna().sum()),
    )
    for kn, row in per_knesset.iterrows():
        parts = []
        if row["coalition"]:
            parts.append(f"C:{row['coalition']}")
        if row["opposition"]:
            parts.append(f"O:{row['opposition']}")
        if row["unknown"]:
            parts.append(f"?:{row['unknown']}")
        print(f"    K{kn:2d}: {row['total']:3d} factions  ({', '.join(parts)})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge researcher faction CSVs into comprehensive coalition status file."
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Process and display results without writing output file"
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  Coalition/Opposition Faction Data Import")
    print(f"{'='*60}\n")

    # Step 1: Merge all source files
    print("Step 1: Reading and merging source files...")
    merged = merge_faction_data(DEFAULT_FILES)

    # Step 2: Parse mid-term change dates
    print("\nStep 2: Parsing mid-term change dates...")
    with_dates = parse_change_dates(merged)

    # Step 3: Format output
    print("\nStep 3: Formatting output...")
    output = format_output(with_dates)

    # Step 4: Summary
    print_summary(output)

    # Step 5: Write output
    if not args.dry_run:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output.to_csv(args.output, index=False)
        print(f"\n  Output saved to: {args.output}")
        print(f"  Original file preserved: data/faction_coalition_status.csv")
    else:
        print(f"\n  [DRY RUN] Would save to: {args.output}")

    print(f"\n{'='*60}")
    print("  Done!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
