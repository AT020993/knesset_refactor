#!/usr/bin/env python3
"""
Export uncoded private bills with Knesset website links for RA coding.

Reads filtered_Bill_פרטית_with_coding.csv, filters rows where
MAJORIL is NA/empty, adds Knesset website URLs, and exports to Excel.

Usage:
    python export_uncoded_private_bills.py
    python export_uncoded_private_bills.py --output my_output.xlsx
"""

import argparse
from pathlib import Path

import pandas as pd

KNESSET_BILL_URL = (
    "https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/"
    "LawBill.aspx?t=lawsuggestionssearch&lawitemid={bill_id}"
)

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_ROOT / "filtered_Bill_פרטית_with_coding.csv"
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "data" / "gap_analysis" / "uncoded_private_bills_with_links.xlsx"
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export uncoded private bills with Knesset website links"
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    # Read CSV
    df = pd.read_csv(args.input, encoding="utf-8-sig")
    print(f"Read {len(df)} total rows from {args.input.name}")

    # Filter uncoded rows (MAJORIL is NA or string "NA")
    uncoded = df[
        df["MAJORIL"].isna() | (df["MAJORIL"].astype(str).str.strip().isin(["NA", ""]))
    ].copy()
    print(f"Found {len(uncoded)} uncoded rows")

    # Add Knesset website link
    uncoded["KnessetLink"] = uncoded["ItemID"].apply(
        lambda bid: KNESSET_BILL_URL.format(bill_id=int(bid))
    )

    # Select and rename output columns
    output_cols = [
        "ItemID",
        "KnessetNum",
        "ItemName",
        "Status",
        "InitiatorName",
        "FactionName",
        "KnessetLink",
    ]
    output_df = uncoded[output_cols].copy()
    output_df = output_df.rename(columns={"ItemID": "BillID"})

    # Sort by KnessetNum, then BillID
    output_df = output_df.sort_values(["KnessetNum", "BillID"])

    # Export
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_excel(args.output, index=False, engine="openpyxl")
    print(f"\nExported {len(output_df)} uncoded bills to {args.output}")

    # Summary by Knesset
    print("\nBy Knesset:")
    for kn, group in output_df.groupby("KnessetNum"):
        print(f"  K{kn}: {len(group)} bills")

    print(f"\nDone. Share {args.output.name} with Shahaf for RA distribution.")


if __name__ == "__main__":
    main()
