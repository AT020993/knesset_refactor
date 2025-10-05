#!/usr/bin/env python3
"""
Faction Attribution Validation Script

Runs validation to measure impact of faction attribution fixes.
Compares old (KnessetNum-only) vs new (date-based) logic.

Usage:
    python validation/run_validation.py

Output:
    - Console summary of validation results
    - Detailed CSV report (validation_results.csv)
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config.settings import Settings


def run_validation():
    """Run faction attribution validation and display results."""

    print("=" * 80)
    print("FACTION ATTRIBUTION VALIDATION")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Get database path
    db_path = Settings.DATA_DIR / 'warehouse.duckdb'
    if not db_path.exists():
        print(f"❌ ERROR: Database not found at {db_path}")
        print("Please run data refresh first.")
        return 1

    print(f"📊 Database: {db_path}\n")
    print("🔍 Running validation query (this may take a minute)...\n")

    try:
        # Connect to database
        con = duckdb.connect(str(db_path), read_only=True)

        # Complete comparison query
        comparison_query = """
        WITH BillFirstSubmission AS (
            SELECT
                B.BillID,
                MIN(earliest_date) as FirstSubmissionDate
            FROM KNS_Bill B
            LEFT JOIN (
                SELECT BI.BillID, MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
                FROM KNS_BillInitiator BI WHERE BI.LastUpdatedDate IS NOT NULL GROUP BY BI.BillID
                UNION ALL
                SELECT csi.ItemID as BillID, MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
                FROM KNS_CmtSessionItem csi JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL GROUP BY csi.ItemID
                UNION ALL
                SELECT psi.ItemID as BillID, MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
                FROM KNS_PlmSessionItem psi JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL GROUP BY psi.ItemID
                UNION ALL
                SELECT B.BillID, CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
                FROM KNS_Bill B WHERE B.PublicationDate IS NOT NULL
            ) all_dates ON B.BillID = all_dates.BillID
            WHERE all_dates.earliest_date IS NOT NULL
            GROUP BY B.BillID
        ),
        OldLogic AS (
            SELECT
                b.BillID,
                bi.PersonID,
                p.FirstName || ' ' || p.LastName as MKName,
                b.KnessetNum,
                COALESCE(f.Name, 'Unknown Faction') as FactionName
            FROM KNS_Bill b
            JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
            JOIN KNS_Person p ON bi.PersonID = p.PersonID
            LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
                AND b.KnessetNum = ptp.KnessetNum
                AND ptp.FactionID IS NOT NULL
            LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
            WHERE bi.Ordinal = 1 AND bi.PersonID IS NOT NULL AND b.KnessetNum = 25
        ),
        NewLogic AS (
            SELECT
                b.BillID,
                bi.PersonID,
                p.FirstName || ' ' || p.LastName as MKName,
                b.KnessetNum,
                COALESCE(f.Name, 'Unknown Faction') as FactionName
            FROM KNS_Bill b
            LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
            JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
            JOIN KNS_Person p ON bi.PersonID = p.PersonID
            LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
                AND b.KnessetNum = ptp.KnessetNum
                AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
                    BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
                    AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
                AND ptp.FactionID IS NOT NULL
            LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
            WHERE bi.Ordinal = 1 AND bi.PersonID IS NOT NULL AND b.KnessetNum = 25
        )
        SELECT
            o.BillID,
            o.PersonID,
            o.MKName,
            o.KnessetNum,
            o.FactionName as OldFaction,
            n.FactionName as NewFaction,
            CASE WHEN o.FactionName != n.FactionName THEN 1 ELSE 0 END as Changed
        FROM OldLogic o
        JOIN NewLogic n ON o.BillID = n.BillID AND o.PersonID = n.PersonID
        ORDER BY Changed DESC, o.MKName, o.BillID;
        """

        df = con.execute(comparison_query).fetchdf()

        total_bills = len(df)
        changed_bills = df['Changed'].sum()
        unchanged_bills = total_bills - changed_bills
        change_pct = (changed_bills / total_bills * 100) if total_bills > 0 else 0

        print("=" * 80)
        print("📈 VALIDATION RESULTS (Knesset 25)")
        print("=" * 80)
        print(f"Total Bills Analyzed.............. {total_bills:,}")
        print(f"Bills with Changed Attribution.... {changed_bills:,} ({change_pct:.2f}%)")
        print(f"Bills with Correct Attribution.... {unchanged_bills:,} ({100-change_pct:.2f}%)")
        print("=" * 80)

        if changed_bills > 0:
            # Get changed bills only
            changed_df = df[df['Changed'] == 1]

            print(f"\n📊 TOP 20 CHANGED ATTRIBUTIONS")
            print("-" * 80)
            print(f"{'MK Name':<30} {'Old Faction':<25} → {'New Faction':<25}")
            print("-" * 80)

            for _, row in changed_df.head(20).iterrows():
                print(f"{row['MKName']:<30} {row['OldFaction']:<25} → {row['NewFaction']:<25}")

            # Faction transition summary
            faction_changes = changed_df.groupby(['OldFaction', 'NewFaction']).size().reset_index(name='Count')
            faction_changes = faction_changes.sort_values('Count', ascending=False)

            print(f"\n📊 FACTION TRANSITION BREAKDOWN (Top 10)")
            print("-" * 80)
            print(f"{'Old Faction':<25} → {'New Faction':<25} {'Count':>10}")
            print("-" * 80)

            for _, row in faction_changes.head(10).iterrows():
                print(f"{row['OldFaction']:<25} → {row['NewFaction']:<25} {row['Count']:>10}")

            # Save detailed CSV
            output_path = Path(__file__).parent / 'validation_results.csv'
            changed_df.to_csv(output_path, index=False, encoding='utf-8')
            print(f"\n💾 Detailed results saved to: {output_path}")
            print(f"   Changed bills: {len(changed_df)}")
        else:
            print("\n✅ No faction attribution changes detected!")
            print("   All bills already had correct faction attribution.")

        con.close()

        print("\n" + "=" * 80)
        print("✅ Validation complete!")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\n❌ ERROR during validation: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(run_validation())
