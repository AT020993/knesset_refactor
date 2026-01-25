#!/usr/bin/env python3
"""
Diagnostic script to check database state and fix migration artifacts.

Run this locally with your database file:
    python diagnose_db.py

Or specify a path:
    python diagnose_db.py --db path/to/warehouse.duckdb
"""

import argparse
import sys
from pathlib import Path

import duckdb


def diagnose_and_fix(db_path: Path):
    """Diagnose and fix database issues."""
    print(f"Checking database: {db_path}")
    print("=" * 60)

    if not db_path.exists():
        print(f"ERROR: Database file not found: {db_path}")
        return False

    conn = duckdb.connect(str(db_path))

    try:
        # 1. List all tables
        print("\n1. All tables in database:")
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        for t in tables:
            print(f"   - {t[0]}")

        # 2. Check for _new tables (migration artifacts)
        print("\n2. Checking for migration artifact tables (*_new):")
        new_tables = [t[0] for t in tables if t[0].endswith('_new')]
        if new_tables:
            print(f"   FOUND: {new_tables}")
        else:
            print("   None found (good)")

        # 3. List all views
        print("\n3. All views in database:")
        try:
            views = conn.execute(
                "SELECT table_name FROM information_schema.views WHERE table_schema = 'main'"
            ).fetchall()
            if views:
                for v in views:
                    print(f"   - {v[0]}")
            else:
                print("   None")
        except Exception as e:
            print(f"   Error listing views: {e}")

        # 4. Check for any object referencing UserBillCAP_new
        print("\n4. Searching for references to 'UserBillCAP_new':")

        # Check view definitions
        try:
            view_defs = conn.execute(
                "SELECT view_name, view_definition FROM information_schema.views "
                "WHERE view_definition LIKE '%UserBillCAP_new%'"
            ).fetchall()
            if view_defs:
                print(f"   FOUND in views: {[v[0] for v in view_defs]}")
                for v in view_defs:
                    print(f"   View '{v[0]}' definition: {v[1][:200]}...")
            else:
                print("   Not found in views")
        except Exception as e:
            print(f"   Could not check views: {e}")

        # 5. Check UserBillCAP table structure
        print("\n5. UserBillCAP table structure:")
        try:
            columns = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'UserBillCAP' ORDER BY ordinal_position"
            ).fetchall()
            if columns:
                for col in columns:
                    print(f"   - {col[0]}: {col[1]}")
            else:
                print("   Table does not exist!")
        except Exception as e:
            print(f"   Error: {e}")

        # 6. Check foreign key constraints
        print("\n6. Foreign key constraints on UserBillCAP:")
        try:
            # DuckDB doesn't have a standard FK info view, try pragma
            fks = conn.execute("PRAGMA table_info('UserBillCAP')").fetchall()
            print(f"   Table info: {len(fks)} columns")
        except Exception as e:
            print(f"   Could not check: {e}")

        # 7. Check sequences
        print("\n7. Sequences in database:")
        try:
            seqs = conn.execute("SELECT * FROM duckdb_sequences()").fetchall()
            for s in seqs:
                print(f"   - {s}")
        except Exception as e:
            print(f"   Could not list: {e}")

        # 8. Try the exact query that fails
        print("\n8. Testing the failing query pattern:")
        try:
            result = conn.execute(
                "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
            ).fetchone()
            print(f"   Query succeeded: {result[0]} rows")
        except Exception as e:
            print(f"   Query FAILED: {e}")

        # 9. Check DuckDB internal catalog for stale references
        print("\n9. Checking DuckDB catalog for 'UserBillCAP_new':")
        try:
            catalog_check = conn.execute("""
                SELECT * FROM duckdb_tables()
                WHERE table_name LIKE '%UserBillCAP%'
            """).fetchall()
            for row in catalog_check:
                print(f"   Found: {row}")
        except Exception as e:
            print(f"   Could not check: {e}")

        # 10. Attempt fixes
        print("\n10. Attempting fixes:")

        # Drop any _new tables
        for t in new_tables:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {t}")
                print(f"   Dropped {t}")
            except Exception as e:
                print(f"   Could not drop {t}: {e}")

        # Force drop UserBillCAP_new even if it "doesn't exist"
        try:
            conn.execute("DROP TABLE IF EXISTS UserBillCAP_new")
            print("   Executed DROP TABLE IF EXISTS UserBillCAP_new")
        except Exception as e:
            print(f"   DROP failed: {e}")

        # Vacuum to clean up
        try:
            conn.execute("VACUUM")
            print("   VACUUM completed")
        except Exception as e:
            print(f"   VACUUM failed: {e}")

        # 11. Test query again after fixes
        print("\n11. Re-testing query after fixes:")
        try:
            result = conn.execute(
                "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
            ).fetchone()
            print(f"   Query succeeded: {result[0]} rows")
        except Exception as e:
            print(f"   Query still FAILS: {e}")

        print("\n" + "=" * 60)
        print("Diagnosis complete. Check output above for issues.")
        return True

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Diagnose database issues")
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="Path to DuckDB database file"
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    diagnose_and_fix(db_path)


if __name__ == "__main__":
    main()
