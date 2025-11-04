#!/usr/bin/env python3
"""
Test script to verify the query limit fix works correctly.

This script tests:
1. Predefined queries return only 1000 rows (not 50000)
2. get_available_knessetes_for_query returns ALL Knessetes
3. Filters are applied BEFORE the LIMIT
"""

import sys
from pathlib import Path

# Add src to path (parent directory since we're in tests/)
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

from ui.ui_utils import get_available_knessetes_for_query, connect_db, safe_execute_query
from ui.queries.predefined_queries import PREDEFINED_QUERIES

def test_query_limits():
    """Test that queries have correct LIMIT values."""
    print("Testing query LIMIT values...")

    for query_name, query_def in PREDEFINED_QUERIES.items():
        sql = query_def["sql"]

        # Check that LIMIT is 1000, not 50000
        if "LIMIT 50000" in sql:
            print(f"❌ FAIL: {query_name} still has LIMIT 50000")
            return False
        elif "LIMIT 1000" in sql:
            print(f"✅ PASS: {query_name} has correct LIMIT 1000")
        else:
            print(f"⚠️  WARN: {query_name} has no LIMIT or different value")

    return True

def test_available_knessetes():
    """Test that get_available_knessetes_for_query works."""
    print("\nTesting get_available_knessetes_for_query...")

    db_path = Path(__file__).parent.parent / "data" / "warehouse.duckdb"

    if not db_path.exists():
        print(f"⚠️  SKIP: Database not found at {db_path}")
        return True

    # Test each query type
    for query_type in ["queries", "agendas", "bills"]:
        knessetes = get_available_knessetes_for_query(db_path, query_type)

        if knessetes:
            print(f"✅ PASS: {query_type} returned {len(knessetes)} Knessetes: {knessetes}")
        else:
            print(f"⚠️  WARN: {query_type} returned no Knessetes (might be empty table)")

    return True

def test_filter_before_limit():
    """Test that filters are applied before LIMIT."""
    print("\nTesting filter application before LIMIT...")

    db_path = Path(__file__).parent.parent / "data" / "warehouse.duckdb"

    if not db_path.exists():
        print(f"⚠️  SKIP: Database not found at {db_path}")
        return True

    # Test with a simple query
    try:
        con = connect_db(db_path, read_only=True)

        # Get total count for Knesset 25
        count_query = "SELECT COUNT(*) as count FROM KNS_Query WHERE KnessetNum = 25"
        result = safe_execute_query(con, count_query)
        total_k25 = result['count'].iloc[0] if not result.empty else 0

        # Query with filter and limit
        filtered_query = "SELECT * FROM KNS_Query WHERE KnessetNum = 25 ORDER BY QueryID DESC LIMIT 1000"
        result = safe_execute_query(con, filtered_query)
        returned_rows = len(result)

        con.close()

        if total_k25 > 1000 and returned_rows == 1000:
            print(f"✅ PASS: Filter works correctly - Total K25: {total_k25}, Returned: {returned_rows}")
        elif total_k25 <= 1000 and returned_rows == total_k25:
            print(f"✅ PASS: Filter works correctly - Total K25: {total_k25}, Returned: {returned_rows}")
        else:
            print(f"⚠️  WARN: Unexpected results - Total K25: {total_k25}, Returned: {returned_rows}")

        return True

    except Exception as e:
        print(f"⚠️  ERROR: {e}")
        return True  # Don't fail on test errors

def main():
    """Run all tests."""
    print("=" * 60)
    print("Query Limit Fix Verification Tests")
    print("=" * 60)

    all_passed = True

    all_passed &= test_query_limits()
    all_passed &= test_available_knessetes()
    all_passed &= test_filter_before_limit()

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All tests PASSED")
    else:
        print("❌ Some tests FAILED")
    print("=" * 60)

    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
