#!/usr/bin/env python3
"""
Test script for Full Dataset Download feature.
Demonstrates the SQL query modification functionality.
"""

import re


def remove_limit_offset_from_query(sql: str) -> str:
    """
    Remove LIMIT and OFFSET clauses from SQL query.

    Args:
        sql: SQL query string with LIMIT/OFFSET

    Returns:
        Modified SQL query without LIMIT/OFFSET clauses
    """
    # Remove LIMIT clause (handles "LIMIT 1000" or "LIMIT 1000 OFFSET 1000")
    sql = re.sub(r'\s+LIMIT\s+\d+', '', sql, flags=re.IGNORECASE)
    # Remove standalone OFFSET clause
    sql = re.sub(r'\s+OFFSET\s+\d+', '', sql, flags=re.IGNORECASE)
    return sql.strip()


def test_query_modification():
    """Test the query modification with various SQL patterns."""

    test_cases = [
        {
            "name": "Simple query with LIMIT",
            "input": "SELECT * FROM KNS_Bill WHERE KnessetNum = 25 LIMIT 1000",
            "expected_removed": ["LIMIT 1000"],
            "expected_preserved": ["SELECT *", "FROM KNS_Bill", "WHERE KnessetNum = 25"]
        },
        {
            "name": "Query with LIMIT and OFFSET",
            "input": "SELECT * FROM KNS_Bill WHERE KnessetNum = 25 ORDER BY BillID DESC LIMIT 1000 OFFSET 2000",
            "expected_removed": ["LIMIT 1000", "OFFSET 2000"],
            "expected_preserved": ["SELECT *", "FROM KNS_Bill", "WHERE KnessetNum = 25", "ORDER BY BillID DESC"]
        },
        {
            "name": "Complex query with CTE",
            "input": """
                WITH BillFirstSubmission AS (
                    SELECT BillID, MIN(FirstSubmissionDate) as SubmitDate
                    FROM KNS_BillInitiator
                    GROUP BY BillID
                )
                SELECT b.*, bfs.SubmitDate
                FROM KNS_Bill b
                JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
                WHERE b.KnessetNum = 25
                ORDER BY bfs.SubmitDate DESC
                LIMIT 1000
            """,
            "expected_removed": ["LIMIT 1000"],
            "expected_preserved": ["WITH BillFirstSubmission", "SELECT b.*", "JOIN", "WHERE b.KnessetNum = 25"]
        },
        {
            "name": "Query with faction filters",
            "input": """
                SELECT b.*, f.FactionName
                FROM KNS_Bill b
                JOIN KNS_PersonToPosition ptp ON b.ProposerID = ptp.PersonID
                JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                WHERE b.KnessetNum IN (25)
                  AND ptp.FactionID IN (123, 456)
                ORDER BY b.PublicationDate DESC
                LIMIT 1000 OFFSET 5000
            """,
            "expected_removed": ["LIMIT 1000", "OFFSET 5000"],
            "expected_preserved": [
                "WHERE b.KnessetNum IN (25)",
                "AND ptp.FactionID IN (123, 456)",
                "ORDER BY"
            ]
        },
        {
            "name": "Query with multiple ORDER BY",
            "input": "SELECT * FROM table WHERE x > 5 ORDER BY y DESC, z ASC LIMIT 1000",
            "expected_removed": ["LIMIT 1000"],
            "expected_preserved": ["ORDER BY y DESC, z ASC"]
        }
    ]

    print("=" * 80)
    print("TESTING: Full Dataset Download - SQL Query Modification")
    print("=" * 80)
    print()

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"Test {i}: {test['name']}")
        print("-" * 80)

        # Run the modification
        result = remove_limit_offset_from_query(test['input'])

        # Check what was removed
        removed_ok = True
        for expected_removal in test['expected_removed']:
            if expected_removal.upper() in result.upper():
                print(f"  ❌ FAILED: '{expected_removal}' should be removed but still present")
                removed_ok = False
                all_passed = False

        if removed_ok:
            print(f"  ✅ PASSED: All expected clauses removed ({', '.join(test['expected_removed'])})")

        # Check what was preserved
        preserved_ok = True
        for expected_preservation in test['expected_preserved']:
            if expected_preservation.upper() not in result.upper():
                print(f"  ❌ FAILED: '{expected_preservation}' should be preserved but is missing")
                preserved_ok = False
                all_passed = False

        if preserved_ok:
            print(f"  ✅ PASSED: All expected clauses preserved")

        # Show the result (truncated for readability)
        result_preview = result.replace('\n', ' ').replace('  ', ' ')[:100]
        print(f"  Result preview: {result_preview}...")
        print()

    print("=" * 80)
    if all_passed:
        print("✅ ALL TESTS PASSED!")
    else:
        print("❌ SOME TESTS FAILED")
    print("=" * 80)
    print()

    return all_passed


def demo_filter_preservation():
    """Demonstrate that filters are preserved while LIMIT/OFFSET are removed."""

    print("=" * 80)
    print("DEMONSTRATION: Filter Preservation")
    print("=" * 80)
    print()

    # Simulate a query that would come from the sidebar_components after filters are applied
    original_query = """
        WITH BillFirstSubmission AS (
            SELECT
                bi.BillID,
                MIN(
                    CASE
                        WHEN bi.LastUpdatedDate IS NOT NULL
                        THEN bi.LastUpdatedDate
                        ELSE '9999-12-31'
                    END
                ) as FirstSubmitDate
            FROM KNS_BillInitiator bi
            GROUP BY bi.BillID
        )
        SELECT
            b.BillID,
            b.Name,
            b.KnessetNum,
            bfs.FirstSubmitDate,
            f.FactionName
        FROM KNS_Bill b
        LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
        LEFT JOIN KNS_PersonToPosition ptp ON b.ProposerID = ptp.PersonID
            AND b.KnessetNum = ptp.KnessetNum
        LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
        WHERE b.KnessetNum IN (25) AND ptp.FactionID IN (123)
        ORDER BY bfs.FirstSubmitDate DESC
        OFFSET 2000 LIMIT 1000
    """

    print("ORIGINAL QUERY (with pagination):")
    print("-" * 80)
    print(original_query.strip())
    print()

    modified_query = remove_limit_offset_from_query(original_query)

    print("MODIFIED QUERY (for full download):")
    print("-" * 80)
    print(modified_query)
    print()

    print("VERIFICATION:")
    print("-" * 80)
    print(f"  Contains 'WHERE b.KnessetNum IN (25)': {'✅' if 'WHERE b.KnessetNum IN (25)' in modified_query else '❌'}")
    print(f"  Contains 'ptp.FactionID IN (123)': {'✅' if 'ptp.FactionID IN (123)' in modified_query else '❌'}")
    print(f"  Contains 'ORDER BY': {'✅' if 'ORDER BY' in modified_query else '❌'}")
    print(f"  Contains 'LIMIT': {'❌' if 'LIMIT' not in modified_query.upper() else '✅ ERROR'}")
    print(f"  Contains 'OFFSET': {'❌' if 'OFFSET' not in modified_query.upper() else '✅ ERROR'}")
    print()
    print("=" * 80)
    print()


def main():
    """Run all tests and demonstrations."""

    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "FULL DATASET DOWNLOAD - TEST SUITE" + " " * 24 + "║")
    print("╚" + "=" * 78 + "╝")
    print("\n")

    # Run tests
    tests_passed = test_query_modification()

    # Run demonstration
    demo_filter_preservation()

    # Summary
    if tests_passed:
        print("✅ Implementation verified successfully!")
        print("   - SQL query modification works correctly")
        print("   - Filters are preserved")
        print("   - LIMIT and OFFSET are removed")
        print("   - Ready for production use")
    else:
        print("⚠️  Some tests failed - review implementation")

    print()


if __name__ == "__main__":
    main()
