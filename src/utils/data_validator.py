"""
Data validation utilities for ensuring calculation accuracy and consistency.

This module provides validation functions to check the correctness of complex
calculations throughout the Knesset data processing system.
"""

import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from backend.connection_manager import get_db_connection, safe_execute_query


class DataValidator:
    """Comprehensive data validation utilities."""

    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj

    def validate_bill_initiator_counts(self) -> Dict[str, Any]:
        """
        Validate bill initiator counting logic for consistency.

        Returns:
            Dictionary with validation results
        """
        results: Dict[str, Any] = {}

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:

                # Test 1: Check for bills with no primary initiators
                query1 = """
                SELECT
                    COUNT(*) as bills_without_primary_initiators
                FROM KNS_Bill b
                LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                WHERE bi.BillID IS NULL
                """

                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    results["bills_without_primary_initiators"] = int(
                        result1.iloc[0]["bills_without_primary_initiators"]
                    )

                # Test 2: Check for bills with multiple primary initiators (Ordinal = 1)
                query2 = """
                SELECT
                    COUNT(*) as bills_with_multiple_primaries
                FROM (
                    SELECT
                        BillID,
                        COUNT(*) as primary_count
                    FROM KNS_BillInitiator
                    WHERE Ordinal = 1
                    GROUP BY BillID
                    HAVING COUNT(*) > 1
                )
                """

                result2 = safe_execute_query(con, query2, self.logger)
                if not result2.empty:
                    results["bills_with_multiple_primaries"] = int(result2.iloc[0]["bills_with_multiple_primaries"])

                # Test 3: Validate total member count calculation
                query3 = """
                SELECT
                    b.BillID,
                    COUNT(DISTINCT bi.PersonID) as actual_total_members,
                    COUNT(DISTINCT CASE WHEN bi.Ordinal = 1 THEN bi.PersonID END) as actual_main_count,
                    COUNT(DISTINCT CASE WHEN bi.Ordinal > 1 OR bi.IsInitiator IS NULL THEN bi.PersonID END) as actual_supporting_count
                FROM KNS_Bill b
                LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                GROUP BY b.BillID
                HAVING COUNT(DISTINCT bi.PersonID) !=
                    COUNT(DISTINCT CASE WHEN bi.Ordinal = 1 THEN bi.PersonID END) +
                    COUNT(DISTINCT CASE WHEN bi.Ordinal > 1 OR bi.IsInitiator IS NULL THEN bi.PersonID END)
                LIMIT 10
                """

                result3 = safe_execute_query(con, query3, self.logger)
                results["bills_with_counting_inconsistencies"] = len(result3)
                if len(result3) > 0:
                    results["sample_inconsistent_bills"] = result3.to_dict("records")

                # Test 4: Check coalition/opposition percentage calculation accuracy
                query4 = """
                WITH TestCalculation AS (
                    SELECT
                        b.BillID,
                        COUNT(DISTINCT bi.PersonID) as total_members,
                        COUNT(DISTINCT CASE WHEN ufs.CoalitionStatus = 'Coalition' THEN bi.PersonID END) as coalition_members,
                        COUNT(DISTINCT CASE WHEN ufs.CoalitionStatus = 'Opposition' THEN bi.PersonID END) as opposition_members,
                        CASE
                            WHEN COUNT(DISTINCT bi.PersonID) > 0 THEN
                                ROUND((COUNT(DISTINCT CASE WHEN ufs.CoalitionStatus = 'Coalition' THEN bi.PersonID END) * 100.0)
                                      / COUNT(DISTINCT bi.PersonID), 1)
                            ELSE 0.0
                        END as calculated_coalition_pct
                    FROM KNS_Bill b
                    LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                    LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID AND b.KnessetNum = ptp.KnessetNum
                    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID AND b.KnessetNum = ufs.KnessetNum
                    WHERE b.KnessetNum = 25 -- Test with recent data
                    GROUP BY b.BillID
                    HAVING COUNT(DISTINCT bi.PersonID) > 0
                    LIMIT 5
                )
                SELECT
                    *,
                    (coalition_members + opposition_members) as accounted_members,
                    (total_members - coalition_members - opposition_members) as unaccounted_members
                FROM TestCalculation
                WHERE coalition_members + opposition_members < total_members
                """

                result4 = safe_execute_query(con, query4, self.logger)
                results["bills_with_incomplete_coalition_data"] = len(result4)
                if len(result4) > 0:
                    results["sample_coalition_issues"] = result4.to_dict("records")

                # Test 5: Validate that supporting member counts are consistent
                query5 = """
                SELECT
                    COUNT(*) as bills_with_supporting_count_errors
                FROM (
                    SELECT
                        b.BillID,
                        COUNT(DISTINCT CASE WHEN bi.Ordinal > 1 OR bi.IsInitiator IS NULL THEN bi.PersonID END) as supporting_count,
                        COUNT(DISTINCT CASE WHEN bi.Ordinal > 1 THEN bi.PersonID END) as ordinal_based_count
                    FROM KNS_Bill b
                    LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                    GROUP BY b.BillID
                    HAVING supporting_count != ordinal_based_count
                        AND ordinal_based_count > 0 -- Only check bills that have supporting members
                )
                """

                result5 = safe_execute_query(con, query5, self.logger)
                if not result5.empty:
                    results["bills_with_supporting_count_errors"] = int(
                        result5.iloc[0]["bills_with_supporting_count_errors"]
                    )

                results["validation_status"] = "completed"
                results["validation_passed"] = all(
                    [
                        results.get("bills_without_primary_initiators", 0) == 0,
                        results.get("bills_with_multiple_primaries", 0) == 0,
                        results.get("bills_with_counting_inconsistencies", 0) == 0,
                        results.get("bills_with_supporting_count_errors", 0) == 0,
                    ]
                )

        except Exception as e:
            self.logger.error(f"Error in bill initiator validation: {e}", exc_info=True)
            results["validation_status"] = "error"
            results["error_message"] = str(e)
            results["validation_passed"] = False

        return results

    def validate_committee_join_consistency(self) -> Dict[str, Any]:
        """
        Validate committee join operations and data type consistency.

        Returns:
            Dictionary with validation results
        """
        results: Dict[str, Any] = {}

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:

                # Test 1: Check data type consistency for CommitteeID fields
                query1 = """
                SELECT
                    COUNT(*) as bills_with_committee_id,
                    COUNT(CASE WHEN c.CommitteeID IS NOT NULL THEN 1 END) as successful_joins,
                    ROUND(100.0 * COUNT(CASE WHEN c.CommitteeID IS NOT NULL THEN 1 END) / COUNT(*), 1) as join_success_rate
                FROM KNS_Bill b
                LEFT JOIN KNS_Committee c ON CAST(b.CommitteeID AS BIGINT) = c.CommitteeID
                WHERE b.CommitteeID IS NOT NULL
                """

                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    row = result1.iloc[0]
                    results["committee_join_stats"] = {
                        "bills_with_committee_id": int(row["bills_with_committee_id"]),
                        "successful_joins": int(row["successful_joins"]),
                        "join_success_rate": float(row["join_success_rate"]),
                    }

                # Test 2: Check for data type mismatches
                query2 = """
                SELECT
                    b.BillID,
                    b.CommitteeID as bill_committee_id,
                    c.CommitteeID as committee_table_id,
                    TYPEOF(b.CommitteeID) as bill_committee_type,
                    TYPEOF(c.CommitteeID) as committee_table_type
                FROM KNS_Bill b
                LEFT JOIN KNS_Committee c ON CAST(b.CommitteeID AS BIGINT) = c.CommitteeID
                WHERE b.CommitteeID IS NOT NULL
                    AND c.CommitteeID IS NULL
                LIMIT 10
                """

                result2 = safe_execute_query(con, query2, self.logger)
                results["unmatched_committee_ids"] = len(result2)
                if len(result2) > 0:
                    results["sample_unmatched_committees"] = result2.to_dict("records")

                # Test 3: Check for missing committee data by Knesset
                query3 = """
                SELECT
                    b.KnessetNum,
                    COUNT(DISTINCT b.CommitteeID) as distinct_committee_ids_in_bills,
                    COUNT(DISTINCT c.CommitteeID) as distinct_committees_in_table,
                    COUNT(DISTINCT CASE WHEN c.CommitteeID IS NOT NULL THEN b.CommitteeID END) as matched_committee_ids
                FROM KNS_Bill b
                LEFT JOIN KNS_Committee c ON CAST(b.CommitteeID AS BIGINT) = c.CommitteeID
                WHERE b.CommitteeID IS NOT NULL
                GROUP BY b.KnessetNum
                ORDER BY b.KnessetNum DESC
                LIMIT 10
                """

                result3 = safe_execute_query(con, query3, self.logger)
                results["committee_coverage_by_knesset"] = result3.to_dict("records") if not result3.empty else []

                # Test 4: Check alternative join approaches
                query4 = """
                SELECT
                    'without_cast' as approach,
                    COUNT(CASE WHEN c.CommitteeID IS NOT NULL THEN 1 END) as successful_joins
                FROM KNS_Bill b
                LEFT JOIN KNS_Committee c ON b.CommitteeID = c.CommitteeID
                WHERE b.CommitteeID IS NOT NULL

                UNION ALL

                SELECT
                    'with_cast' as approach,
                    COUNT(CASE WHEN c.CommitteeID IS NOT NULL THEN 1 END) as successful_joins
                FROM KNS_Bill b
                LEFT JOIN KNS_Committee c ON CAST(b.CommitteeID AS BIGINT) = c.CommitteeID
                WHERE b.CommitteeID IS NOT NULL
                """

                result4 = safe_execute_query(con, query4, self.logger)
                results["join_approach_comparison"] = result4.to_dict("records") if not result4.empty else []

                results["validation_status"] = "completed"
                results["validation_passed"] = (
                    results.get("committee_join_stats", {}).get("join_success_rate", 0) > 70.0
                )

        except Exception as e:
            self.logger.error(f"Error in committee join validation: {e}", exc_info=True)
            results["validation_status"] = "error"
            results["error_message"] = str(e)
            results["validation_passed"] = False

        return results

    def validate_date_calculations(self) -> Dict[str, Any]:
        """
        Validate date calculation logic and chronological consistency.

        Returns:
            Dictionary with validation results
        """
        results: Dict[str, Any] = {}

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:

                # Test 1: Check FirstBillSubmissionDate logic
                query1 = """
                WITH DateSources AS (
                    SELECT
                        b.BillID,
                        b.KnessetNum,
                        MIN(CAST(bi.LastUpdatedDate AS TIMESTAMP)) as earliest_initiator_date,
                        MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_committee_date,
                        MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_plenum_date,
                        CAST(b.PublicationDate AS TIMESTAMP) as publication_date,
                        COALESCE(
                            MIN(CAST(bi.LastUpdatedDate AS TIMESTAMP)),
                            MIN(CAST(cs.StartDate AS TIMESTAMP)),
                            MIN(CAST(ps.StartDate AS TIMESTAMP)),
                            CAST(b.PublicationDate AS TIMESTAMP)
                        ) as calculated_first_date
                    FROM KNS_Bill b
                    LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                    LEFT JOIN KNS_CmtSessionItem csi ON b.BillID = csi.ItemID
                    LEFT JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                    LEFT JOIN KNS_PlmSessionItem psi ON b.BillID = psi.ItemID
                    LEFT JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                    WHERE b.KnessetNum IS NOT NULL
                    GROUP BY b.BillID, b.KnessetNum, b.PublicationDate
                    LIMIT 100
                ),
                ChronologyTest AS (
                    SELECT
                        *,
                        CASE
                            WHEN earliest_committee_date IS NOT NULL AND earliest_plenum_date IS NOT NULL
                                AND earliest_committee_date > earliest_plenum_date THEN 1
                            ELSE 0
                        END as committee_after_plenum_flag,
                        CASE
                            WHEN calculated_first_date < '1948-01-01' OR calculated_first_date > CURRENT_DATE THEN 1
                            ELSE 0
                        END as invalid_date_flag
                    FROM DateSources
                )
                SELECT
                    COUNT(*) as total_bills_tested,
                    SUM(committee_after_plenum_flag) as bills_with_committee_after_plenum,
                    SUM(invalid_date_flag) as bills_with_invalid_dates,
                    COUNT(CASE WHEN calculated_first_date IS NOT NULL THEN 1 END) as bills_with_calculated_dates
                FROM ChronologyTest
                """

                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    row = result1.iloc[0]
                    results["date_validation_stats"] = {
                        "total_bills_tested": int(row["total_bills_tested"]),
                        "bills_with_committee_after_plenum": int(row["bills_with_committee_after_plenum"]),
                        "bills_with_invalid_dates": int(row["bills_with_invalid_dates"]),
                        "bills_with_calculated_dates": int(row["bills_with_calculated_dates"]),
                    }

                # Test 2: Check for temporal inconsistencies
                query2 = """
                SELECT
                    b.BillID,
                    b.KnessetNum,
                    CAST(bi.LastUpdatedDate AS TIMESTAMP) as initiator_date,
                    CAST(cs.StartDate AS TIMESTAMP) as committee_date,
                    CAST(ps.StartDate AS TIMESTAMP) as plenum_date
                FROM KNS_Bill b
                LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                LEFT JOIN KNS_CmtSessionItem csi ON b.BillID = csi.ItemID
                LEFT JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                LEFT JOIN KNS_PlmSessionItem psi ON b.BillID = psi.ItemID
                LEFT JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                WHERE bi.LastUpdatedDate IS NOT NULL
                    AND cs.StartDate IS NOT NULL
                    AND ps.StartDate IS NOT NULL
                    AND (
                        CAST(cs.StartDate AS TIMESTAMP) < CAST(bi.LastUpdatedDate AS TIMESTAMP)
                        OR CAST(ps.StartDate AS TIMESTAMP) < CAST(bi.LastUpdatedDate AS TIMESTAMP)
                        OR CAST(ps.StartDate AS TIMESTAMP) < CAST(cs.StartDate AS TIMESTAMP)
                    )
                LIMIT 10
                """

                result2 = safe_execute_query(con, query2, self.logger)
                results["temporal_inconsistencies"] = len(result2)
                if len(result2) > 0:
                    results["sample_temporal_issues"] = result2.to_dict("records")

                results["validation_status"] = "completed"
                results["validation_passed"] = all(
                    [
                        results.get("date_validation_stats", {}).get("bills_with_invalid_dates", 0) == 0,
                        results.get("temporal_inconsistencies", 0) < 5,  # Allow some tolerance
                    ]
                )

        except Exception as e:
            self.logger.error(f"Error in date calculation validation: {e}", exc_info=True)
            results["validation_status"] = "error"
            results["error_message"] = str(e)
            results["validation_passed"] = False

        return results

    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """
        Run all validation tests and return comprehensive results.

        Returns:
            Dictionary with all validation results
        """
        self.logger.info("Starting comprehensive data validation...")

        validation_results = {"timestamp": pd.Timestamp.now().isoformat(), "database_path": str(self.db_path)}

        # Run all validation tests
        validation_results["bill_initiator_validation"] = self.validate_bill_initiator_counts()
        validation_results["committee_join_validation"] = self.validate_committee_join_consistency()
        validation_results["date_calculation_validation"] = self.validate_date_calculations()

        # Overall assessment
        all_passed = all(
            [
                validation_results["bill_initiator_validation"].get("validation_passed", False),
                validation_results["committee_join_validation"].get("validation_passed", False),
                validation_results["date_calculation_validation"].get("validation_passed", False),
            ]
        )

        validation_results["overall_validation_passed"] = all_passed
        validation_results["summary"] = {
            "tests_run": 3,
            "tests_passed": sum(
                [
                    validation_results["bill_initiator_validation"].get("validation_passed", False),
                    validation_results["committee_join_validation"].get("validation_passed", False),
                    validation_results["date_calculation_validation"].get("validation_passed", False),
                ]
            ),
            "critical_issues": [],
        }

        # Collect critical issues
        if not validation_results["bill_initiator_validation"].get("validation_passed", False):
            validation_results["summary"]["critical_issues"].append("Bill initiator counting inconsistencies detected")

        if not validation_results["committee_join_validation"].get("validation_passed", False):
            validation_results["summary"]["critical_issues"].append("Committee join issues detected")

        if not validation_results["date_calculation_validation"].get("validation_passed", False):
            validation_results["summary"]["critical_issues"].append("Date calculation inconsistencies detected")

        self.logger.info(f"Data validation completed. Overall status: {'PASS' if all_passed else 'FAIL'}")

        return validation_results
