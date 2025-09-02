"""
Date validation utilities for ensuring chronological consistency and business rule compliance.

This module provides comprehensive validation for all calculated date fields throughout
the Knesset data processing system, ensuring dates are logically consistent and within
valid ranges.
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, date

from backend.connection_manager import get_db_connection, safe_execute_query


class DateValidator:
    """Comprehensive date validation utilities."""
    
    # Business rule constants
    ISRAEL_INDEPENDENCE_DATE = "1948-05-14"
    FIRST_KNESSET_DATE = "1949-01-25" 
    MAX_FUTURE_YEARS = 2  # Allow dates up to 2 years in the future
    
    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj
    
    def validate_bill_submission_dates(self) -> Dict[str, Any]:
        """
        Validate FirstBillSubmissionDate calculation and chronological consistency.
        
        Returns:
            Dictionary with validation results
        """
        results = {}
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                
                # Test 1: Check for impossible dates (before Israel's existence)
                query1 = f"""
                WITH BillDates AS (
                    SELECT 
                        b.BillID,
                        b.KnessetNum,
                        CAST(bi.LastUpdatedDate AS TIMESTAMP) as initiator_date,
                        CAST(cs.StartDate AS TIMESTAMP) as committee_date,
                        CAST(ps.StartDate AS TIMESTAMP) as plenum_date,
                        CAST(b.PublicationDate AS TIMESTAMP) as publication_date
                    FROM KNS_Bill b
                    LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                    LEFT JOIN KNS_CmtSessionItem csi ON b.BillID = csi.ItemID
                    LEFT JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                    LEFT JOIN KNS_PlmSessionItem psi ON b.BillID = psi.ItemID
                    LEFT JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                    WHERE b.KnessetNum IS NOT NULL
                )
                SELECT 
                    COUNT(*) as total_bills_with_dates,
                    COUNT(CASE WHEN initiator_date < '{self.FIRST_KNESSET_DATE}' THEN 1 END) as bills_with_early_initiator_dates,
                    COUNT(CASE WHEN committee_date < '{self.FIRST_KNESSET_DATE}' THEN 1 END) as bills_with_early_committee_dates,
                    COUNT(CASE WHEN plenum_date < '{self.FIRST_KNESSET_DATE}' THEN 1 END) as bills_with_early_plenum_dates,
                    COUNT(CASE WHEN publication_date < '{self.FIRST_KNESSET_DATE}' THEN 1 END) as bills_with_early_publication_dates,
                    COUNT(CASE WHEN initiator_date > CURRENT_DATE + INTERVAL '{self.MAX_FUTURE_YEARS} years' THEN 1 END) as bills_with_future_initiator_dates,
                    COUNT(CASE WHEN committee_date > CURRENT_DATE + INTERVAL '{self.MAX_FUTURE_YEARS} years' THEN 1 END) as bills_with_future_committee_dates,
                    COUNT(CASE WHEN plenum_date > CURRENT_DATE + INTERVAL '{self.MAX_FUTURE_YEARS} years' THEN 1 END) as bills_with_future_plenum_dates
                FROM BillDates
                """
                
                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    row = result1.iloc[0]
                    results['date_range_validation'] = {
                        'total_bills_with_dates': int(row['total_bills_with_dates']),
                        'bills_with_early_dates': sum([
                            int(row['bills_with_early_initiator_dates']),
                            int(row['bills_with_early_committee_dates']),
                            int(row['bills_with_early_plenum_dates']),
                            int(row['bills_with_early_publication_dates'])
                        ]),
                        'bills_with_future_dates': sum([
                            int(row['bills_with_future_initiator_dates']),
                            int(row['bills_with_future_committee_dates']),
                            int(row['bills_with_future_plenum_dates'])
                        ])
                    }
                
                # Test 2: Check chronological consistency (submission < committee < plenum)
                query2 = """
                WITH BillTimeline AS (
                    SELECT 
                        b.BillID,
                        b.KnessetNum,
                        CAST(bi.LastUpdatedDate AS TIMESTAMP) as initiator_date,
                        MIN(CAST(cs.StartDate AS TIMESTAMP)) as first_committee_date,
                        MIN(CAST(ps.StartDate AS TIMESTAMP)) as first_plenum_date
                    FROM KNS_Bill b
                    LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                    LEFT JOIN KNS_CmtSessionItem csi ON b.BillID = csi.ItemID
                    LEFT JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                    LEFT JOIN KNS_PlmSessionItem psi ON b.BillID = psi.ItemID
                    LEFT JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                    WHERE b.KnessetNum IS NOT NULL
                    GROUP BY b.BillID, b.KnessetNum, bi.LastUpdatedDate
                )
                SELECT 
                    COUNT(*) as total_bills,
                    COUNT(CASE WHEN initiator_date IS NOT NULL AND first_committee_date IS NOT NULL 
                               AND first_committee_date < initiator_date THEN 1 END) as committee_before_initiator,
                    COUNT(CASE WHEN initiator_date IS NOT NULL AND first_plenum_date IS NOT NULL 
                               AND first_plenum_date < initiator_date THEN 1 END) as plenum_before_initiator,
                    COUNT(CASE WHEN first_committee_date IS NOT NULL AND first_plenum_date IS NOT NULL 
                               AND first_plenum_date < first_committee_date THEN 1 END) as plenum_before_committee,
                    COUNT(CASE WHEN initiator_date IS NOT NULL AND first_committee_date IS NOT NULL 
                               AND first_plenum_date IS NOT NULL THEN 1 END) as bills_with_full_timeline
                FROM BillTimeline
                """
                
                result2 = safe_execute_query(con, query2, self.logger)
                if not result2.empty:
                    row = result2.iloc[0]
                    results['chronological_consistency'] = {
                        'total_bills': int(row['total_bills']),
                        'committee_before_initiator': int(row['committee_before_initiator']),
                        'plenum_before_initiator': int(row['plenum_before_initiator']),
                        'plenum_before_committee': int(row['plenum_before_committee']),
                        'bills_with_full_timeline': int(row['bills_with_full_timeline'])
                    }
                
                # Test 3: Validate FirstBillSubmissionDate calculation accuracy
                query3 = """
                WITH BillFirstSubmissionRecalc AS (
                    SELECT 
                        B.BillID,
                        MIN(earliest_date) as recalculated_first_date
                    FROM KNS_Bill B
                    LEFT JOIN (
                        -- Initiator assignment dates
                        SELECT 
                            BI.BillID,
                            MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_BillInitiator BI
                        WHERE BI.LastUpdatedDate IS NOT NULL
                        GROUP BY BI.BillID
                        
                        UNION ALL
                        
                        -- Committee session dates
                        SELECT 
                            csi.ItemID as BillID,
                            MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_CmtSessionItem csi
                        JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                        WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL
                        GROUP BY csi.ItemID
                        
                        UNION ALL
                        
                        -- Plenum session dates
                        SELECT 
                            psi.ItemID as BillID,
                            MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
                        FROM KNS_PlmSessionItem psi
                        JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
                        WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL
                        GROUP BY psi.ItemID
                        
                        UNION ALL
                        
                        -- Publication dates
                        SELECT 
                            B.BillID,
                            CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
                        FROM KNS_Bill B
                        WHERE B.PublicationDate IS NOT NULL
                    ) all_dates ON B.BillID = all_dates.BillID
                    WHERE all_dates.earliest_date IS NOT NULL
                    GROUP BY B.BillID
                    LIMIT 1000  -- Test sample
                )
                SELECT 
                    COUNT(*) as bills_tested,
                    COUNT(CASE WHEN recalculated_first_date IS NOT NULL THEN 1 END) as bills_with_calculated_dates,
                    AVG(DATE_DIFF('day', DATE '1949-01-25', CAST(recalculated_first_date AS DATE))) as avg_days_since_first_knesset
                FROM BillFirstSubmissionRecalc
                """
                
                result3 = safe_execute_query(con, query3, self.logger)
                if not result3.empty:
                    row = result3.iloc[0]
                    results['first_submission_calculation'] = {
                        'bills_tested': int(row['bills_tested']),
                        'bills_with_calculated_dates': int(row['bills_with_calculated_dates']) if row['bills_with_calculated_dates'] else 0,
                        'avg_days_since_first_knesset': float(row['avg_days_since_first_knesset']) if row['avg_days_since_first_knesset'] else 0.0
                    }
                
                results['validation_status'] = 'completed'
                results['validation_passed'] = all([
                    results.get('date_range_validation', {}).get('bills_with_early_dates', 1) == 0,
                    results.get('date_range_validation', {}).get('bills_with_future_dates', 1) == 0,
                    results.get('chronological_consistency', {}).get('committee_before_initiator', 1) < 10,  # Allow some tolerance
                    results.get('first_submission_calculation', {}).get('bills_with_calculated_dates', 0) > 0
                ])
                
        except Exception as e:
            self.logger.error(f"Error in bill submission date validation: {e}", exc_info=True)
            results['validation_status'] = 'error'
            results['error_message'] = str(e)
            results['validation_passed'] = False
        
        return results
    
    def validate_knesset_date_consistency(self) -> Dict[str, Any]:
        """
        Validate that dates align with Knesset term boundaries.
        
        Returns:
            Dictionary with validation results
        """
        results = {}
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                
                # Test 1: Check if dates align with Knesset terms
                query1 = """
                WITH KnessetDates AS (
                    SELECT 
                        KnessetNum,
                        CAST(StartDate AS TIMESTAMP) as start_date,
                        CAST(FinishDate AS TIMESTAMP) as finish_date
                    FROM KNS_KnessetDates
                    WHERE StartDate IS NOT NULL
                ),
                BillDateValidation AS (
                    SELECT 
                        b.BillID,
                        b.KnessetNum as bill_knesset,
                        CAST(bi.LastUpdatedDate AS TIMESTAMP) as bill_date,
                        kd.KnessetNum as date_knesset,
                        kd.start_date,
                        kd.finish_date,
                        CASE 
                            WHEN CAST(bi.LastUpdatedDate AS TIMESTAMP) >= kd.start_date 
                                AND (kd.finish_date IS NULL OR CAST(bi.LastUpdatedDate AS TIMESTAMP) <= kd.finish_date)
                            THEN 1 ELSE 0 
                        END as date_in_range
                    FROM KNS_Bill b
                    LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                    LEFT JOIN KnessetDates kd ON b.KnessetNum = kd.KnessetNum
                    WHERE bi.LastUpdatedDate IS NOT NULL 
                        AND kd.start_date IS NOT NULL
                )
                SELECT 
                    COUNT(*) as total_bills_with_dates,
                    SUM(date_in_range) as bills_with_dates_in_range,
                    COUNT(*) - SUM(date_in_range) as bills_with_dates_out_of_range,
                    ROUND(100.0 * SUM(date_in_range) / COUNT(*), 1) as date_accuracy_percentage
                FROM BillDateValidation
                """
                
                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    row = result1.iloc[0]
                    results['knesset_date_alignment'] = {
                        'total_bills_with_dates': int(row['total_bills_with_dates']),
                        'bills_with_dates_in_range': int(row['bills_with_dates_in_range']),
                        'bills_with_dates_out_of_range': int(row['bills_with_dates_out_of_range']),
                        'date_accuracy_percentage': float(row['date_accuracy_percentage'])
                    }
                
                # Test 2: Check for missing Knesset date boundaries
                query2 = """
                SELECT 
                    KnessetNum,
                    StartDate,
                    FinishDate,
                    CASE WHEN StartDate IS NULL THEN 1 ELSE 0 END as missing_start,
                    CASE WHEN FinishDate IS NULL AND KnessetNum < 25 THEN 1 ELSE 0 END as missing_finish
                FROM KNS_KnessetDates
                WHERE KnessetNum IS NOT NULL
                ORDER BY KnessetNum DESC
                """
                
                result2 = safe_execute_query(con, query2, self.logger)
                if not result2.empty:
                    results['knesset_boundary_completeness'] = {
                        'total_knessets': len(result2),
                        'knessets_missing_start_date': int(result2['missing_start'].sum()),
                        'knessets_missing_finish_date': int(result2['missing_finish'].sum()),
                        'knesset_date_data': result2.to_dict('records')[:10]  # Sample for review
                    }
                
                results['validation_status'] = 'completed'
                results['validation_passed'] = all([
                    results.get('knesset_date_alignment', {}).get('date_accuracy_percentage', 0) > 90.0,
                    results.get('knesset_boundary_completeness', {}).get('knessets_missing_start_date', 1) == 0
                ])
                
        except Exception as e:
            self.logger.error(f"Error in Knesset date consistency validation: {e}", exc_info=True)
            results['validation_status'] = 'error'
            results['error_message'] = str(e)
            results['validation_passed'] = False
        
        return results
    
    def validate_session_date_consistency(self) -> Dict[str, Any]:
        """
        Validate committee and plenum session date consistency.
        
        Returns:
            Dictionary with validation results
        """
        results = {}
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                
                # Test 1: Check session date ranges
                query1 = f"""
                SELECT 
                    'Committee Sessions' as session_type,
                    COUNT(*) as total_sessions,
                    COUNT(CASE WHEN CAST(StartDate AS TIMESTAMP) < '{self.FIRST_KNESSET_DATE}' THEN 1 END) as sessions_before_first_knesset,
                    COUNT(CASE WHEN CAST(StartDate AS TIMESTAMP) > CURRENT_DATE + INTERVAL '{self.MAX_FUTURE_YEARS} years' THEN 1 END) as sessions_too_far_future,
                    COUNT(CASE WHEN FinishDate IS NOT NULL AND CAST(FinishDate AS TIMESTAMP) < CAST(StartDate AS TIMESTAMP) THEN 1 END) as sessions_finish_before_start
                FROM KNS_CommitteeSession
                WHERE StartDate IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    'Plenum Sessions' as session_type,
                    COUNT(*) as total_sessions,
                    COUNT(CASE WHEN CAST(StartDate AS TIMESTAMP) < '{self.FIRST_KNESSET_DATE}' THEN 1 END) as sessions_before_first_knesset,
                    COUNT(CASE WHEN CAST(StartDate AS TIMESTAMP) > CURRENT_DATE + INTERVAL '{self.MAX_FUTURE_YEARS} years' THEN 1 END) as sessions_too_far_future,
                    COUNT(CASE WHEN FinishDate IS NOT NULL AND CAST(FinishDate AS TIMESTAMP) < CAST(StartDate AS TIMESTAMP) THEN 1 END) as sessions_finish_before_start
                FROM KNS_PlenumSession
                WHERE StartDate IS NOT NULL
                """
                
                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    results['session_date_validation'] = result1.to_dict('records')
                
                # Test 2: Check session duration consistency
                query2 = """
                WITH SessionDurations AS (
                    SELECT 
                        'Committee' as session_type,
                        CommitteeSessionID as session_id,
                        DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) as duration_hours
                    FROM KNS_CommitteeSession
                    WHERE StartDate IS NOT NULL AND FinishDate IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT 
                        'Plenum' as session_type,
                        PlenumSessionID as session_id,
                        DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) as duration_hours
                    FROM KNS_PlenumSession
                    WHERE StartDate IS NOT NULL AND FinishDate IS NOT NULL
                )
                SELECT 
                    session_type,
                    COUNT(*) as sessions_with_duration,
                    AVG(duration_hours) as avg_duration_hours,
                    MAX(duration_hours) as max_duration_hours,
                    COUNT(CASE WHEN duration_hours < 0 THEN 1 END) as sessions_with_negative_duration,
                    COUNT(CASE WHEN duration_hours > 24 THEN 1 END) as sessions_longer_than_day
                FROM SessionDurations
                GROUP BY session_type
                """
                
                result2 = safe_execute_query(con, query2, self.logger)
                if not result2.empty:
                    results['session_duration_validation'] = result2.to_dict('records')
                
                results['validation_status'] = 'completed'
                # Check if any sessions have negative durations or are before first Knesset
                validation_issues = []
                for session_data in results.get('session_date_validation', []):
                    validation_issues.extend([
                        session_data.get('sessions_before_first_knesset', 0),
                        session_data.get('sessions_finish_before_start', 0)
                    ])
                for duration_data in results.get('session_duration_validation', []):
                    validation_issues.append(duration_data.get('sessions_with_negative_duration', 0))
                
                results['validation_passed'] = all(issue == 0 for issue in validation_issues)
                
        except Exception as e:
            self.logger.error(f"Error in session date consistency validation: {e}", exc_info=True)
            results['validation_status'] = 'error'
            results['error_message'] = str(e)
            results['validation_passed'] = False
        
        return results
    
    def get_date_correction_suggestions(self) -> Dict[str, str]:
        """
        Generate queries to suggest corrections for date issues.
        
        Returns:
            Dictionary of correction suggestion queries
        """
        return {
            "bills_with_early_dates": f"""
                -- Bills with dates before first Knesset (likely data errors)
                SELECT 
                    b.BillID,
                    b.KnessetNum,
                    b.Name as BillName,
                    CAST(bi.LastUpdatedDate AS TIMESTAMP) as problematic_date,
                    'Before first Knesset ({self.FIRST_KNESSET_DATE})' as issue
                FROM KNS_Bill b
                JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                WHERE CAST(bi.LastUpdatedDate AS TIMESTAMP) < '{self.FIRST_KNESSET_DATE}'
                ORDER BY bi.LastUpdatedDate
                LIMIT 20
            """,
            
            "chronologically_inconsistent_bills": """
                -- Bills where committee sessions appear before bill initiation
                SELECT 
                    b.BillID,
                    b.KnessetNum,
                    b.Name as BillName,
                    CAST(bi.LastUpdatedDate AS TIMESTAMP) as initiator_date,
                    MIN(CAST(cs.StartDate AS TIMESTAMP)) as first_committee_date,
                    DATE_DIFF('day', MIN(CAST(cs.StartDate AS TIMESTAMP)), CAST(bi.LastUpdatedDate AS TIMESTAMP)) as days_difference
                FROM KNS_Bill b
                JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID AND bi.Ordinal = 1
                JOIN KNS_CmtSessionItem csi ON b.BillID = csi.ItemID
                JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
                WHERE bi.LastUpdatedDate IS NOT NULL 
                    AND cs.StartDate IS NOT NULL
                GROUP BY b.BillID, b.KnessetNum, b.Name, bi.LastUpdatedDate
                HAVING MIN(CAST(cs.StartDate AS TIMESTAMP)) < CAST(bi.LastUpdatedDate AS TIMESTAMP)
                ORDER BY days_difference DESC
                LIMIT 20
            """,
            
            "sessions_with_duration_issues": """
                -- Sessions with impossible durations
                SELECT 
                    'Committee' as session_type,
                    CommitteeSessionID as session_id,
                    StartDate,
                    FinishDate,
                    DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) as duration_hours,
                    CASE 
                        WHEN DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) < 0 
                        THEN 'Finish before start'
                        WHEN DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) > 24 
                        THEN 'Duration over 24 hours'
                    END as issue
                FROM KNS_CommitteeSession
                WHERE StartDate IS NOT NULL 
                    AND FinishDate IS NOT NULL
                    AND (CAST(FinishDate AS TIMESTAMP) < CAST(StartDate AS TIMESTAMP)
                         OR DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) > 24)
                
                UNION ALL
                
                SELECT 
                    'Plenum' as session_type,
                    CAST(PlenumSessionID AS VARCHAR) as session_id,
                    StartDate,
                    FinishDate,
                    DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) as duration_hours,
                    CASE 
                        WHEN DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) < 0 
                        THEN 'Finish before start'
                        WHEN DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) > 24 
                        THEN 'Duration over 24 hours'
                    END as issue
                FROM KNS_PlenumSession
                WHERE StartDate IS NOT NULL 
                    AND FinishDate IS NOT NULL
                    AND (CAST(FinishDate AS TIMESTAMP) < CAST(StartDate AS TIMESTAMP)
                         OR DATE_DIFF('hour', CAST(StartDate AS TIMESTAMP), CAST(FinishDate AS TIMESTAMP)) > 24)
                ORDER BY duration_hours DESC
                LIMIT 20
            """
        }
    
    def run_comprehensive_date_validation(self) -> Dict[str, Any]:
        """
        Run all date validation tests.
        
        Returns:
            Dictionary with all validation results
        """
        self.logger.info("Starting comprehensive date validation...")
        
        validation_results = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'database_path': str(self.db_path)
        }
        
        # Run all validation tests
        validation_results['bill_submission_validation'] = self.validate_bill_submission_dates()
        validation_results['knesset_date_validation'] = self.validate_knesset_date_consistency()
        validation_results['session_date_validation'] = self.validate_session_date_consistency()
        
        # Overall assessment
        all_passed = all([
            validation_results['bill_submission_validation'].get('validation_passed', False),
            validation_results['knesset_date_validation'].get('validation_passed', False),
            validation_results['session_date_validation'].get('validation_passed', False)
        ])
        
        validation_results['overall_validation_passed'] = all_passed
        validation_results['summary'] = {
            'tests_run': 3,
            'tests_passed': sum([
                validation_results['bill_submission_validation'].get('validation_passed', False),
                validation_results['knesset_date_validation'].get('validation_passed', False),
                validation_results['session_date_validation'].get('validation_passed', False)
            ]),
            'date_issues': []
        }
        
        # Collect date issues
        if not validation_results['bill_submission_validation'].get('validation_passed', False):
            validation_results['summary']['date_issues'].append('Bill submission date inconsistencies detected')
            
        if not validation_results['knesset_date_validation'].get('validation_passed', False):
            validation_results['summary']['date_issues'].append('Knesset date alignment issues detected')
            
        if not validation_results['session_date_validation'].get('validation_passed', False):
            validation_results['summary']['date_issues'].append('Session date consistency problems detected')
        
        self.logger.info(f"Date validation completed. Overall status: {'PASS' if all_passed else 'FAIL'}")
        
        return validation_results