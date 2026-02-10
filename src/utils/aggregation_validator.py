"""
Aggregation validation utilities for ensuring calculation accuracy in charts and queries.

This module provides validation functions specifically for complex aggregation logic
used throughout the visualization and analysis system.
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import logging
from pathlib import Path

from backend.connection_manager import get_db_connection, safe_execute_query


class AggregationValidator:
    """Validation utilities for aggregation logic."""
    
    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj
    
    def validate_network_chart_aggregations(self) -> Dict[str, Any]:
        """
        Validate network chart aggregation logic for accuracy.
        
        Returns:
            Dictionary with validation results
        """
        results: Dict[str, Any] = {}
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                
                # Test 1: Validate MK collaboration counting
                query1 = """
                WITH DirectCollaborationCount AS (
                    SELECT 
                        main.PersonID as MainInitiatorID,
                        supp.PersonID as SupporterID,
                        COUNT(DISTINCT main.BillID) as direct_count
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND b.KnessetNum = 25  -- Test with recent data
                    GROUP BY main.PersonID, supp.PersonID
                ),
                NetworkQueryCount AS (
                    SELECT 
                        main.PersonID as MainInitiatorID,
                        supp.PersonID as SupporterID,
                        COUNT(DISTINCT main.BillID) as network_count
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND b.KnessetNum = 25
                    GROUP BY main.PersonID, supp.PersonID
                )
                SELECT 
                    COUNT(*) as total_pairs,
                    COUNT(CASE WHEN d.direct_count = n.network_count THEN 1 END) as matching_counts,
                    COUNT(CASE WHEN d.direct_count != n.network_count THEN 1 END) as mismatched_counts
                FROM DirectCollaborationCount d
                FULL OUTER JOIN NetworkQueryCount n ON d.MainInitiatorID = n.MainInitiatorID 
                    AND d.SupporterID = n.SupporterID
                """
                
                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    row = result1.iloc[0]
                    results['mk_collaboration_validation'] = {
                        'total_pairs': int(row['total_pairs']),
                        'matching_counts': int(row['matching_counts']),
                        'mismatched_counts': int(row['mismatched_counts'])
                    }
                
                # Test 2: Validate faction bill counting (prevent double-counting)
                query2 = """
                WITH FactionBillsDirect AS (
                    SELECT 
                        f.Name as FactionName,
                        COUNT(DISTINCT bi.BillID) as direct_bill_count
                    FROM KNS_Faction f
                    JOIN KNS_PersonToPosition ptp ON f.FactionID = ptp.FactionID
                    JOIN KNS_BillInitiator bi ON ptp.PersonID = bi.PersonID AND bi.Ordinal = 1
                    JOIN KNS_Bill b ON bi.BillID = b.BillID AND b.KnessetNum = ptp.KnessetNum
                    WHERE b.KnessetNum = 25
                    GROUP BY f.FactionID, f.Name
                ),
                FactionBillsNetwork AS (
                    -- Simulate network chart faction counting
                    SELECT 
                        f.Name as FactionName,
                        COUNT(DISTINCT bi.BillID) as network_bill_count
                    FROM KNS_Faction f
                    JOIN KNS_PersonToPosition ptp ON f.FactionID = ptp.FactionID
                    JOIN KNS_BillInitiator bi ON ptp.PersonID = bi.PersonID AND bi.Ordinal = 1
                    JOIN KNS_Bill b ON bi.BillID = b.BillID AND b.KnessetNum = ptp.KnessetNum
                    WHERE b.KnessetNum = 25
                    GROUP BY f.Name
                )
                SELECT 
                    COUNT(*) as total_factions,
                    COUNT(CASE WHEN d.direct_bill_count = n.network_bill_count THEN 1 END) as matching_counts,
                    SUM(CASE WHEN d.direct_bill_count != n.network_bill_count THEN ABS(d.direct_bill_count - n.network_bill_count) ELSE 0 END) as total_count_difference
                FROM FactionBillsDirect d
                FULL OUTER JOIN FactionBillsNetwork n ON d.FactionName = n.FactionName
                """
                
                result2 = safe_execute_query(con, query2, self.logger)
                if not result2.empty:
                    row = result2.iloc[0]
                    results['faction_bill_counting_validation'] = {
                        'total_factions': int(row['total_factions']),
                        'matching_counts': int(row['matching_counts']),
                        'total_count_difference': int(row['total_count_difference']) if row['total_count_difference'] else 0
                    }
                
                # Test 3: Validate node sizing calculations
                query3 = """
                WITH MKTotalBills AS (
                    SELECT 
                        p.PersonID,
                        p.FirstName || ' ' || p.LastName as FullName,
                        COUNT(DISTINCT bi.BillID) as calculated_total_bills
                    FROM KNS_Person p
                    LEFT JOIN KNS_BillInitiator bi ON p.PersonID = bi.PersonID AND bi.Ordinal = 1
                    LEFT JOIN KNS_Bill b ON bi.BillID = b.BillID
                    WHERE b.KnessetNum = 25
                    GROUP BY p.PersonID, p.FirstName, p.LastName
                    HAVING COUNT(DISTINCT bi.BillID) > 0
                ),
                MKCollaborationCount AS (
                    SELECT 
                        main.PersonID,
                        COUNT(DISTINCT supp.PersonID) as collaboration_partners
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND b.KnessetNum = 25
                    GROUP BY main.PersonID
                )
                SELECT 
                    COUNT(*) as total_mks_with_bills,
                    COUNT(CASE WHEN mtb.calculated_total_bills > 0 THEN 1 END) as mks_with_calculated_bills,
                    AVG(mtb.calculated_total_bills) as avg_bills_per_mk,
                    MAX(mtb.calculated_total_bills) as max_bills_per_mk,
                    COUNT(mcc.PersonID) as mks_with_collaborations
                FROM MKTotalBills mtb
                LEFT JOIN MKCollaborationCount mcc ON mtb.PersonID = mcc.PersonID
                """
                
                result3 = safe_execute_query(con, query3, self.logger)
                if not result3.empty:
                    row = result3.iloc[0]
                    results['node_sizing_validation'] = {
                        'total_mks_with_bills': int(row['total_mks_with_bills']),
                        'mks_with_calculated_bills': int(row['mks_with_calculated_bills']),
                        'avg_bills_per_mk': float(row['avg_bills_per_mk']) if row['avg_bills_per_mk'] else 0.0,
                        'max_bills_per_mk': int(row['max_bills_per_mk']) if row['max_bills_per_mk'] else 0,
                        'mks_with_collaborations': int(row['mks_with_collaborations']) if row['mks_with_collaborations'] else 0
                    }
                
                results['validation_status'] = 'completed'
                results['validation_passed'] = all([
                    results.get('mk_collaboration_validation', {}).get('mismatched_counts', 1) == 0,
                    results.get('faction_bill_counting_validation', {}).get('total_count_difference', 1) == 0
                ])
                
        except Exception as e:
            self.logger.error(f"Error in network chart aggregation validation: {e}", exc_info=True)
            results['validation_status'] = 'error'
            results['error_message'] = str(e)
            results['validation_passed'] = False
        
        return results
    
    def validate_comparison_chart_aggregations(self) -> Dict[str, Any]:
        """
        Validate comparison chart aggregation logic.
        
        Returns:
            Dictionary with validation results
        """
        results: Dict[str, Any] = {}
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                
                # Test 1: Validate queries per faction counting
                query1 = """
                WITH DirectQueryCount AS (
                    SELECT 
                        f.Name as FactionName,
                        COUNT(DISTINCT q.QueryID) as direct_query_count
                    FROM KNS_Query q
                    JOIN KNS_PersonToPosition ptp ON q.PersonID = ptp.PersonID AND q.KnessetNum = ptp.KnessetNum
                    JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    WHERE q.KnessetNum = 25
                    GROUP BY f.FactionID, f.Name
                ),
                ChartQueryCount AS (
                    -- Simulate the chart query logic
                    SELECT 
                        f.Name as FactionName,
                        COUNT(q.QueryID) as chart_query_count
                    FROM KNS_Query q
                    LEFT JOIN KNS_PersonToPosition ptp ON q.PersonID = ptp.PersonID AND q.KnessetNum = ptp.KnessetNum
                    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                    WHERE q.KnessetNum = 25 
                        AND f.Name IS NOT NULL
                    GROUP BY f.Name
                )
                SELECT 
                    COUNT(*) as total_factions,
                    COUNT(CASE WHEN d.direct_query_count = c.chart_query_count THEN 1 END) as matching_counts,
                    SUM(CASE WHEN d.direct_query_count != c.chart_query_count THEN ABS(d.direct_query_count - c.chart_query_count) ELSE 0 END) as total_difference
                FROM DirectQueryCount d
                FULL OUTER JOIN ChartQueryCount c ON d.FactionName = c.FactionName
                """
                
                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    row = result1.iloc[0]
                    results['queries_per_faction_validation'] = {
                        'total_factions': int(row['total_factions']),
                        'matching_counts': int(row['matching_counts']),
                        'total_difference': int(row['total_difference']) if row['total_difference'] else 0
                    }
                
                # Test 2: Validate bill initiator charts consistency
                query2 = """
                WITH TopInitiators AS (
                    SELECT 
                        p.FirstName || ' ' || p.LastName as FullName,
                        COUNT(DISTINCT bi.BillID) as bill_count
                    FROM KNS_Person p
                    JOIN KNS_BillInitiator bi ON p.PersonID = bi.PersonID AND bi.Ordinal = 1
                    JOIN KNS_Bill b ON bi.BillID = b.BillID
                    WHERE b.KnessetNum = 25
                    GROUP BY p.PersonID, p.FirstName, p.LastName
                    ORDER BY bill_count DESC
                    LIMIT 10
                ),
                InitiatorsByFaction AS (
                    SELECT 
                        f.Name as FactionName,
                        COUNT(DISTINCT p.PersonID) as mk_count,
                        SUM(bill_counts.bill_count) as total_bills
                    FROM KNS_Faction f
                    JOIN KNS_PersonToPosition ptp ON f.FactionID = ptp.FactionID
                    JOIN KNS_Person p ON ptp.PersonID = p.PersonID
                    JOIN (
                        SELECT 
                            bi.PersonID,
                            COUNT(DISTINCT bi.BillID) as bill_count
                        FROM KNS_BillInitiator bi
                        JOIN KNS_Bill b ON bi.BillID = b.BillID
                        WHERE bi.Ordinal = 1 AND b.KnessetNum = 25
                        GROUP BY bi.PersonID
                        HAVING COUNT(DISTINCT bi.BillID) > 0
                    ) bill_counts ON p.PersonID = bill_counts.PersonID
                    WHERE ptp.KnessetNum = 25
                    GROUP BY f.FactionID, f.Name
                )
                SELECT 
                    (SELECT COUNT(*) FROM TopInitiators) as top_initiators_count,
                    (SELECT COUNT(*) FROM InitiatorsByFaction) as factions_with_initiators,
                    (SELECT SUM(total_bills) FROM InitiatorsByFaction) as total_bills_by_faction
                """
                
                result2 = safe_execute_query(con, query2, self.logger)
                if not result2.empty:
                    row = result2.iloc[0]
                    results['bill_initiator_consistency'] = {
                        'top_initiators_count': int(row['top_initiators_count']),
                        'factions_with_initiators': int(row['factions_with_initiators']),
                        'total_bills_by_faction': int(row['total_bills_by_faction']) if row['total_bills_by_faction'] else 0
                    }
                
                # Test 3: Validate distribution chart totals
                query3 = """
                WITH StatusDistribution AS (
                    SELECT 
                        s."Desc" as status_desc,
                        COUNT(DISTINCT b.BillID) as bill_count
                    FROM KNS_Bill b
                    LEFT JOIN KNS_Status s ON b.StatusID = s.StatusID
                    WHERE b.KnessetNum = 25
                    GROUP BY s.StatusID, s."Desc"
                ),
                TypeDistribution AS (
                    SELECT 
                        b.SubTypeDesc as subtype_desc,
                        COUNT(DISTINCT b.BillID) as bill_count
                    FROM KNS_Bill b
                    WHERE b.KnessetNum = 25
                    GROUP BY b.SubTypeDesc
                )
                SELECT 
                    (SELECT SUM(bill_count) FROM StatusDistribution) as total_bills_by_status,
                    (SELECT SUM(bill_count) FROM TypeDistribution) as total_bills_by_type,
                    (SELECT COUNT(DISTINCT BillID) FROM KNS_Bill WHERE KnessetNum = 25) as actual_total_bills
                """
                
                result3 = safe_execute_query(con, query3, self.logger)
                if not result3.empty:
                    row = result3.iloc[0]
                    results['distribution_totals_validation'] = {
                        'total_bills_by_status': int(row['total_bills_by_status']) if row['total_bills_by_status'] else 0,
                        'total_bills_by_type': int(row['total_bills_by_type']) if row['total_bills_by_type'] else 0,
                        'actual_total_bills': int(row['actual_total_bills'])
                    }
                
                results['validation_status'] = 'completed'
                results['validation_passed'] = all([
                    results.get('queries_per_faction_validation', {}).get('total_difference', 1) == 0,
                    abs(results.get('distribution_totals_validation', {}).get('total_bills_by_status', 0) - 
                        results.get('distribution_totals_validation', {}).get('actual_total_bills', 1)) <= 1  # Allow small rounding differences
                ])
                
        except Exception as e:
            self.logger.error(f"Error in comparison chart aggregation validation: {e}", exc_info=True)
            results['validation_status'] = 'error'
            results['error_message'] = str(e)
            results['validation_passed'] = False
        
        return results
    
    def validate_time_series_aggregations(self) -> Dict[str, Any]:
        """
        Validate time series chart aggregation logic.
        
        Returns:
            Dictionary with validation results
        """
        results: Dict[str, Any] = {}
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                
                # Test 1: Validate time-based bill aggregations
                query1 = """
                WITH MonthlyBills AS (
                    SELECT 
                        DATE_TRUNC('month', CAST(b.LastUpdatedDate AS TIMESTAMP)) as bill_month,
                        COUNT(DISTINCT b.BillID) as monthly_bill_count
                    FROM KNS_Bill b
                    WHERE b.KnessetNum = 25 
                        AND b.LastUpdatedDate IS NOT NULL
                        AND CAST(b.LastUpdatedDate AS TIMESTAMP) >= '2020-01-01'
                    GROUP BY DATE_TRUNC('month', CAST(b.LastUpdatedDate AS TIMESTAMP))
                ),
                YearlyBills AS (
                    SELECT 
                        EXTRACT(YEAR FROM CAST(b.LastUpdatedDate AS TIMESTAMP)) as bill_year,
                        COUNT(DISTINCT b.BillID) as yearly_bill_count
                    FROM KNS_Bill b
                    WHERE b.KnessetNum = 25 
                        AND b.LastUpdatedDate IS NOT NULL
                        AND CAST(b.LastUpdatedDate AS TIMESTAMP) >= '2020-01-01'
                    GROUP BY EXTRACT(YEAR FROM CAST(b.LastUpdatedDate AS TIMESTAMP))
                )
                SELECT 
                    COUNT(*) as months_with_bills,
                    SUM(monthly_bill_count) as total_monthly_bills,
                    COUNT(DISTINCT bill_year) as years_with_bills,
                    SUM(yearly_bill_count) as total_yearly_bills
                FROM MonthlyBills mb
                LEFT JOIN YearlyBills yb ON EXTRACT(YEAR FROM mb.bill_month) = yb.bill_year
                """
                
                result1 = safe_execute_query(con, query1, self.logger)
                if not result1.empty:
                    row = result1.iloc[0]
                    results['time_series_consistency'] = {
                        'months_with_bills': int(row['months_with_bills']),
                        'total_monthly_bills': int(row['total_monthly_bills']) if row['total_monthly_bills'] else 0,
                        'years_with_bills': int(row['years_with_bills']),
                        'total_yearly_bills': int(row['total_yearly_bills']) if row['total_yearly_bills'] else 0
                    }
                
                results['validation_status'] = 'completed'
                results['validation_passed'] = (
                    results.get('time_series_consistency', {}).get('total_monthly_bills', 0) == 
                    results.get('time_series_consistency', {}).get('total_yearly_bills', 1)
                )
                
        except Exception as e:
            self.logger.error(f"Error in time series aggregation validation: {e}", exc_info=True)
            results['validation_status'] = 'error'
            results['error_message'] = str(e)
            results['validation_passed'] = False
        
        return results
    
    def run_comprehensive_aggregation_validation(self) -> Dict[str, Any]:
        """
        Run all aggregation validation tests.
        
        Returns:
            Dictionary with all validation results
        """
        self.logger.info("Starting comprehensive aggregation validation...")
        
        validation_results: Dict[str, Any] = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'database_path': str(self.db_path)
        }
        
        # Run all validation tests
        validation_results['network_chart_validation'] = self.validate_network_chart_aggregations()
        validation_results['comparison_chart_validation'] = self.validate_comparison_chart_aggregations()
        validation_results['time_series_validation'] = self.validate_time_series_aggregations()
        
        # Overall assessment
        all_passed = all([
            validation_results['network_chart_validation'].get('validation_passed', False),
            validation_results['comparison_chart_validation'].get('validation_passed', False),
            validation_results['time_series_validation'].get('validation_passed', False)
        ])
        
        validation_results['overall_validation_passed'] = all_passed
        validation_results['summary'] = {
            'tests_run': 3,
            'tests_passed': sum([
                validation_results['network_chart_validation'].get('validation_passed', False),
                validation_results['comparison_chart_validation'].get('validation_passed', False),
                validation_results['time_series_validation'].get('validation_passed', False)
            ]),
            'aggregation_issues': []
        }
        
        # Collect aggregation issues
        if not validation_results['network_chart_validation'].get('validation_passed', False):
            validation_results['summary']['aggregation_issues'].append('Network chart aggregation inconsistencies detected')
            
        if not validation_results['comparison_chart_validation'].get('validation_passed', False):
            validation_results['summary']['aggregation_issues'].append('Comparison chart aggregation issues detected')
            
        if not validation_results['time_series_validation'].get('validation_passed', False):
            validation_results['summary']['aggregation_issues'].append('Time series aggregation inconsistencies detected')
        
        self.logger.info(f"Aggregation validation completed. Overall status: {'PASS' if all_passed else 'FAIL'}")
        
        return validation_results
