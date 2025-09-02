"""
Committee resolution utility for consistent committee lookups and joins.

This module provides standardized approaches to resolve committee information
and handle data type mismatches in committee joins.
"""

from typing import Dict, List, Optional, Any


class CommitteeResolver:
    """Centralized committee resolution logic."""
    
    @staticmethod
    def get_safe_committee_join_clause(
        bill_table_alias: str = "B",
        committee_table_alias: str = "C",
        committee_id_field: str = "CommitteeID"
    ) -> str:
        """
        Generate a safe committee JOIN clause that handles data type issues.
        
        This attempts multiple join strategies to maximize committee resolution:
        1. Direct join without casting (fastest)  
        2. Cast bill CommitteeID to BIGINT (current approach)
        3. Cast committee CommitteeID to match bill data type
        
        Args:
            bill_table_alias: Alias for the bill table
            committee_table_alias: Alias for the committee table  
            committee_id_field: Name of the CommitteeID field
            
        Returns:
            SQL JOIN clause for committee resolution
        """
        return f"""
        LEFT JOIN KNS_Committee {committee_table_alias} ON (
            -- Try direct join first (most efficient)
            {bill_table_alias}.{committee_id_field} = {committee_table_alias}.{committee_id_field}
            -- Fallback to cast-based join if needed
            OR CAST({bill_table_alias}.{committee_id_field} AS BIGINT) = {committee_table_alias}.{committee_id_field}
            -- Additional fallback for string-based IDs
            OR CAST({bill_table_alias}.{committee_id_field} AS VARCHAR) = CAST({committee_table_alias}.{committee_id_field} AS VARCHAR)
        )"""
    
    @staticmethod
    def get_committee_name_with_fallback(
        committee_table_alias: str = "C",
        bill_table_alias: str = "B",
        committee_id_field: str = "CommitteeID"
    ) -> str:
        """
        Generate committee name field with intelligent fallback.
        
        Args:
            committee_table_alias: Alias for committee table
            bill_table_alias: Alias for bill table
            committee_id_field: CommitteeID field name
            
        Returns:
            SQL expression for committee name with fallback
        """
        return f"""
        COALESCE(
            {committee_table_alias}.Name,
            CASE 
                WHEN {bill_table_alias}.{committee_id_field} IS NOT NULL THEN 
                    'Committee ' || CAST({bill_table_alias}.{committee_id_field} AS VARCHAR)
                ELSE NULL 
            END
        )"""
    
    @staticmethod
    def get_enhanced_committee_resolution_cte() -> str:
        """
        Generate CTE for enhanced committee resolution with historical data.
        
        This attempts to resolve committee names by looking across multiple
        Knessets and time periods to improve the success rate.
        
        Returns:
            SQL CTE for enhanced committee resolution
        """
        return """
        EnhancedCommitteeResolution AS (
            SELECT DISTINCT
                c.CommitteeID,
                c.Name as CommitteeName,
                c.KnessetNum as CommitteeKnessetNum,
                c.CommitteeTypeDesc,
                c.AdditionalTypeDesc,
                c.CommitteeParentName,
                -- Rank by most recent Knesset and most complete data
                ROW_NUMBER() OVER (
                    PARTITION BY c.CommitteeID 
                    ORDER BY 
                        c.KnessetNum DESC,
                        CASE WHEN c.Name IS NOT NULL THEN 0 ELSE 1 END,
                        CASE WHEN c.CommitteeTypeDesc IS NOT NULL THEN 0 ELSE 1 END
                ) as rn
            FROM KNS_Committee c
            WHERE c.CommitteeID IS NOT NULL
        )"""
    
    @staticmethod
    def get_enhanced_committee_join_clause(
        bill_table_alias: str = "B",
        committee_cte_alias: str = "ecr"
    ) -> str:
        """
        Generate enhanced committee JOIN using the resolution CTE.
        
        Args:
            bill_table_alias: Alias for bill table
            committee_cte_alias: Alias for enhanced committee resolution CTE
            
        Returns:
            SQL JOIN clause using enhanced resolution
        """
        return f"""
        LEFT JOIN EnhancedCommitteeResolution {committee_cte_alias} ON (
            CAST({bill_table_alias}.CommitteeID AS BIGINT) = {committee_cte_alias}.CommitteeID
            AND {committee_cte_alias}.rn = 1
        )"""
    
    @staticmethod
    def validate_committee_data_types() -> Dict[str, str]:
        """
        Generate queries to validate committee data type consistency.
        
        Returns:
            Dictionary of validation queries
        """
        return {
            "committee_id_types": """
                SELECT 
                    'KNS_Bill.CommitteeID' as table_field,
                    TYPEOF(CommitteeID) as data_type,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT CommitteeID) as unique_values
                FROM KNS_Bill 
                WHERE CommitteeID IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    'KNS_Committee.CommitteeID' as table_field,
                    TYPEOF(CommitteeID) as data_type,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT CommitteeID) as unique_values
                FROM KNS_Committee 
                WHERE CommitteeID IS NOT NULL
            """,
            
            "committee_id_overlap": """
                WITH BillCommitteeIDs AS (
                    SELECT DISTINCT CAST(CommitteeID AS BIGINT) as CommitteeID
                    FROM KNS_Bill 
                    WHERE CommitteeID IS NOT NULL
                ),
                TableCommitteeIDs AS (
                    SELECT DISTINCT CommitteeID
                    FROM KNS_Committee
                    WHERE CommitteeID IS NOT NULL
                )
                SELECT 
                    COUNT(DISTINCT b.CommitteeID) as bill_committee_ids,
                    COUNT(DISTINCT t.CommitteeID) as table_committee_ids,
                    COUNT(DISTINCT CASE WHEN t.CommitteeID IS NOT NULL THEN b.CommitteeID END) as overlapping_ids,
                    ROUND(100.0 * COUNT(DISTINCT CASE WHEN t.CommitteeID IS NOT NULL THEN b.CommitteeID END) / COUNT(DISTINCT b.CommitteeID), 1) as overlap_percentage
                FROM BillCommitteeIDs b
                LEFT JOIN TableCommitteeIDs t ON b.CommitteeID = t.CommitteeID
            """,
            
            "committee_coverage_by_knesset": """
                SELECT 
                    b.KnessetNum,
                    COUNT(DISTINCT b.BillID) as total_bills,
                    COUNT(DISTINCT CASE WHEN b.CommitteeID IS NOT NULL THEN b.BillID END) as bills_with_committee_id,
                    COUNT(DISTINCT CASE WHEN c.Name IS NOT NULL THEN b.BillID END) as bills_with_committee_name,
                    ROUND(100.0 * COUNT(DISTINCT CASE WHEN c.Name IS NOT NULL THEN b.BillID END) / 
                          COUNT(DISTINCT CASE WHEN b.CommitteeID IS NOT NULL THEN b.BillID END), 1) as name_resolution_rate
                FROM KNS_Bill b
                LEFT JOIN KNS_Committee c ON CAST(b.CommitteeID AS BIGINT) = c.CommitteeID
                WHERE b.KnessetNum IS NOT NULL
                GROUP BY b.KnessetNum
                ORDER BY b.KnessetNum DESC
            """
        }
    
    @staticmethod  
    def get_committee_data_fixes() -> Dict[str, str]:
        """
        Generate queries to identify and suggest fixes for committee data issues.
        
        Returns:
            Dictionary of diagnostic and fix queries
        """
        return {
            "missing_committee_names": """
                -- Find bills with CommitteeID but no resolved committee name
                SELECT 
                    b.BillID,
                    b.KnessetNum,
                    b.CommitteeID,
                    b.Name as BillName,
                    'Missing committee name' as issue
                FROM KNS_Bill b
                LEFT JOIN KNS_Committee c ON CAST(b.CommitteeID AS BIGINT) = c.CommitteeID
                WHERE b.CommitteeID IS NOT NULL 
                    AND c.Name IS NULL
                ORDER BY b.KnessetNum DESC, b.BillID DESC
                LIMIT 20
            """,
            
            "duplicate_committee_ids": """
                -- Find committee IDs that appear multiple times with different names
                SELECT 
                    CommitteeID,
                    COUNT(DISTINCT Name) as name_count,
                    GROUP_CONCAT(DISTINCT Name, ' | ') as all_names,
                    COUNT(*) as record_count
                FROM KNS_Committee
                WHERE CommitteeID IS NOT NULL AND Name IS NOT NULL
                GROUP BY CommitteeID
                HAVING COUNT(DISTINCT Name) > 1
                ORDER BY name_count DESC
            """,
            
            "committee_historical_coverage": """
                -- Analyze committee data availability across Knessets
                SELECT 
                    c.KnessetNum,
                    COUNT(DISTINCT c.CommitteeID) as committees_in_table,
                    COUNT(DISTINCT c.Name) as unique_committee_names,
                    COUNT(DISTINCT CASE WHEN c.Name IS NOT NULL THEN c.CommitteeID END) as committees_with_names
                FROM KNS_Committee c
                GROUP BY c.KnessetNum
                ORDER BY c.KnessetNum DESC
            """,
            
            "suggested_committee_name_fixes": """
                -- Suggest committee name fixes based on historical data
                WITH CommitteeNameHistory AS (
                    SELECT 
                        CommitteeID,
                        Name,
                        KnessetNum,
                        COUNT(*) as usage_count,
                        ROW_NUMBER() OVER (PARTITION BY CommitteeID ORDER BY KnessetNum DESC, COUNT(*) DESC) as preference_rank
                    FROM KNS_Committee
                    WHERE CommitteeID IS NOT NULL AND Name IS NOT NULL
                    GROUP BY CommitteeID, Name, KnessetNum
                )
                SELECT 
                    cnh.CommitteeID,
                    cnh.Name as suggested_name,
                    cnh.KnessetNum as source_knesset,
                    cnh.usage_count,
                    COUNT(DISTINCT b.BillID) as would_affect_bills
                FROM CommitteeNameHistory cnh
                JOIN KNS_Bill b ON CAST(b.CommitteeID AS BIGINT) = cnh.CommitteeID
                LEFT JOIN KNS_Committee c ON CAST(b.CommitteeID AS BIGINT) = c.CommitteeID AND c.Name IS NOT NULL
                WHERE cnh.preference_rank = 1
                    AND c.Name IS NULL  -- Only for currently unresolved committee IDs
                GROUP BY cnh.CommitteeID, cnh.Name, cnh.KnessetNum, cnh.usage_count
                ORDER BY would_affect_bills DESC
                LIMIT 50
            """
        }


def get_committee_name_with_enhanced_fallback(
    committee_alias: str = "c",
    bill_alias: str = "b"
) -> str:
    """
    Get enhanced committee name field with multiple fallback strategies.
    
    Args:
        committee_alias: Alias for committee table/CTE
        bill_alias: Alias for bill table
        
    Returns:
        SQL expression for committee name with enhanced fallback
    """
    return f"""
    COALESCE(
        {committee_alias}.CommitteeName,
        {committee_alias}.Name,
        CASE 
            WHEN {bill_alias}.CommitteeID IS NOT NULL THEN 
                'Committee ' || CAST({bill_alias}.CommitteeID AS VARCHAR)
            ELSE NULL 
        END
    )"""


def build_committee_filter_condition(
    committee_filter: Optional[List[str]], 
    committee_alias: str = "c"
) -> str:
    """
    Build committee filter condition with proper SQL escaping.
    
    Args:
        committee_filter: List of committee names to filter by
        committee_alias: Alias for committee table
        
    Returns:
        SQL WHERE condition for committee filtering
    """
    if not committee_filter:
        return "1=1"
    
    # Escape single quotes for SQL safety  
    safe_committees = [name.replace("'", "''") for name in committee_filter]
    committee_list = "', '".join(safe_committees)
    return f"({committee_alias}.Name IN ('{committee_list}') OR {committee_alias}.CommitteeName IN ('{committee_list}'))"