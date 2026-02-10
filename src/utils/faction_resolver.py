"""
Standardized faction resolution utility for consistent faction lookups across all queries and charts.

This module provides a single, consistent approach to resolve faction affiliations for MKs,
eliminating inconsistencies between different calculation methods throughout the system.

This module uses SQLTemplates for the core CTE definitions to avoid duplication.
"""

from typing import List, Optional, Dict, Any

from ui.queries.sql_templates import SQLTemplates


class FactionResolver:
    """Centralized faction resolution logic."""
    
    @staticmethod
    def get_standard_faction_lookup_cte(
        table_alias: str = "ptp",
        person_id_field: str = "PersonID",
        knesset_num_field: str = "KnessetNum"
    ) -> str:
        """
        Generate standardized faction lookup CTE.

        This uses a consistent ranking approach:
        1. Prioritize non-null FactionID
        2. Order by KnessetNum DESC (most recent Knesset first)
        3. Order by StartDate DESC (most recent position first)

        Args:
            table_alias: Alias to use for the PersonToPosition table
            person_id_field: Name of the PersonID field (must be "PersonID")
            knesset_num_field: Name of the KnessetNum field (must be "KnessetNum")

        Returns:
            SQL CTE definition for faction lookup

        Note:
            Uses SQLTemplates.get_standard_faction_lookup() internally.
            The person_id_field and knesset_num_field parameters are kept for
            backward compatibility but must match the default values.
        """
        # Delegate to SQLTemplates for the core CTE logic
        return SQLTemplates.get_standard_faction_lookup(table_alias)
    
    @staticmethod
    def get_faction_join_clause(
        main_table_alias: str,
        person_id_field: str = "PersonID",
        knesset_num_field: str = "KnessetNum",
        faction_alias: str = "f"
    ) -> str:
        """
        Generate standardized faction JOIN clause.
        
        Args:
            main_table_alias: Alias of the main table being joined
            person_id_field: Name of the PersonID field in main table
            knesset_num_field: Name of the KnessetNum field in main table
            faction_alias: Alias to use for the faction table
            
        Returns:
            SQL JOIN clause for faction resolution
        """
        return f"""
        LEFT JOIN StandardFactionLookup sfl ON {main_table_alias}.{person_id_field} = sfl.PersonID 
            AND {main_table_alias}.{knesset_num_field} = sfl.KnessetNum 
            AND sfl.rn = 1
        LEFT JOIN KNS_Faction {faction_alias} ON sfl.FactionID = {faction_alias}.FactionID"""
    
    @staticmethod
    def get_coalition_status_join_clause(
        main_table_alias: str,
        knesset_num_field: str = "KnessetNum",
        faction_alias: str = "f",
        coalition_alias: str = "ufs"
    ) -> str:
        """
        Generate coalition status JOIN clause.
        
        Args:
            main_table_alias: Alias of the main table
            knesset_num_field: Name of the KnessetNum field
            faction_alias: Alias of the faction table
            coalition_alias: Alias for UserFactionCoalitionStatus table
            
        Returns:
            SQL JOIN clause for coalition status
        """
        return f"""
        LEFT JOIN UserFactionCoalitionStatus {coalition_alias} ON {faction_alias}.FactionID = {coalition_alias}.FactionID 
            AND {main_table_alias}.{knesset_num_field} = {coalition_alias}.KnessetNum"""
    
    @staticmethod
    def get_complete_faction_resolution_query(
        main_table: str,
        main_alias: str,
        person_id_field: str = "PersonID",
        knesset_num_field: str = "KnessetNum",
        additional_joins: str = "",
        select_fields: Optional[List[str]] = None,
        where_clause: str = "1=1",
        group_by_fields: Optional[List[str]] = None,
        order_by_clause: str = ""
    ) -> str:
        """
        Generate a complete query with standardized faction resolution.
        
        Args:
            main_table: Main table name
            main_alias: Alias for main table
            person_id_field: PersonID field name
            knesset_num_field: KnessetNum field name  
            additional_joins: Any additional JOIN clauses
            select_fields: List of SELECT fields
            where_clause: WHERE condition
            group_by_fields: Fields for GROUP BY
            order_by_clause: ORDER BY clause
            
        Returns:
            Complete SQL query with faction resolution
        """
        if select_fields is None:
            select_fields = [f"{main_alias}.*"]
            
        select_clause = ",\n    ".join(select_fields)
        
        faction_cte = FactionResolver.get_standard_faction_lookup_cte()
        faction_joins = FactionResolver.get_faction_join_clause(main_alias, person_id_field, knesset_num_field)
        coalition_join = FactionResolver.get_coalition_status_join_clause(main_alias, knesset_num_field)
        
        query = f"""
        WITH {faction_cte}
        SELECT
            {select_clause}
        FROM {main_table} {main_alias}
        {faction_joins}
        {coalition_join}
        {additional_joins}
        WHERE {where_clause}
        """
        
        if group_by_fields:
            group_by_clause = ",\n    ".join(group_by_fields)
            query += f"\nGROUP BY\n    {group_by_clause}"
            
        if order_by_clause:
            query += f"\nORDER BY {order_by_clause}"
            
        return query
    
    @staticmethod
    def get_network_chart_faction_subquery(knesset_filter_condition: str = "1=1") -> str:
        """
        Generate faction resolution subquery specifically for network charts.
        
        This handles the more complex case where we need faction resolution
        across multiple Knessets with fallback logic.
        
        Args:
            knesset_filter_condition: WHERE condition for Knesset filtering
            
        Returns:
            SQL subquery for faction resolution in network charts
        """
        return f"""
        (SELECT f_inner.Name 
         FROM KNS_PersonToPosition ptp_inner 
         JOIN KNS_Faction f_inner ON ptp_inner.FactionID = f_inner.FactionID
         WHERE ptp_inner.PersonID = p.PersonID 
             AND {knesset_filter_condition.replace('b.KnessetNum', 'ptp_inner.KnessetNum')}
             AND ptp_inner.FactionID IS NOT NULL
         ORDER BY ptp_inner.KnessetNum DESC, ptp_inner.StartDate DESC 
         LIMIT 1)"""
    
    @staticmethod
    def validate_faction_resolution_consistency() -> Dict[str, Any]:
        """
        Generate validation queries to check faction resolution consistency.
        
        Returns:
            Dictionary containing validation query templates
        """
        return {
            "duplicate_faction_assignments": """
                -- Check for MKs with multiple faction assignments in same Knesset
                SELECT 
                    PersonID, 
                    KnessetNum, 
                    COUNT(DISTINCT FactionID) as faction_count,
                    GROUP_CONCAT(DISTINCT FactionID) as faction_ids
                FROM KNS_PersonToPosition 
                WHERE FactionID IS NOT NULL
                GROUP BY PersonID, KnessetNum
                HAVING COUNT(DISTINCT FactionID) > 1
            """,
            
            "missing_faction_data": """
                -- Check for MKs without faction assignments
                SELECT 
                    p.PersonID,
                    p.FirstName || ' ' || p.LastName as FullName,
                    COUNT(DISTINCT bi.BillID) as bills_initiated
                FROM KNS_Person p
                JOIN KNS_BillInitiator bi ON p.PersonID = bi.PersonID
                LEFT JOIN KNS_PersonToPosition ptp ON p.PersonID = ptp.PersonID 
                    AND ptp.FactionID IS NOT NULL
                WHERE ptp.PersonID IS NULL
                GROUP BY p.PersonID, p.FirstName, p.LastName
                ORDER BY bills_initiated DESC
            """,
            
            "faction_coverage_by_knesset": """
                -- Check faction resolution coverage by Knesset
                WITH AllMKsWithBills AS (
                    SELECT DISTINCT 
                        bi.PersonID,
                        b.KnessetNum
                    FROM KNS_BillInitiator bi
                    JOIN KNS_Bill b ON bi.BillID = b.BillID
                    WHERE bi.Ordinal = 1
                ),
                MKsWithFactions AS (
                    SELECT DISTINCT
                        amwb.PersonID,
                        amwb.KnessetNum
                    FROM AllMKsWithBills amwb
                    JOIN KNS_PersonToPosition ptp ON amwb.PersonID = ptp.PersonID 
                        AND amwb.KnessetNum = ptp.KnessetNum
                        AND ptp.FactionID IS NOT NULL
                )
                SELECT 
                    amwb.KnessetNum,
                    COUNT(DISTINCT amwb.PersonID) as total_mks_with_bills,
                    COUNT(DISTINCT mwf.PersonID) as mks_with_factions,
                    ROUND(100.0 * COUNT(DISTINCT mwf.PersonID) / COUNT(DISTINCT amwb.PersonID), 1) as coverage_percentage
                FROM AllMKsWithBills amwb
                LEFT JOIN MKsWithFactions mwf ON amwb.PersonID = mwf.PersonID 
                    AND amwb.KnessetNum = mwf.KnessetNum
                GROUP BY amwb.KnessetNum
                ORDER BY amwb.KnessetNum DESC
            """
        }


# Utility functions for common faction resolution patterns
def get_faction_name_field(table_alias: str = "f", fallback: str = "'Unknown'") -> str:
    """Get standardized faction name field with fallback."""
    return f"COALESCE({table_alias}.Name, {fallback})"


def get_coalition_status_field(table_alias: str = "ufs", fallback: str = "'Unknown'") -> str:
    """Get standardized coalition status field with fallback.""" 
    return f"COALESCE({table_alias}.CoalitionStatus, {fallback})"


def build_faction_filter_condition(faction_filter: Optional[List[str]], table_alias: str = "f") -> str:
    """Build faction filter condition with proper SQL escaping."""
    if not faction_filter:
        return "1=1"
    
    # Escape single quotes for SQL safety
    safe_factions = [faction.replace("'", "''") for faction in faction_filter]
    faction_list = "', '".join(safe_factions)
    return f"{table_alias}.Name IN ('{faction_list}')"
