"""
Secure query building utilities with parameterized query support.

This module provides utilities for building SQL queries with proper parameter binding
to prevent SQL injection and improve query safety throughout the system.
"""

from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import re

from utils.faction_resolver import FactionResolver, get_faction_name_field


class FilterOperator(Enum):
    """Supported filter operators."""
    EQUALS = "="
    NOT_EQUALS = "!="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    BETWEEN = "BETWEEN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


class SecureQueryBuilder:
    """Secure query builder with parameter binding support."""

    def __init__(self):
        self.params: Dict[str, Any] = {}
        self.param_counter: int = 0

    def _get_next_param_name(self, base_name: str = "param") -> str:
        """Generate next parameter name."""
        self.param_counter += 1
        return f"{base_name}_{self.param_counter}"

    def add_parameter(self, value: Any, param_name: Optional[str] = None) -> str:
        """
        Add a parameter and return the parameter placeholder.

        Args:
            value: The parameter value
            param_name: Optional parameter name (auto-generated if not provided)

        Returns:
            Parameter placeholder string (e.g., "$param_1")
        """
        if param_name is None:
            param_name = self._get_next_param_name()

        self.params[param_name] = value
        return f"${param_name}"

    def build_filter_condition(
        self,
        column: str,
        operator: FilterOperator,
        value: Any = None,
        values: Optional[List[Any]] = None
    ) -> str:
        """
        Build a secure filter condition with parameter binding.

        Args:
            column: Column name
            operator: Filter operator
            value: Single value (for operators like =, >, <)
            values: List of values (for operators like IN, BETWEEN)

        Returns:
            SQL condition string with parameter placeholders
        """
        if operator in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
            return f"{column} {operator.value}"

        if operator in [FilterOperator.IN, FilterOperator.NOT_IN]:
            if not values:
                raise ValueError(f"Values list required for {operator.value} operator")

            param_placeholders = []
            for val in values:
                param_name = self.add_parameter(val)
                param_placeholders.append(param_name)

            placeholders_str = ", ".join(param_placeholders)
            return f"{column} {operator.value} ({placeholders_str})"

        if operator == FilterOperator.BETWEEN:
            if not values or len(values) != 2:
                raise ValueError("BETWEEN operator requires exactly 2 values")

            param1 = self.add_parameter(values[0])
            param2 = self.add_parameter(values[1])
            return f"{column} BETWEEN {param1} AND {param2}"

        if operator in [
            FilterOperator.EQUALS, FilterOperator.NOT_EQUALS,
            FilterOperator.GREATER_THAN, FilterOperator.LESS_THAN,
            FilterOperator.GREATER_EQUAL, FilterOperator.LESS_EQUAL,
            FilterOperator.LIKE, FilterOperator.NOT_LIKE
        ]:
            if value is None:
                raise ValueError(f"Value required for {operator.value} operator")

            param_name = self.add_parameter(value)
            return f"{column} {operator.value} {param_name}"

        raise ValueError(f"Unsupported operator: {operator}")

    def build_knesset_filter(
        self,
        knesset_numbers: Optional[List[int]],
        column: str = "KnessetNum"
    ) -> Tuple[str, bool]:
        """
        Build Knesset number filter condition.

        Args:
            knesset_numbers: List of Knesset numbers to filter by
            column: Column name for Knesset number

        Returns:
            Tuple of (condition string, is_single_knesset boolean)
        """
        if not knesset_numbers:
            return "1=1", False

        if len(knesset_numbers) == 1:
            condition = self.build_filter_condition(
                column, FilterOperator.EQUALS, knesset_numbers[0]
            )
            return condition, True
        else:
            condition = self.build_filter_condition(
                column, FilterOperator.IN, values=knesset_numbers
            )
            return condition, False

    def build_faction_filter(
        self,
        faction_names: Optional[List[str]],
        column: str = "FactionName"
    ) -> str:
        """
        Build faction name filter condition.

        Args:
            faction_names: List of faction names to filter by
            column: Column name for faction

        Returns:
            SQL condition string
        """
        if not faction_names:
            return "1=1"

        return self.build_filter_condition(
            column, FilterOperator.IN, values=faction_names
        )

    def build_date_range_filter(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        column: str = "SubmitDate"
    ) -> str:
        """
        Build date range filter condition.

        Args:
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            column: Column name for date

        Returns:
            SQL condition string
        """
        conditions = []

        if start_date:
            conditions.append(
                self.build_filter_condition(
                    column, FilterOperator.GREATER_EQUAL, start_date
                )
            )

        if end_date:
            conditions.append(
                self.build_filter_condition(
                    f"date({column})", FilterOperator.LESS_EQUAL, end_date
                )
            )

        if conditions:
            return " AND ".join(conditions)
        else:
            return "1=1"

    def build_advanced_filters(
        self,
        filters: Dict[str, Any],
        table_prefix: str = ""
    ) -> str:
        """
        Build advanced filter conditions with proper parameter binding.

        Args:
            filters: Dictionary of filter criteria
            table_prefix: Table prefix for column names

        Returns:
            SQL WHERE clause conditions
        """
        conditions = []
        prefix = f"{table_prefix}." if table_prefix else ""

        # Query-specific filters
        if filters.get('query_type_filter'):
            conditions.append(
                self.build_filter_condition(
                    f"{prefix}TypeDesc", FilterOperator.IN,
                    values=filters['query_type_filter']
                )
            )

        # Status filters using joined table alias
        if filters.get('query_status_filter'):
            conditions.append(
                self.build_filter_condition(
                    's."Desc"', FilterOperator.IN,
                    values=filters['query_status_filter']
                )
            )

        # Agenda-specific filters
        if filters.get('session_type_filter'):
            conditions.append(
                self.build_filter_condition(
                    f"{prefix}SubTypeDesc", FilterOperator.IN,
                    values=filters['session_type_filter']
                )
            )

        # Bill-specific filters
        if filters.get('bill_type_filter'):
            conditions.append(
                self.build_filter_condition(
                    f"{prefix}SubTypeDesc", FilterOperator.IN,
                    values=filters['bill_type_filter']
                )
            )

        # Bill status filters
        if filters.get('bill_status_filter'):
            conditions.append(
                self.build_filter_condition(
                    's."Desc"', FilterOperator.IN,
                    values=filters['bill_status_filter']
                )
            )

        # Bill origin filter
        bill_origin = filters.get('bill_origin_filter', 'All Bills')
        if bill_origin == 'Private Bills Only':
            conditions.append(f"{prefix}PrivateNumber IS NOT NULL")
        elif bill_origin == 'Governmental Bills Only':
            conditions.append(f"{prefix}PrivateNumber IS NULL")

        # Date filters
        if filters.get('start_date'):
            conditions.append(
                self.build_filter_condition(
                    f"{prefix}SubmitDate", FilterOperator.GREATER_EQUAL,
                    filters['start_date']
                )
            )

        if filters.get('end_date'):
            conditions.append(
                self.build_filter_condition(
                    f"date({prefix}SubmitDate)", FilterOperator.LESS_EQUAL,
                    filters['end_date']
                )
            )

        return " AND ".join(conditions) if conditions else "1=1"

    def build_secure_query(
        self,
        select_clause: str,
        from_clause: str,
        where_conditions: Optional[List[str]] = None,
        group_by: Optional[str] = None,
        having_clause: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Build a complete secure SQL query.

        Args:
            select_clause: SELECT clause
            from_clause: FROM clause with JOINs
            where_conditions: List of WHERE conditions
            group_by: GROUP BY clause
            having_clause: HAVING clause
            order_by: ORDER BY clause
            limit: LIMIT value

        Returns:
            Complete SQL query string
        """
        query_parts = [
            f"SELECT {select_clause}",
            f"FROM {from_clause}"
        ]

        if where_conditions:
            non_empty_conditions = [cond for cond in where_conditions if cond.strip() != "1=1"]
            if non_empty_conditions:
                query_parts.append(f"WHERE {' AND '.join(non_empty_conditions)}")

        if group_by:
            query_parts.append(f"GROUP BY {group_by}")

        if having_clause:
            query_parts.append(f"HAVING {having_clause}")

        if order_by:
            query_parts.append(f"ORDER BY {order_by}")

        if limit:
            # Limit should be handled as parameter too for extra security
            limit_param = self.add_parameter(limit)
            query_parts.append(f"LIMIT {limit_param}")

        return "\n".join(query_parts)

    def get_parameters(self) -> Dict[str, Any]:
        """Get all accumulated parameters."""
        return self.params.copy()

    def reset(self):
        """Reset the builder for reuse."""
        self.params.clear()
        self.param_counter = 0


class QueryTemplate:
    """Template-based query builder for common patterns."""

    _ALLOWED_TIME_UNITS = {"day", "week", "month", "quarter", "year"}

    @staticmethod
    def build_faction_analysis_query(
        metric_column: str,
        metric_name: str,
        table_name: str,
        builder: SecureQueryBuilder,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        additional_conditions: Optional[List[str]] = None
    ) -> str:
        """
        Build standardized faction analysis query.

        Args:
            metric_column: Column to aggregate (e.g., "QueryID", "BillID")
            metric_name: Name for the aggregated metric
            table_name: Main table name
            builder: SecureQueryBuilder instance
            knesset_filter: Knesset numbers to filter
            faction_filter: Faction names to filter
            additional_conditions: Additional WHERE conditions

        Returns:
            Complete SQL query string
        """
        safe_metric_column = validate_column_name(metric_column)
        safe_table_name = validate_column_name(table_name)
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", metric_name):
            raise ValueError(f"Invalid metric name: {metric_name}")

        # Build filter conditions
        conditions = []

        knesset_condition, _ = builder.build_knesset_filter(knesset_filter)
        if knesset_condition != "1=1":
            conditions.append(knesset_condition)

        faction_condition = builder.build_faction_filter(faction_filter, "f.Name")
        if faction_condition != "1=1":
            conditions.append(faction_condition)

        if additional_conditions:
            conditions.extend(additional_conditions)

        # Use standardized faction resolution
        faction_cte = FactionResolver.get_standard_faction_lookup_cte()

        query = f"""
        WITH {faction_cte}
        SELECT
            {get_faction_name_field('f', "'Unknown'")} AS FactionName,
            COUNT({safe_metric_column}) AS {metric_name}
        FROM {safe_table_name} main
        LEFT JOIN StandardFactionLookup sfl ON main.PersonID = sfl.PersonID
            AND main.KnessetNum = sfl.KnessetNum
            AND sfl.rn = 1
        LEFT JOIN KNS_Faction f ON sfl.FactionID = f.FactionID
        WHERE f.Name IS NOT NULL
        """

        if conditions:
            query += f" AND {' AND '.join(conditions)}"

        query += f"""
        GROUP BY f.Name
        ORDER BY {metric_name} DESC
        LIMIT {builder.add_parameter(20)}
        """

        return query

    @staticmethod
    def build_time_series_query(
        date_column: str,
        metric_column: str,
        table_name: str,
        builder: SecureQueryBuilder,
        time_unit: str = "month",
        knesset_filter: Optional[List[int]] = None
    ) -> str:
        """
        Build standardized time series analysis query.

        Args:
            date_column: Date column to group by
            metric_column: Column to aggregate
            table_name: Main table name
            builder: SecureQueryBuilder instance
            time_unit: Time unit for grouping (month, year, week)
            knesset_filter: Knesset numbers to filter

        Returns:
            Complete SQL query string
        """
        normalized_time_unit = time_unit.strip().lower()
        if normalized_time_unit not in QueryTemplate._ALLOWED_TIME_UNITS:
            raise ValueError(
                f"Invalid time_unit: {time_unit}. "
                f"Allowed values: {sorted(QueryTemplate._ALLOWED_TIME_UNITS)}"
            )

        safe_date_column = validate_column_name(date_column)
        safe_metric_column = validate_column_name(metric_column)
        safe_table_name = validate_column_name(table_name)
        time_unit_placeholder = builder.add_parameter(
            normalized_time_unit,
            param_name="time_unit",
        )

        conditions = []

        knesset_condition, _ = builder.build_knesset_filter(knesset_filter)
        if knesset_condition != "1=1":
            conditions.append(knesset_condition)

        # Add date range validation
        conditions.append(
            builder.build_filter_condition(
                f"CAST({safe_date_column} AS TIMESTAMP)",
                FilterOperator.GREATER_EQUAL,
                "1949-01-25"  # First Knesset date
            )
        )

        # Keep upper bound as SQL expression (not a string parameter).
        conditions.append(
            f"CAST({safe_date_column} AS TIMESTAMP) <= CURRENT_DATE + INTERVAL '1 year'"
        )

        query = f"""
        SELECT
            DATE_TRUNC({time_unit_placeholder}, CAST({safe_date_column} AS TIMESTAMP)) as time_period,
            COUNT(DISTINCT {safe_metric_column}) as metric_count
        FROM {safe_table_name}
        WHERE {safe_date_column} IS NOT NULL
        """

        if conditions:
            query += f" AND {' AND '.join(conditions)}"

        query += f"""
        GROUP BY DATE_TRUNC({time_unit_placeholder}, CAST({safe_date_column} AS TIMESTAMP))
        ORDER BY time_period
        """

        return query


# Utility functions for common secure operations
def build_safe_in_clause(values: List[Any], builder: SecureQueryBuilder) -> str:
    """
    Build a safe IN clause with parameter binding.

    Args:
        values: List of values for IN clause
        builder: SecureQueryBuilder instance

    Returns:
        Parameter placeholders for IN clause
    """
    if not values:
        return ""

    placeholders = []
    for value in values:
        placeholder = builder.add_parameter(value)
        placeholders.append(placeholder)

    return f"({', '.join(placeholders)})"


def validate_column_name(column_name: str) -> str:
    """
    Validate and sanitize column name to prevent injection.

    Args:
        column_name: Column name to validate

    Returns:
        Sanitized column name

    Raises:
        ValueError: If column name contains invalid characters
    """
    # Allow alphanumeric, underscore, dot, and quotes for table prefixes
    if not re.match(r'^[a-zA-Z0-9_."]+$', column_name):
        raise ValueError(f"Invalid column name: {column_name}")

    return column_name


def build_pagination_clause(
    page: int,
    page_size: int,
    builder: SecureQueryBuilder
) -> str:
    """
    Build secure pagination clause.

    Args:
        page: Page number (0-based)
        page_size: Number of records per page
        builder: SecureQueryBuilder instance

    Returns:
        SQL LIMIT and OFFSET clause
    """
    if page < 0 or page_size <= 0:
        raise ValueError("Invalid pagination parameters")

    # Limit maximum page size for performance
    max_page_size = 1000
    if page_size > max_page_size:
        page_size = max_page_size

    offset = page * page_size
    limit_param = builder.add_parameter(page_size)
    offset_param = builder.add_parameter(offset)

    return f"LIMIT {limit_param} OFFSET {offset_param}"
