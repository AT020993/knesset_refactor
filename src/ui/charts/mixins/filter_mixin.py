"""Chart filter mixin for building SQL filter conditions.

This mixin provides filter building functionality for chart classes:
- Standard filter building using FilterBuilder
- Secure filter building with parameter binding
- Advanced filter conditions for specific entity types
"""

from typing import Any, Dict, List, Optional, Tuple

from ui.queries.filter_builder import FilterBuilder
from utils.query_builder import SecureQueryBuilder, FilterOperator


class ChartFilterMixin:
    """Mixin providing filter building capabilities for charts.

    This mixin handles:
    - Building filter conditions for Knesset and faction filtering
    - Building secure parameterized filters
    - Advanced filters for queries, bills, and agendas
    """

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes for SQL injection prevention.

        Args:
            value: String to escape.

        Returns:
            Escaped string safe for SQL.
        """
        return value.replace("'", "''")

    def build_filters(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        table_prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> Dict[str, Any]:
        """Build common filter conditions for SQL queries.

        Uses FilterBuilder internally for consistent filter generation.

        Args:
            knesset_filter: List of Knesset numbers to filter by.
            faction_filter: List of faction names to filter by.
            table_prefix: Table alias prefix (e.g., "q" for "q.KnessetNum").
            date_column: Column name for date filters (default: "SubmitDate").
            **kwargs: Additional filter parameters:
                - query_type_filter: List of query types
                - query_status_filter: List of query statuses
                - session_type_filter: List of session types
                - bill_type_filter: List of bill types
                - bill_status_filter: List of bill statuses
                - bill_origin_filter: "All Bills", "Private Bills Only", "Governmental Bills Only"
                - start_date: Start date for date range filter
                - end_date: End date for date range filter

        Returns:
            Dictionary with all filter conditions and metadata:
                - knesset_condition: SQL condition for Knesset filter
                - knesset_title: Human-readable title for display
                - is_single_knesset: Boolean indicating single Knesset selection
                - faction_condition: SQL condition for faction filter
                - advanced_conditions: List of additional filter conditions
        """
        builder = FilterBuilder(table_prefix=table_prefix, date_column=date_column)
        builder.add_knesset(knesset_filter).add_faction(faction_filter).from_kwargs(**kwargs)
        filters = builder.build()

        # Add agenda_status_condition for backward compatibility
        filters["agenda_status_condition"] = "1=1"

        return filters

    def build_secure_filters(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        table_prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> Tuple[Dict[str, Any], SecureQueryBuilder]:
        """Build common filter conditions using secure parameter binding.

        Similar to build_filters but uses parameterized queries for security.

        Args:
            knesset_filter: List of Knesset numbers to filter by.
            faction_filter: List of faction names to filter by.
            table_prefix: Table alias prefix (e.g., "q" for "q.KnessetNum").
            date_column: Column name for date filters (default: "SubmitDate").
            **kwargs: Additional filter parameters (same as build_filters).

        Returns:
            Tuple of (filters_dict, SecureQueryBuilder):
                - filters_dict: Dictionary with filter conditions
                - builder: SecureQueryBuilder with bound parameters
        """
        builder = SecureQueryBuilder()
        filters: Dict[str, Any] = {}

        # Add table prefix with dot if provided
        prefix = f"{table_prefix}." if table_prefix else ""

        # Build Knesset filter
        knesset_condition, is_single = builder.build_knesset_filter(
            knesset_filter, f"{prefix}KnessetNum"
        )
        filters["knesset_condition"] = knesset_condition
        filters["is_single_knesset"] = is_single

        if knesset_filter:
            if is_single:
                filters["knesset_title"] = f"Knesset {knesset_filter[0]}"
            else:
                filters["knesset_title"] = f"Knessets: {', '.join(map(str, knesset_filter))}"
        else:
            filters["knesset_title"] = "All Knessets"

        # Build faction filter
        if faction_filter:
            filters["faction_condition"] = builder.build_faction_filter(
                faction_filter, "FactionName"
            )
        else:
            filters["faction_condition"] = "1=1"

        # Add advanced filters with secure parameter binding
        filters["advanced_conditions"] = self._build_secure_advanced_filters(
            builder, prefix, date_column, **kwargs
        )

        return filters, builder

    def _build_secure_advanced_filters(
        self,
        builder: SecureQueryBuilder,
        prefix: str = "",
        date_column: str = "SubmitDate",
        **kwargs,
    ) -> List[str]:
        """Build advanced filter conditions using secure parameter binding.

        Handles entity-specific filters like query types, bill statuses, etc.

        Args:
            builder: SecureQueryBuilder for parameter binding.
            prefix: Table prefix with trailing dot (e.g., "q.").
            date_column: Column name for date filters.
            **kwargs: Filter values for various entity types.

        Returns:
            List of SQL condition strings.
        """
        conditions = []

        # Query-specific filters
        query_type_filter = kwargs.get("query_type_filter", [])
        if query_type_filter:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}TypeDesc", FilterOperator.IN, values=query_type_filter
                )
            )

        # Status filters using joined table alias
        query_status_filter = kwargs.get("query_status_filter", [])
        if query_status_filter:
            conditions.append(
                builder.build_filter_condition(
                    's."Desc"', FilterOperator.IN, values=query_status_filter
                )
            )

        # Agenda-specific filters
        session_type_filter = kwargs.get("session_type_filter", [])
        if session_type_filter:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}SubTypeDesc", FilterOperator.IN, values=session_type_filter
                )
            )

        # Bill-specific filters
        bill_type_filter = kwargs.get("bill_type_filter", [])
        if bill_type_filter:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}SubTypeDesc", FilterOperator.IN, values=bill_type_filter
                )
            )

        bill_status_filter = kwargs.get("bill_status_filter", [])
        if bill_status_filter:
            conditions.append(
                builder.build_filter_condition(
                    's."Desc"', FilterOperator.IN, values=bill_status_filter
                )
            )

        # Bill origin filter (not parameterized - static conditions)
        bill_origin_filter = kwargs.get("bill_origin_filter", "All Bills")
        if bill_origin_filter == "Private Bills Only":
            conditions.append(f"{prefix}PrivateNumber IS NOT NULL")
        elif bill_origin_filter == "Governmental Bills Only":
            conditions.append(f"{prefix}PrivateNumber IS NULL")

        # Date filters with parameter binding
        start_date = kwargs.get("start_date")
        if start_date:
            conditions.append(
                builder.build_filter_condition(
                    f"{prefix}{date_column}", FilterOperator.GREATER_EQUAL, start_date
                )
            )

        end_date = kwargs.get("end_date")
        if end_date:
            conditions.append(
                builder.build_filter_condition(
                    f"date({prefix}{date_column})", FilterOperator.LESS_EQUAL, end_date
                )
            )

        return conditions
