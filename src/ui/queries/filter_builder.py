"""Unified filter builder for SQL query conditions.

This module provides the FilterBuilder class for building SQL WHERE conditions
in a consistent, fluent interface style. It consolidates filter building logic
that was previously duplicated across multiple chart modules.
"""

from typing import Any, Dict, List, Optional, Tuple


class FilterBuilder:
    """Unified filter builder for SQL query conditions.

    Consolidates filter building logic that was previously duplicated across
    build_filters(), build_secure_filters(), and _build_legacy_advanced_filters().

    Supports both legacy string-based conditions and parameterized conditions.

    Example:
        builder = FilterBuilder(table_prefix="q")
        builder.add_knesset([25]).add_faction(["Likud"]).add_date_range(start="2023-01-01")
        filters = builder.build()
    """

    # Filter type constants
    BILL_ORIGIN_ALL = "All Bills"
    BILL_ORIGIN_PRIVATE = "Private Bills Only"
    BILL_ORIGIN_GOVERNMENTAL = "Governmental Bills Only"

    def __init__(self, table_prefix: str = "", date_column: str = "SubmitDate"):
        """Initialize the FilterBuilder.

        Args:
            table_prefix: Table alias prefix (e.g., "q" for "q.KnessetNum")
            date_column: Column name for date filters (default: "SubmitDate")
        """
        self.prefix = f"{table_prefix}." if table_prefix else ""
        self.date_column = date_column
        self._knesset_filter: Optional[List[int]] = None
        self._faction_filter: Optional[List[str]] = None
        self._query_type_filter: List[str] = []
        self._query_status_filter: List[str] = []
        self._session_type_filter: List[str] = []
        self._bill_type_filter: List[str] = []
        self._bill_status_filter: List[str] = []
        self._bill_origin_filter: str = self.BILL_ORIGIN_ALL
        self._start_date: Optional[str] = None
        self._end_date: Optional[str] = None

    def add_knesset(self, knesset_filter: Optional[List[int]]) -> 'FilterBuilder':
        """Add Knesset filter."""
        self._knesset_filter = knesset_filter
        return self

    def add_faction(self, faction_filter: Optional[List[str]]) -> 'FilterBuilder':
        """Add faction filter."""
        self._faction_filter = faction_filter
        return self

    def add_query_type(self, query_type_filter: List[str]) -> 'FilterBuilder':
        """Add query type filter."""
        self._query_type_filter = query_type_filter
        return self

    def add_query_status(self, query_status_filter: List[str]) -> 'FilterBuilder':
        """Add query status filter."""
        self._query_status_filter = query_status_filter
        return self

    def add_session_type(self, session_type_filter: List[str]) -> 'FilterBuilder':
        """Add session type filter."""
        self._session_type_filter = session_type_filter
        return self

    def add_bill_type(self, bill_type_filter: List[str]) -> 'FilterBuilder':
        """Add bill type filter."""
        self._bill_type_filter = bill_type_filter
        return self

    def add_bill_status(self, bill_status_filter: List[str]) -> 'FilterBuilder':
        """Add bill status filter."""
        self._bill_status_filter = bill_status_filter
        return self

    def add_bill_origin(self, bill_origin_filter: str) -> 'FilterBuilder':
        """Add bill origin filter (All Bills / Private Bills Only / Governmental Bills Only)."""
        self._bill_origin_filter = bill_origin_filter
        return self

    def add_date_range(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> 'FilterBuilder':
        """Add date range filter."""
        self._start_date = start_date
        self._end_date = end_date
        return self

    def from_kwargs(self, **kwargs) -> 'FilterBuilder':
        """Populate filters from kwargs (for backward compatibility).

        Args:
            **kwargs: Filter parameters from chart methods
        """
        if kwargs.get("query_type_filter"):
            self.add_query_type(kwargs["query_type_filter"])
        if kwargs.get("query_status_filter"):
            self.add_query_status(kwargs["query_status_filter"])
        if kwargs.get("session_type_filter"):
            self.add_session_type(kwargs["session_type_filter"])
        if kwargs.get("bill_type_filter"):
            self.add_bill_type(kwargs["bill_type_filter"])
        if kwargs.get("bill_status_filter"):
            self.add_bill_status(kwargs["bill_status_filter"])
        if kwargs.get("bill_origin_filter"):
            self.add_bill_origin(kwargs["bill_origin_filter"])
        if kwargs.get("start_date"):
            self._start_date = str(kwargs["start_date"])
        if kwargs.get("end_date"):
            self._end_date = str(kwargs["end_date"])
        return self

    @staticmethod
    def _escape_sql_string(value: str) -> str:
        """Escape single quotes for SQL injection prevention."""
        return value.replace("'", "''")

    def _build_knesset_condition(self) -> Tuple[str, bool, str]:
        """Build Knesset filter condition.

        Returns:
            Tuple of (sql_condition, is_single_knesset, title_string)
        """
        if self._knesset_filter:
            if len(self._knesset_filter) == 1:
                return (
                    f"{self.prefix}KnessetNum = {self._knesset_filter[0]}",
                    True,
                    f"Knesset {self._knesset_filter[0]}"
                )
            else:
                knesset_str = ", ".join(map(str, self._knesset_filter))
                return (
                    f"{self.prefix}KnessetNum IN ({knesset_str})",
                    False,
                    f"Knessets: {', '.join(map(str, self._knesset_filter))}"
                )
        return "1=1", False, "All Knessets"

    def _build_faction_condition(self) -> str:
        """Build faction filter condition."""
        if self._faction_filter:
            escaped = [f"'{self._escape_sql_string(f)}'" for f in self._faction_filter]
            return f"FactionName IN ({', '.join(escaped)})"
        return "1=1"

    def _build_in_condition(self, column: str, values: List[str]) -> str:
        """Build IN condition for a list of string values."""
        if not values:
            return "1=1"
        escaped = [f"'{self._escape_sql_string(v)}'" for v in values]
        return f"{column} IN ({', '.join(escaped)})"

    def build(self) -> Dict[str, Any]:
        """Build the complete filter dictionary.

        Returns:
            Dictionary with all filter conditions and metadata.
        """
        knesset_cond, is_single, knesset_title = self._build_knesset_condition()

        filters = {
            # Core filters
            "knesset_condition": knesset_cond,
            "is_single_knesset": is_single,
            "knesset_title": knesset_title,
            "faction_condition": self._build_faction_condition(),

            # Named conditions for specific filter types
            "query_type_condition": self._build_in_condition(
                f"{self.prefix}TypeDesc", self._query_type_filter
            ),
            "query_status_condition": self._build_in_condition(
                's."Desc"', self._query_status_filter
            ),
            "session_type_condition": self._build_in_condition(
                f"{self.prefix}SubTypeDesc", self._session_type_filter
            ),
            "bill_type_condition": self._build_in_condition(
                f"{self.prefix}SubTypeDesc", self._bill_type_filter
            ),
            "bill_status_condition": self._build_in_condition(
                's."Desc"', self._bill_status_filter
            ),

            # Bill origin filter
            "bill_origin_condition": "1=1",

            # Date conditions
            "start_date_condition": "1=1",
            "end_date_condition": "1=1",
        }

        # Bill origin filter
        if self._bill_origin_filter == self.BILL_ORIGIN_PRIVATE:
            filters["bill_origin_condition"] = f"{self.prefix}PrivateNumber IS NOT NULL"
        elif self._bill_origin_filter == self.BILL_ORIGIN_GOVERNMENTAL:
            filters["bill_origin_condition"] = f"{self.prefix}PrivateNumber IS NULL"

        # Date filters
        if self._start_date:
            filters["start_date_condition"] = (
                f"{self.prefix}{self.date_column} >= '{self._escape_sql_string(self._start_date)}'"
            )
        if self._end_date:
            filters["end_date_condition"] = (
                f"date({self.prefix}{self.date_column}) <= '{self._escape_sql_string(self._end_date)}'"
            )

        return filters
