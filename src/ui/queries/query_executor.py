"""Query execution logic with typed contracts and parameterized filtering."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from .predefined_queries import get_query_definition
from .types import PaginationSpec, QueryRequest


class QueryExecutor:
    """Handles execution of predefined queries with safe filtering."""

    def __init__(
        self,
        db_path: Path,
        connect_func: Optional[Callable[..., Any]],
        logger: logging.Logger,
    ):
        self.db_path = db_path
        self.connect_func = connect_func
        self.logger = logger

    def execute_query_with_filters(
        self,
        query_name: str,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[int]] = None,
        safe_execute_func: Optional[Callable[..., pd.DataFrame]] = None,
        document_type_filter: Optional[List[str]] = None,
        page_offset: int = 0,
    ) -> Tuple[pd.DataFrame, str, List[str]]:
        """Execute a predefined query with optional filters and pagination."""
        definition = get_query_definition(query_name)
        if not definition:
            self.logger.error("Query '%s' not found", query_name)
            return pd.DataFrame(), "", ["Error: Query not found"]

        request = QueryRequest(
            definition=definition,
            knesset_numbers=tuple(knesset_filter or []),
            faction_ids=tuple(faction_filter or []),
            document_types=tuple(document_type_filter or []),
            pagination=PaginationSpec(
                limit=self._extract_default_limit(definition.sql),
                offset=max(page_offset, 0),
            ),
        )

        sql, params, applied_filters = self._build_query(request)
        result_df = self._run_query(sql, params, safe_execute_func)
        self.logger.info(
            "Executed query '%s' with %d rows", query_name, len(result_df)
        )
        return result_df, sql, applied_filters

    def update_session_state_with_results(
        self,
        query_name: str,
        results_df: pd.DataFrame,
        executed_sql: str,
        applied_filters_info: List[str],
    ) -> None:
        """Update Streamlit session state with query results."""
        st.session_state.executed_query_name = query_name
        st.session_state.query_results_df = results_df
        st.session_state.last_executed_sql = executed_sql
        st.session_state.applied_filters_info_query = applied_filters_info
        st.session_state.show_query_results = True

    def execute_table_exploration(
        self,
        table_name: str,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[int]] = None,
        faction_display_map: Optional[Dict[str, int]] = None,
        safe_execute_func: Optional[Callable[..., pd.DataFrame]] = None,
    ) -> Tuple[pd.DataFrame, bool]:
        """Execute table exploration with optional filters."""
        try:
            base_query = f"SELECT * FROM {table_name}"
            where_conditions: list[str] = []
            params: list[Any] = []

            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                if knesset_filter:
                    placeholders = ", ".join(["?"] * len(knesset_filter))
                    where_conditions.append(f"KnessetNum IN ({placeholders})")
                    params.extend(knesset_filter)

                if faction_filter and faction_display_map:
                    where_conditions.append("FactionID IN (" + ", ".join(["?"] * len(faction_filter)) + ")")
                    params.extend(faction_filter)

                final_query = base_query
                if where_conditions:
                    final_query += " WHERE " + " AND ".join(where_conditions)
                final_query += " LIMIT 1000"

                execute = safe_execute_func or safe_execute_query
                results_df = execute(con, final_query, logger_obj=self.logger, params=params)

            return results_df, True
        except Exception as e:
            self.logger.error("Error exploring table '%s': %s", table_name, e, exc_info=True)
            return pd.DataFrame(), False

    def update_session_state_with_table_results(
        self,
        table_name: str,
        results_df: pd.DataFrame,
    ) -> None:
        """Update Streamlit session state with table exploration results."""
        st.session_state.executed_table_explorer_name = table_name
        st.session_state.table_explorer_df = results_df
        st.session_state.show_table_explorer_results = True

    def _run_query(
        self,
        sql: str,
        params: Sequence[Any],
        safe_execute_func: Optional[Callable[..., pd.DataFrame]],
    ) -> pd.DataFrame:
        execute = safe_execute_func or safe_execute_query

        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
            return execute(con, sql, logger_obj=self.logger, params=list(params))

    def _build_query(self, request: QueryRequest) -> tuple[str, list[Any], list[str]]:
        base_sql, default_limit = self._strip_trailing_limit(request.definition.sql)

        params: list[Any] = []
        conditions: list[str] = []
        applied_filters: list[str] = []

        if request.knesset_numbers and request.definition.knesset_filter_column:
            # Strip table alias (e.g. "B.KnessetNum" -> "KnessetNum") because
            # filters are applied outside the subquery where aliases aren't in scope
            col = self._strip_table_alias(request.definition.knesset_filter_column)
            clause, clause_params = self._build_in_clause(col, request.knesset_numbers)
            conditions.append(clause)
            params.extend(clause_params)
            applied_filters.append(
                f"KnessetNum IN ({', '.join(map(str, request.knesset_numbers))})"
            )

        faction_col = request.definition.faction_filter_column
        if request.faction_ids and faction_col and faction_col != "NULL":
            col = self._strip_table_alias(faction_col)
            clause, clause_params = self._build_in_clause(col, request.faction_ids)
            conditions.append(clause)
            params.extend(clause_params)
            applied_filters.append(
                f"FactionID IN ({', '.join(map(str, request.faction_ids))})"
            )

        document_clause = self._build_document_filter_clause(request.document_types)
        if document_clause:
            conditions.append(document_clause)
            applied_filters.append(
                f"Document Types: {', '.join(request.document_types)}"
            )

        query = f"SELECT * FROM ({base_sql}) AS base_query"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query_limit = request.pagination.limit or default_limit
        if query_limit:
            query += " LIMIT ?"
            params.append(query_limit)

        if request.pagination.offset > 0:
            query += " OFFSET ?"
            params.append(request.pagination.offset)
            applied_filters.append(f"Offset: {request.pagination.offset}")

        return query, params, applied_filters

    @staticmethod
    def _strip_table_alias(column: str) -> str:
        """Strip table alias prefix from a column name.

        E.g. "B.KnessetNum" -> "KnessetNum", "KnessetNum" -> "KnessetNum"
        """
        return column.rsplit(".", 1)[-1]

    @staticmethod
    def _build_in_clause(column: str, values: Sequence[int]) -> tuple[str, list[int]]:
        placeholders = ", ".join(["?"] * len(values))
        return f"{column} IN ({placeholders})", list(values)

    @staticmethod
    def _build_document_filter_clause(document_types: Sequence[str]) -> str:
        if not document_types:
            return ""

        doc_type_conditions: list[str] = []
        for doc_type in document_types:
            if doc_type == "Published Law":
                doc_type_conditions.append("BillPublishedLawDocCount > 0")
            elif doc_type == "First Reading":
                doc_type_conditions.append("BillFirstReadingDocCount > 0")
            elif doc_type in ["2nd/3rd Reading", "Second & Third Reading"]:
                doc_type_conditions.append("BillSecondThirdReadingDocCount > 0")
            elif doc_type in ["Early Discussion", "Early Stage Discussion"]:
                doc_type_conditions.append("BillEarlyDiscussionDocCount > 0")
            elif doc_type == "Other":
                doc_type_conditions.append("BillOtherDocCount > 0")

        if not doc_type_conditions:
            return ""
        return "(" + " OR ".join(doc_type_conditions) + ")"

    @staticmethod
    def _extract_default_limit(sql: str) -> int:
        _, default_limit = QueryExecutor._strip_trailing_limit(sql)
        return default_limit or 1000

    @staticmethod
    def _strip_trailing_limit(sql: str) -> tuple[str, int | None]:
        cleaned_sql = sql.strip().rstrip(";")
        limit_pos = QueryExecutor._find_top_level_keyword(cleaned_sql, "LIMIT")

        if limit_pos == -1:
            return cleaned_sql, None

        limit_section = cleaned_sql[limit_pos + len("LIMIT"):].strip()
        if not limit_section:
            return cleaned_sql, None

        first_token = limit_section.split()[0]
        try:
            limit_value = int(first_token)
        except ValueError:
            return cleaned_sql, None

        base_sql = cleaned_sql[:limit_pos].rstrip()
        return base_sql, limit_value

    @staticmethod
    def _find_top_level_keyword(sql: str, keyword: str) -> int:
        target = keyword.lower()
        sql_lower = sql.lower()

        depth = 0
        in_single = False
        in_double = False

        for idx, char in enumerate(sql):
            if char == "'" and not in_double:
                in_single = not in_single
                continue
            if char == '"' and not in_single:
                in_double = not in_double
                continue
            if in_single or in_double:
                continue

            if char == "(":
                depth += 1
                continue
            if char == ")":
                depth = max(depth - 1, 0)
                continue

            if depth != 0:
                continue

            if sql_lower.startswith(target, idx):
                prev_char = sql_lower[idx - 1] if idx > 0 else " "
                next_pos = idx + len(target)
                next_char = sql_lower[next_pos] if next_pos < len(sql_lower) else " "
                if not (prev_char.isalnum() or prev_char == "_") and not (
                    next_char.isalnum() or next_char == "_"
                ):
                    return idx

        return -1
