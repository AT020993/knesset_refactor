"""Queries package.

This package contains query-related utilities:
- filter_builder: FilterBuilder class for building SQL WHERE conditions
- predefined_queries: Pre-built query templates for common data retrieval
- query_executor: Query execution with caching
- sql_templates: Reusable SQL CTEs and templates
"""

from .filter_builder import FilterBuilder

__all__ = ['FilterBuilder']
