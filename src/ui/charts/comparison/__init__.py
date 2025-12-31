"""Comparison charts package.

This package contains comparison and faction analysis charts including:
- Queries per faction
- Bills per faction
- Coalition status comparisons
- Top initiators
- Ministry comparisons

The charts have been refactored into separate modules for better maintainability:
- query_charts.py - QueryComparisonCharts class
- agenda_charts.py - AgendaComparisonCharts class
- bill_charts.py - BillComparisonCharts class

For backward compatibility, ComparisonCharts is still exported and delegates to the new classes.
"""

# Backward compatible facade
from .charts import ComparisonCharts

# Individual chart classes for direct usage
from .query_charts import QueryComparisonCharts
from .agenda_charts import AgendaComparisonCharts
from .bill_charts import BillComparisonCharts

__all__ = [
    # Main facade (backward compatible)
    'ComparisonCharts',
    # Individual chart classes
    'QueryComparisonCharts',
    'AgendaComparisonCharts',
    'BillComparisonCharts',
]
