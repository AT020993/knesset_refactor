"""Comparison charts package.

This package contains comparison and faction analysis charts including:
- Queries per faction
- Bills per faction
- Coalition status comparisons
- Top initiators
- Ministry comparisons

For backward compatibility, all classes are re-exported at the package level.
"""

from .charts import ComparisonCharts

__all__ = ['ComparisonCharts']
