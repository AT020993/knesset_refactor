"""Comparison and faction analysis chart generators.

This module is a facade for backward compatibility.
The actual implementation has been moved to the comparison/ package.

All imports from this module will continue to work:
    from src.ui.charts.comparison import ComparisonCharts

For new code, you can also import directly from the package:
    from src.ui.charts.comparison.charts import ComparisonCharts
"""

# Re-export ComparisonCharts from the package for backward compatibility
from .comparison import ComparisonCharts

__all__ = ['ComparisonCharts']
