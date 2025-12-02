"""Network and connection chart generators.

This module is a facade for backward compatibility.
The actual implementation has been moved to the network/ package.

All imports from this module will continue to work:
    from src.ui.charts.network import NetworkCharts

For new code, you can also import directly from the package:
    from src.ui.charts.network.charts import NetworkCharts
"""

# Re-export NetworkCharts from the package for backward compatibility
from .network import NetworkCharts

# Re-export any other public symbols if they exist
__all__ = ['NetworkCharts']
