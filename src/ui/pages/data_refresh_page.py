"""Main data refresh page UI components.

This module is a facade for backward compatibility.
The actual implementation has been moved to the data_refresh/ package.

All imports from this module will continue to work:
    from src.ui.pages.data_refresh_page import DataRefreshPageRenderer

For new code, you can also import directly from the package:
    from src.ui.pages.data_refresh.page import DataRefreshPageRenderer
"""

# Re-export DataRefreshPageRenderer from the package for backward compatibility
from .data_refresh import DataRefreshPageRenderer

__all__ = ['DataRefreshPageRenderer']
