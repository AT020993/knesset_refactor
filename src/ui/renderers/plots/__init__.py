"""Plots page rendering package.

This package contains components for the predefined visualizations page:
- PlotFilterPanels: Filter panel rendering for different chart types
- PlotsPageRenderer: Main page orchestration (in plots_page.py, parent directory)

For backward compatibility, PlotsPageRenderer remains in the parent directory.
"""

from .filter_panels import PlotFilterPanels

__all__ = [
    'PlotFilterPanels',
]
