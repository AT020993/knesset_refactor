"""
CAP Annotation Renderers Package

Provides modular components for the CAP annotation system.
Refactored for better separation of concerns and maintainability.

Components:
- CAPAuthHandler: Authentication and session management
- CAPStatsRenderer: Statistics and coverage dashboards
- CAPFormRenderer: Annotation form rendering
- CAPCodedBillsRenderer: Coded bills list and filtering
- CAPAdminRenderer: Admin panel for user management
- CAPBillQueueRenderer: Bill queue display and selection
- CAPPDFViewer: Embedded PDF document viewing
- CAPCategorySelector: Hierarchical category selection UI
"""

from ui.renderers.cap.auth_handler import CAPAuthHandler
from ui.renderers.cap.stats_renderer import CAPStatsRenderer
from ui.renderers.cap.form_renderer import CAPFormRenderer
from ui.renderers.cap.coded_bills_renderer import CAPCodedBillsRenderer
from ui.renderers.cap.admin_renderer import CAPAdminRenderer
from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer
from ui.renderers.cap.pdf_viewer import CAPPDFViewer
from ui.renderers.cap.category_selector import CAPCategorySelector

__all__ = [
    "CAPAuthHandler",
    "CAPStatsRenderer",
    "CAPFormRenderer",
    "CAPCodedBillsRenderer",
    "CAPAdminRenderer",
    "CAPBillQueueRenderer",
    "CAPPDFViewer",
    "CAPCategorySelector",
]
