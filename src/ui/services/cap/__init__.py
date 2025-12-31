"""
CAP Annotation Service Package

This package provides services for the Democratic Erosion bill annotation system,
allowing researchers to classify bills according to a specialized codebook.

Modules:
- taxonomy: Taxonomy operations (table creation, CSV loading, category lookups)
- repository: CRUD operations for bill annotations
- statistics: Analytics and export functionality

For backward compatibility, the main CAPAnnotationService class is available
at the package level.
"""

from .taxonomy import CAPTaxonomyService
from .repository import CAPAnnotationRepository
from .statistics import CAPStatisticsService

__all__ = [
    "CAPTaxonomyService",
    "CAPAnnotationRepository",
    "CAPStatisticsService",
]
