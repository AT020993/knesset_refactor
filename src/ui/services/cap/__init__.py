"""
CAP Annotation Service Package

This package provides services for the Democratic Erosion bill annotation system,
allowing researchers to classify bills according to a specialized codebook.

Modules:
- taxonomy: Taxonomy operations (table creation, CSV loading, category lookups)
- repository: CRUD operations for bill annotations
- statistics: Analytics and export functionality
- user_service: User authentication and management

For backward compatibility, the main CAPAnnotationService class is available
at the package level.
"""

from logging import Logger
from pathlib import Path

from .taxonomy import CAPTaxonomyService
from .repository import CAPAnnotationRepository
from .statistics import CAPStatisticsService

try:
    from .user_service import CAPUserService, get_user_service
except ModuleNotFoundError as exc:
    if exc.name != "bcrypt":
        raise

    class CAPUserService:  # type: ignore[no-redef]
        """Placeholder when optional dependency bcrypt is unavailable."""

    def get_user_service(
        db_path: Path, logger_obj: Logger | None = None
    ) -> CAPUserService:  # type: ignore[no-redef]
        raise ModuleNotFoundError(
            "CAP user service requires optional dependency 'bcrypt'."
        ) from exc

__all__ = [
    "CAPTaxonomyService",
    "CAPAnnotationRepository",
    "CAPStatisticsService",
    "CAPUserService",
    "get_user_service",
]
