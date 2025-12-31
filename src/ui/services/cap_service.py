"""
CAP (Comparative Agendas Project) Annotation Service

This module provides services for the Democratic Erosion bill annotation system,
allowing researchers to classify bills according to a specialized codebook
for tracking legislation that affects democratic institutions and rights.

The codebook uses:
- Major categories (1=Government Institutions, 2=Civil Institutions, 3=Rights)
- Minor categories (101-108, 201-204, 301-306)
- Direction coding (+1=Strengthening, -1=Weakening, 0=Other)

This is a facade module that composes focused services from the cap/ package.
For specific functionality, you can also import directly from:
- ui.services.cap.taxonomy: CAPTaxonomyService
- ui.services.cap.repository: CAPAnnotationRepository
- ui.services.cap.statistics: CAPStatisticsService
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

# Import the focused service modules
from ui.services.cap.taxonomy import CAPTaxonomyService
from ui.services.cap.repository import CAPAnnotationRepository
from ui.services.cap.statistics import CAPStatisticsService


class CAPAnnotationService:
    """
    Facade service for managing CAP/Democratic Erosion bill annotations.

    This class composes three focused services:
    - CAPTaxonomyService: Taxonomy operations
    - CAPAnnotationRepository: CRUD operations
    - CAPStatisticsService: Analytics and export
    """

    # Re-export constants from taxonomy for backward compatibility
    TAXONOMY_FILE = CAPTaxonomyService.TAXONOMY_FILE
    DIRECTION_STRENGTHENING = CAPTaxonomyService.DIRECTION_STRENGTHENING
    DIRECTION_WEAKENING = CAPTaxonomyService.DIRECTION_WEAKENING
    DIRECTION_NEUTRAL = CAPTaxonomyService.DIRECTION_NEUTRAL
    DIRECTION_LABELS = CAPTaxonomyService.DIRECTION_LABELS

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the CAP annotation service."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)

        # Initialize composed services
        self._taxonomy = CAPTaxonomyService(db_path, self.logger)
        self._repository = CAPAnnotationRepository(db_path, self.logger)
        self._statistics = CAPStatisticsService(db_path, self.logger)

    # --- Taxonomy Operations (delegated to CAPTaxonomyService) ---

    def ensure_tables_exist(self) -> bool:
        """Create the CAP annotation tables if they don't exist."""
        return self._taxonomy.ensure_tables_exist()

    def load_taxonomy_from_csv(self) -> bool:
        """Load the taxonomy from CSV file into the database."""
        return self._taxonomy.load_taxonomy_from_csv()

    def get_taxonomy(self) -> pd.DataFrame:
        """Get the full taxonomy as a DataFrame."""
        return self._taxonomy.get_taxonomy()

    def get_major_categories(self) -> List[Dict[str, Any]]:
        """Get list of major categories."""
        return self._taxonomy.get_major_categories()

    def get_minor_categories(
        self, major_code: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get list of minor categories, optionally filtered by major code."""
        return self._taxonomy.get_minor_categories(major_code)

    def clear_cache(self) -> None:
        """Clear the taxonomy cache."""
        self._taxonomy.clear_cache()

    # --- CRUD Operations (delegated to CAPAnnotationRepository) ---

    def get_uncoded_bills(
        self, knesset_num: Optional[int] = None, limit: int = 100
    ) -> pd.DataFrame:
        """Get bills that haven't been coded yet."""
        return self._repository.get_uncoded_bills(knesset_num, limit)

    def get_coded_bills(
        self,
        knesset_num: Optional[int] = None,
        cap_code: Optional[int] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Get bills that have been coded."""
        return self._repository.get_coded_bills(knesset_num, cap_code, limit)

    def get_annotation_by_bill_id(self, bill_id: int) -> Optional[Dict[str, Any]]:
        """Get the full annotation details for a specific bill."""
        return self._repository.get_annotation_by_bill_id(bill_id)

    def save_annotation(
        self,
        bill_id: int,
        cap_minor_code: int,
        direction: int,
        assigned_by: str,
        confidence: str = "Medium",
        notes: str = "",
        source: str = "Database",
        submission_date: str = "",
    ) -> bool:
        """Save a bill annotation."""
        return self._repository.save_annotation(
            bill_id=bill_id,
            cap_minor_code=cap_minor_code,
            direction=direction,
            assigned_by=assigned_by,
            confidence=confidence,
            notes=notes,
            source=source,
            submission_date=submission_date,
        )

    def delete_annotation(self, bill_id: int) -> bool:
        """Delete an annotation for a bill."""
        return self._repository.delete_annotation(bill_id)

    def get_bills_not_in_database(
        self, api_bills: pd.DataFrame, limit: int = 100
    ) -> pd.DataFrame:
        """Filter API bills to only those not in the local database."""
        return self._repository.get_bills_not_in_database(api_bills, limit)

    # --- Statistics Operations (delegated to CAPStatisticsService) ---

    def get_annotation_stats(self) -> Dict[str, Any]:
        """Get statistics about annotations."""
        return self._statistics.get_annotation_stats()

    def export_annotations(self, output_path: Path) -> bool:
        """Export all annotations to CSV."""
        return self._statistics.export_annotations(output_path)

    def get_coverage_stats(self) -> Dict[str, Any]:
        """Get annotation coverage statistics."""
        return self._statistics.get_coverage_stats()


def get_cap_service(
    db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> CAPAnnotationService:
    """Factory function to get a CAP annotation service instance."""
    return CAPAnnotationService(db_path, logger_obj)
