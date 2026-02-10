"""
CAP Taxonomy Service

Handles the Democratic Erosion codebook taxonomy operations including:
- Table creation
- Loading taxonomy from CSV
- Category lookups (major and minor)
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

from backend.connection_manager import get_db_connection, safe_execute_query
from . import taxonomy_migration_ops


class CAPTaxonomyService:
    """Service for managing CAP taxonomy operations."""

    # Project root is 5 levels up from src/ui/services/cap/taxonomy.py
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
    TAXONOMY_FILE = PROJECT_ROOT / "data" / "taxonomies" / "democratic_erosion_codebook.csv"

    # Direction codes and labels
    DIRECTION_STRENGTHENING = 1
    DIRECTION_WEAKENING = -1
    DIRECTION_NEUTRAL = 0

    DIRECTION_LABELS = {
        1: ("הרחבה/חיזוק", "Strengthening/Expansion"),
        -1: ("צמצום/פגיעה", "Weakening/Restriction"),
        0: ("אחר", "Other/Neutral"),
    }

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the taxonomy service."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
        self._taxonomy_cache: Optional[pd.DataFrame] = None
        self._tables_initialized = False  # Instance-level flag to prevent re-initialization

    def ensure_tables_exist(self) -> bool:
        """
        Create the CAP annotation tables if they don't exist.

        Tables created:
        - UserCAPTaxonomy: The codebook taxonomy
        - UserBillCAP: Bill annotations (supports multiple annotations per bill)
        - UserResearchers: Researcher accounts
        - _SyncMetadata: Internal table for tracking sync timestamps

        Returns:
            True if successful, False otherwise
        """
        # Skip if already initialized in this instance
        if self._tables_initialized:
            self.logger.debug("Tables already initialized in this instance - skipping")
            return True

        self.logger.info("ensure_tables_exist() called - checking database state...")

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                # Create sync metadata table (for tracking sync timestamps)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS _SyncMetadata (
                        Key VARCHAR PRIMARY KEY,
                        Value VARCHAR,
                        UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Create taxonomy table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS UserCAPTaxonomy (
                        MajorCode INTEGER NOT NULL,
                        MajorTopic_HE VARCHAR NOT NULL,
                        MajorTopic_EN VARCHAR NOT NULL,
                        MinorCode INTEGER PRIMARY KEY,
                        MinorTopic_HE VARCHAR NOT NULL,
                        MinorTopic_EN VARCHAR NOT NULL,
                        Description_HE VARCHAR,
                        Examples_HE VARCHAR
                    )
                """)

                # Create researchers table for multi-user authentication
                # (Must exist before UserBillCAP for FK reference)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS UserResearchers (
                        ResearcherID INTEGER PRIMARY KEY,
                        Username VARCHAR NOT NULL UNIQUE,
                        DisplayName VARCHAR NOT NULL,
                        PasswordHash VARCHAR NOT NULL,
                        Role VARCHAR NOT NULL DEFAULT 'researcher',
                        IsActive BOOLEAN NOT NULL DEFAULT TRUE,
                        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        LastLoginAt TIMESTAMP,
                        CreatedBy VARCHAR
                    )
                """)

                # Check if we need to migrate from old schema
                self._migrate_to_multi_annotator(conn)

                # Remove Direction column if present (v2 simplification)
                self._remove_direction_column(conn)

                # Create performance indexes
                self._ensure_indexes(conn)

                self._tables_initialized = True
                self.logger.info("CAP annotation tables created/verified successfully")
                return True

        except Exception as e:
            self.logger.error(f"Error creating CAP tables: {e}", exc_info=True)
            return False

    def _ensure_indexes(self, conn) -> None:
        """Ensure performance indexes exist on UserBillCAP table."""
        taxonomy_migration_ops.ensure_indexes(self, conn)

    def _cleanup_migration_artifacts(self, conn) -> None:
        """Clean up any leftover temporary tables from interrupted migrations."""
        taxonomy_migration_ops.cleanup_migration_artifacts(self, conn)

    def _migrate_to_multi_annotator(self, conn) -> None:
        """Migrate existing annotations to multi-annotator schema."""
        taxonomy_migration_ops.migrate_to_multi_annotator(self, conn)

    def _remove_direction_column(self, conn) -> None:
        """Remove Direction column from UserBillCAP via safe table swap."""
        taxonomy_migration_ops.remove_direction_column(self, conn)

    def load_taxonomy_from_csv(self) -> bool:
        """
        Load the taxonomy from CSV file into the database.

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.TAXONOMY_FILE.exists():
                self.logger.error(f"Taxonomy file not found: {self.TAXONOMY_FILE}")
                return False

            df = pd.read_csv(self.TAXONOMY_FILE, encoding="utf-8")

            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                for _, row in df.iterrows():
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO UserCAPTaxonomy
                        (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode,
                         MinorTopic_HE, MinorTopic_EN, Description_HE, Examples_HE)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        [
                            row["MajorCode"],
                            row["MajorTopic_HE"],
                            row["MajorTopic_EN"],
                            row["MinorCode"],
                            row["MinorTopic_HE"],
                            row["MinorTopic_EN"],
                            row.get("Description_HE", ""),
                            row.get("Examples_HE", ""),
                        ],
                    )

                self.logger.info(f"Loaded {len(df)} taxonomy entries from CSV")
                self._taxonomy_cache = None
                return True

        except Exception as e:
            self.logger.error(f"Error loading taxonomy from CSV: {e}", exc_info=True)
            return False

    def get_taxonomy(self) -> pd.DataFrame:
        """
        Get the full taxonomy as a DataFrame.

        Returns:
            DataFrame with taxonomy entries
        """
        if self._taxonomy_cache is not None:
            return self._taxonomy_cache

        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = safe_execute_query(
                    conn,
                    """
                    SELECT * FROM UserCAPTaxonomy
                    ORDER BY MajorCode, MinorCode
                """,
                    self.logger,
                )

                if isinstance(result, pd.DataFrame):
                    self._taxonomy_cache = result
                    return result

        except Exception as e:
            self.logger.error(f"Error getting taxonomy: {e}", exc_info=True)

        return pd.DataFrame()

    def get_major_categories(self) -> List[Dict[str, Any]]:
        """Get list of major categories."""
        taxonomy = self.get_taxonomy()
        if taxonomy.empty:
            return []

        majors = (
            taxonomy.groupby(["MajorCode", "MajorTopic_HE", "MajorTopic_EN"])
            .first()
            .reset_index()
        )
        records = majors[["MajorCode", "MajorTopic_HE", "MajorTopic_EN"]].to_dict(
            "records"
        )
        return [{str(k): v for k, v in record.items()} for record in records]

    def get_minor_categories(
        self, major_code: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get list of minor categories, optionally filtered by major code."""
        taxonomy = self.get_taxonomy()
        if taxonomy.empty:
            return []

        if major_code is not None:
            taxonomy = taxonomy[taxonomy["MajorCode"] == major_code]

        records = taxonomy.to_dict("records")
        return [{str(k): v for k, v in record.items()} for record in records]

    def clear_cache(self) -> None:
        """Clear the taxonomy cache."""
        self._taxonomy_cache = None
