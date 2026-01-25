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

                # Create performance indexes
                self._ensure_indexes(conn)

                self._tables_initialized = True
                self.logger.info("CAP annotation tables created/verified successfully")
                return True

        except Exception as e:
            self.logger.error(f"Error creating CAP tables: {e}", exc_info=True)
            return False

    def _ensure_indexes(self, conn) -> None:
        """
        Ensure performance indexes exist on UserBillCAP table.

        Idempotent - IF NOT EXISTS handles already-created indexes.
        These indexes significantly speed up queries that filter by:
        - BillID (single bill lookups)
        - ResearcherID (researcher-specific queries)
        - BillID + ResearcherID (unique constraint queries)
        - AssignedDate (sorting by recency)
        """
        indexes = [
            ("idx_userbillcap_billid", "CREATE INDEX IF NOT EXISTS idx_userbillcap_billid ON UserBillCAP(BillID)"),
            ("idx_userbillcap_researcherid", "CREATE INDEX IF NOT EXISTS idx_userbillcap_researcherid ON UserBillCAP(ResearcherID)"),
            ("idx_userbillcap_bill_researcher", "CREATE INDEX IF NOT EXISTS idx_userbillcap_bill_researcher ON UserBillCAP(BillID, ResearcherID)"),
            ("idx_userbillcap_assigneddate", "CREATE INDEX IF NOT EXISTS idx_userbillcap_assigneddate ON UserBillCAP(AssignedDate DESC)"),
        ]

        for index_name, create_sql in indexes:
            try:
                conn.execute(create_sql)
                self.logger.debug(f"Ensured index: {index_name}")
            except Exception as e:
                self.logger.warning(f"Could not create index {index_name}: {e}")

    def _cleanup_migration_artifacts(self, conn) -> None:
        """
        Clean up any leftover temporary tables from interrupted migrations.

        If a migration was interrupted (e.g., app restart, network error),
        there may be leftover _new tables that cause "table does not exist"
        errors. This method detects and handles these cases.

        Args:
            conn: Active DuckDB connection
        """
        try:
            # Check for leftover UserBillCAP_new table
            new_table_exists = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP_new'"
            ).fetchone()

            if new_table_exists:
                # Check if main table also exists
                main_table_exists = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
                ).fetchone()

                if main_table_exists:
                    # Both tables exist - drop the _new table (migration was interrupted after copy)
                    self.logger.warning(
                        "Found leftover UserBillCAP_new table from interrupted migration - dropping it"
                    )
                    conn.execute("DROP TABLE IF EXISTS UserBillCAP_new")
                else:
                    # Only _new exists - rename it to main table (migration interrupted after drop)
                    self.logger.warning(
                        "Found UserBillCAP_new without UserBillCAP - completing interrupted migration"
                    )
                    conn.execute("ALTER TABLE UserBillCAP_new RENAME TO UserBillCAP")

        except Exception as e:
            self.logger.warning(f"Error cleaning up migration artifacts: {e}")
            # Don't fail - let the normal migration process handle any issues

    def _migrate_to_multi_annotator(self, conn) -> None:
        """
        Migrate existing annotations to multi-annotator schema.

        Old schema: BillID as PRIMARY KEY (one annotation per bill)
        New schema: AnnotationID as PRIMARY KEY, UNIQUE(BillID, ResearcherID)
                    allowing multiple researchers to annotate the same bill.

        Args:
            conn: Active DuckDB connection
        """
        # Cleanup any leftover temporary tables from interrupted migrations
        self._cleanup_migration_artifacts(conn)

        # Check if UserBillCAP table exists
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
        ).fetchdf()

        if tables.empty:
            # Table doesn't exist, create with new schema
            self.logger.info("Creating UserBillCAP table with multi-annotator schema")
            # Create sequence for auto-increment (DuckDB doesn't auto-increment INTEGER PRIMARY KEY)
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1
            """)
            conn.execute("""
                CREATE TABLE UserBillCAP (
                    AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                    BillID INTEGER NOT NULL,
                    ResearcherID INTEGER NOT NULL,
                    CAPMinorCode INTEGER NOT NULL,
                    Direction INTEGER NOT NULL DEFAULT 0,
                    AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    Confidence VARCHAR DEFAULT 'Medium',
                    Notes VARCHAR,
                    Source VARCHAR DEFAULT 'Database',
                    SubmissionDate VARCHAR,
                    FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
                    FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
                    UNIQUE(BillID, ResearcherID)
                )
            """)
            return

        # Check if migration is needed (look for ResearcherID column)
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'UserBillCAP'"
        ).fetchdf()

        column_names = columns["column_name"].str.lower().tolist()
        self.logger.info(f"UserBillCAP columns (lowercase): {column_names}")

        if "researcherid" in column_names:
            self.logger.info("UserBillCAP already has multi-annotator schema - skipping migration")
            return

        # ALSO check if AssignedBy column exists - if not, we can't migrate
        if "assignedby" not in column_names:
            self.logger.warning(
                "UserBillCAP has neither ResearcherID nor AssignedBy column! "
                "This is an invalid state. Skipping migration to avoid errors."
            )
            return

        # Migration needed: AssignedBy (string) -> ResearcherID (FK)
        self.logger.info("Migrating UserBillCAP to multi-annotator schema...")

        # Check if there's any existing data
        existing_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM UserBillCAP"
        ).fetchone()[0]

        if existing_count == 0:
            # No data, just drop and recreate
            self.logger.info("No existing annotations, recreating table with new schema")
            conn.execute("DROP TABLE UserBillCAP")
            # Create sequence for auto-increment (DuckDB doesn't auto-increment INTEGER PRIMARY KEY)
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1
            """)
            conn.execute("""
                CREATE TABLE UserBillCAP (
                    AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                    BillID INTEGER NOT NULL,
                    ResearcherID INTEGER NOT NULL,
                    CAPMinorCode INTEGER NOT NULL,
                    Direction INTEGER NOT NULL DEFAULT 0,
                    AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    Confidence VARCHAR DEFAULT 'Medium',
                    Notes VARCHAR,
                    Source VARCHAR DEFAULT 'Database',
                    SubmissionDate VARCHAR,
                    FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
                    FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
                    UNIQUE(BillID, ResearcherID)
                )
            """)
            return

        # Has existing data - migrate carefully
        self.logger.info(f"Migrating {existing_count} existing annotations...")

        # SAFETY CHECK: Verify AssignedBy column actually exists before migration
        if "assignedby" not in column_names:
            self.logger.error(
                f"Cannot migrate: AssignedBy column not found. "
                f"Available columns: {column_names}. Skipping migration."
            )
            return

        # Create sequence for auto-increment (DuckDB doesn't auto-increment INTEGER PRIMARY KEY)
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1
        """)

        # Drop any leftover UserBillCAP_new from previous failed attempts
        try:
            conn.execute("DROP TABLE IF EXISTS UserBillCAP_new")
            self.logger.debug("Dropped any leftover UserBillCAP_new table")
        except Exception as e:
            self.logger.debug(f"No leftover UserBillCAP_new to drop: {e}")

        # Create new table with proper schema
        conn.execute("""
            CREATE TABLE UserBillCAP_new (
                AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                BillID INTEGER NOT NULL,
                ResearcherID INTEGER NOT NULL,
                CAPMinorCode INTEGER NOT NULL,
                Direction INTEGER NOT NULL DEFAULT 0,
                AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Confidence VARCHAR DEFAULT 'Medium',
                Notes VARCHAR,
                Source VARCHAR DEFAULT 'Database',
                SubmissionDate VARCHAR,
                FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
                FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
                UNIQUE(BillID, ResearcherID)
            )
        """)

        # Migrate data: lookup ResearcherID from AssignedBy display name
        # If no match found, use the first admin (ID 1) as fallback
        conn.execute("""
            INSERT INTO UserBillCAP_new
            (BillID, ResearcherID, CAPMinorCode, Direction, AssignedDate,
             Confidence, Notes, Source, SubmissionDate)
            SELECT
                old.BillID,
                COALESCE(r.ResearcherID, 1) AS ResearcherID,
                old.CAPMinorCode,
                COALESCE(old.Direction, 0),
                old.AssignedDate,
                COALESCE(old.Confidence, 'Medium'),
                old.Notes,
                COALESCE(old.Source, 'Database'),
                old.SubmissionDate
            FROM UserBillCAP old
            LEFT JOIN UserResearchers r ON old.AssignedBy = r.DisplayName
        """)

        # Verify migration
        new_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM UserBillCAP_new"
        ).fetchone()[0]

        if new_count != existing_count:
            # Rollback
            conn.execute("DROP TABLE UserBillCAP_new")
            raise RuntimeError(
                f"Migration verification failed: expected {existing_count}, got {new_count}"
            )

        # Swap tables
        conn.execute("DROP TABLE UserBillCAP")
        conn.execute("ALTER TABLE UserBillCAP_new RENAME TO UserBillCAP")

        self.logger.info(
            f"Successfully migrated {new_count} annotations to multi-annotator schema"
        )

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

                if result is not None:
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
        return majors[["MajorCode", "MajorTopic_HE", "MajorTopic_EN"]].to_dict(
            "records"
        )

    def get_minor_categories(
        self, major_code: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get list of minor categories, optionally filtered by major code."""
        taxonomy = self.get_taxonomy()
        if taxonomy.empty:
            return []

        if major_code is not None:
            taxonomy = taxonomy[taxonomy["MajorCode"] == major_code]

        return taxonomy.to_dict("records")

    def clear_cache(self) -> None:
        """Clear the taxonomy cache."""
        self._taxonomy_cache = None
