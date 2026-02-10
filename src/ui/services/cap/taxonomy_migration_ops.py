"""Schema migration and index ops for CAP taxonomy service."""

from __future__ import annotations

from typing import Any


def ensure_indexes(service: Any, conn) -> None:
    """Ensure performance indexes exist."""
    indexes = [
        (
            "idx_userbillcap_billid",
            "CREATE INDEX IF NOT EXISTS idx_userbillcap_billid ON UserBillCAP(BillID)",
        ),
        (
            "idx_userbillcap_researcherid",
            "CREATE INDEX IF NOT EXISTS idx_userbillcap_researcherid ON UserBillCAP(ResearcherID)",
        ),
        (
            "idx_userbillcap_bill_researcher",
            "CREATE INDEX IF NOT EXISTS idx_userbillcap_bill_researcher ON UserBillCAP(BillID, ResearcherID)",
        ),
        (
            "idx_userbillcap_assigneddate",
            "CREATE INDEX IF NOT EXISTS idx_userbillcap_assigneddate ON UserBillCAP(AssignedDate DESC)",
        ),
    ]

    for index_name, create_sql in indexes:
        try:
            conn.execute(create_sql)
            service.logger.debug(f"Ensured index: {index_name}")
        except Exception as exc:
            service.logger.warning(f"Could not create index {index_name}: {exc}")


def cleanup_migration_artifacts(service: Any, conn) -> None:
    """Remove leftover temporary tables from interrupted migrations."""
    try:
        new_table_exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP_new'"
        ).fetchone()

        if new_table_exists:
            main_table_exists = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
            ).fetchone()

            if main_table_exists:
                service.logger.warning(
                    "Found leftover UserBillCAP_new table from interrupted migration - dropping it"
                )
                conn.execute("DROP TABLE IF EXISTS UserBillCAP_new")
            else:
                service.logger.warning(
                    "Found UserBillCAP_new without UserBillCAP - completing interrupted migration"
                )
                conn.execute("ALTER TABLE UserBillCAP_new RENAME TO UserBillCAP")
    except Exception as exc:
        service.logger.warning(f"Error cleaning up migration artifacts: {exc}")


def migrate_to_multi_annotator(service: Any, conn) -> None:
    """Migrate from single-annotator to multi-annotator schema."""
    cleanup_migration_artifacts(service, conn)

    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
    ).fetchdf()

    if tables.empty:
        service.logger.info("Creating UserBillCAP table with multi-annotator schema")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1")
        conn.execute(
            """
            CREATE TABLE UserBillCAP (
                AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                BillID INTEGER NOT NULL,
                ResearcherID INTEGER NOT NULL,
                CAPMinorCode INTEGER NOT NULL,
                AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Confidence VARCHAR DEFAULT 'Medium',
                Notes VARCHAR,
                Source VARCHAR DEFAULT 'Database',
                SubmissionDate VARCHAR,
                FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
                FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
                UNIQUE(BillID, ResearcherID)
            )
            """
        )
        return

    columns = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'UserBillCAP'"
    ).fetchdf()
    column_names = columns["column_name"].str.lower().tolist()
    service.logger.info(f"UserBillCAP columns (lowercase): {column_names}")

    if "researcherid" in column_names:
        service.logger.info(
            "UserBillCAP already has multi-annotator schema - skipping migration"
        )
        return

    if "assignedby" not in column_names:
        service.logger.warning(
            "UserBillCAP has neither ResearcherID nor AssignedBy column! "
            "This is an invalid state. Skipping migration to avoid errors."
        )
        return

    service.logger.info("Migrating UserBillCAP to multi-annotator schema...")
    existing_count = conn.execute("SELECT COUNT(*) as cnt FROM UserBillCAP").fetchone()[0]

    if existing_count == 0:
        service.logger.info("No existing annotations, recreating table with new schema")
        conn.execute("DROP TABLE UserBillCAP")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1")
        conn.execute(
            """
            CREATE TABLE UserBillCAP (
                AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                BillID INTEGER NOT NULL,
                ResearcherID INTEGER NOT NULL,
                CAPMinorCode INTEGER NOT NULL,
                AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Confidence VARCHAR DEFAULT 'Medium',
                Notes VARCHAR,
                Source VARCHAR DEFAULT 'Database',
                SubmissionDate VARCHAR,
                FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
                FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
                UNIQUE(BillID, ResearcherID)
            )
            """
        )
        return

    if "assignedby" not in column_names:
        service.logger.error(
            f"Cannot migrate: AssignedBy column not found. Available columns: {column_names}. "
            "Skipping migration."
        )
        return

    service.logger.info(f"Migrating {existing_count} existing annotations...")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1")

    try:
        conn.execute("DROP TABLE IF EXISTS UserBillCAP_new")
        service.logger.debug("Dropped any leftover UserBillCAP_new table")
    except Exception as exc:
        service.logger.debug(f"No leftover UserBillCAP_new to drop: {exc}")

    conn.execute(
        """
        CREATE TABLE UserBillCAP_new (
            AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
            BillID INTEGER NOT NULL,
            ResearcherID INTEGER NOT NULL,
            CAPMinorCode INTEGER NOT NULL,
            AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Confidence VARCHAR DEFAULT 'Medium',
            Notes VARCHAR,
            Source VARCHAR DEFAULT 'Database',
            SubmissionDate VARCHAR,
            FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
            FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
            UNIQUE(BillID, ResearcherID)
        )
        """
    )

    conn.execute(
        """
        INSERT INTO UserBillCAP_new
        (BillID, ResearcherID, CAPMinorCode, AssignedDate,
         Confidence, Notes, Source, SubmissionDate)
        SELECT
            old.BillID,
            COALESCE(r.ResearcherID, 1) AS ResearcherID,
            old.CAPMinorCode,
            old.AssignedDate,
            COALESCE(old.Confidence, 'Medium'),
            old.Notes,
            COALESCE(old.Source, 'Database'),
            old.SubmissionDate
        FROM UserBillCAP old
        LEFT JOIN UserResearchers r ON old.AssignedBy = r.DisplayName
        """
    )

    new_count = conn.execute("SELECT COUNT(*) as cnt FROM UserBillCAP_new").fetchone()[0]
    if new_count != existing_count:
        conn.execute("DROP TABLE UserBillCAP_new")
        raise RuntimeError(
            f"Migration verification failed: expected {existing_count}, got {new_count}"
        )

    conn.execute("DROP TABLE UserBillCAP")
    conn.execute("ALTER TABLE UserBillCAP_new RENAME TO UserBillCAP")
    service.logger.info(
        f"Successfully migrated {new_count} annotations to multi-annotator schema"
    )


def remove_direction_column(service: Any, conn) -> None:
    """Remove deprecated Direction column from UserBillCAP."""
    columns = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'UserBillCAP'"
    ).fetchdf()
    column_names = columns["column_name"].str.lower().tolist()

    if "direction" not in column_names:
        service.logger.info("Direction column already removed - skipping migration")
        return

    service.logger.info("Removing Direction column from UserBillCAP...")
    existing_count = conn.execute("SELECT COUNT(*) FROM UserBillCAP").fetchone()[0]
    conn.execute("DROP TABLE IF EXISTS UserBillCAP_new")

    conn.execute(
        """
        CREATE TABLE UserBillCAP_new (
            AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
            BillID INTEGER NOT NULL,
            ResearcherID INTEGER NOT NULL,
            CAPMinorCode INTEGER NOT NULL,
            AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Confidence VARCHAR DEFAULT 'Medium',
            Notes VARCHAR,
            Source VARCHAR DEFAULT 'Database',
            SubmissionDate VARCHAR,
            FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
            FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
            UNIQUE(BillID, ResearcherID)
        )
        """
    )

    conn.execute(
        """
        INSERT INTO UserBillCAP_new
        (AnnotationID, BillID, ResearcherID, CAPMinorCode, AssignedDate,
         Confidence, Notes, Source, SubmissionDate)
        SELECT AnnotationID, BillID, ResearcherID, CAPMinorCode, AssignedDate,
               Confidence, Notes, Source, SubmissionDate
        FROM UserBillCAP
        """
    )

    new_count = conn.execute("SELECT COUNT(*) FROM UserBillCAP_new").fetchone()[0]
    if new_count != existing_count:
        conn.execute("DROP TABLE UserBillCAP_new")
        raise RuntimeError(f"Migration failed: expected {existing_count}, got {new_count}")

    conn.execute("DROP TABLE UserBillCAP")
    conn.execute("ALTER TABLE UserBillCAP_new RENAME TO UserBillCAP")
    service.logger.info(f"Removed Direction column ({new_count} annotations preserved)")
