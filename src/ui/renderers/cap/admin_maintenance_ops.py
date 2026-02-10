"""Database maintenance operations for CAP admin renderer."""

from __future__ import annotations

import os
import shutil
import tempfile
from typing import Any

import duckdb
import streamlit as st


def run_full_catalog_rebuild(renderer: Any) -> None:
    """Completely rebuild database catalog using EXPORT/IMPORT."""
    st.info("Starting full catalog rebuild... This may take a moment.")
    db_path_str = str(renderer.db_path)

    export_dir = tempfile.mkdtemp(prefix="duckdb_export_")
    backup_path = db_path_str + ".backup"

    try:
        st.write("📤 Exporting database...")
        conn = duckdb.connect(db_path_str, read_only=False)
        try:
            conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
            st.write("✅ Export completed")
        finally:
            conn.close()

        st.write("💾 Backing up original database...")
        shutil.copy2(db_path_str, backup_path)

        st.write("🗑️ Removing original database...")
        os.remove(db_path_str)

        wal_path = db_path_str + ".wal"
        if os.path.exists(wal_path):
            os.remove(wal_path)

        st.write("📥 Creating fresh database and importing...")
        conn = duckdb.connect(db_path_str, read_only=False)
        try:
            conn.execute(f"IMPORT DATABASE '{export_dir}'")
            conn.execute("CHECKPOINT")
            st.write("✅ Import completed")

            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            st.write(f"✅ Verified {len(tables)} tables imported")

            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
                ).fetchone()
                count = int(row[0]) if row else 0
                st.write(f"✅ Test query succeeded (count={count})")
            except Exception as exc:
                st.error(f"❌ Test query failed: {exc}")

            if os.path.exists(backup_path):
                os.remove(backup_path)

            st.success(
                "✅ **Full catalog rebuild complete!** "
                "The database now has a clean catalog. Try your operation again."
            )

            st.markdown("---")
            if st.button("☁️ Sync Rebuilt DB to Cloud", key="btn_sync_rebuilt_to_cloud"):
                sync_repaired_db_to_cloud(renderer)
        except Exception as import_exc:
            st.error(f"❌ Import failed: {import_exc}")

            if os.path.exists(backup_path):
                st.write("⏮️ Restoring from backup...")
                if os.path.exists(db_path_str):
                    os.remove(db_path_str)
                shutil.move(backup_path, db_path_str)
                st.warning("Database restored from backup.")
            raise
        finally:
            if conn:
                conn.close()
    except Exception as exc:
        import traceback

        st.error(f"❌ Full catalog rebuild failed: {exc}")
        st.code(traceback.format_exc())
        renderer.logger.error(f"Full catalog rebuild error: {exc}", exc_info=True)
    finally:
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir, ignore_errors=True)


def run_database_repair(renderer: Any) -> None:
    """Run broad diagnostic and repair flow for migration/catalog artifacts."""
    st.info("Running comprehensive database repair...")
    issues_found = []
    fixes_applied = []

    try:
        conn = duckdb.connect(str(renderer.db_path), read_only=False)
        try:
            all_tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            fixes_applied.append(
                f"Found {len(all_tables)} tables: {[t[0] for t in all_tables]}"
            )

            for (table_name,) in all_tables:
                if table_name.endswith("_new"):
                    issues_found.append(f"Found migration artifact table: {table_name}")
                    try:
                        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        fixes_applied.append(f"Dropped table: {table_name}")
                    except Exception as exc:
                        fixes_applied.append(f"Failed to drop {table_name}: {exc}")

            try:
                views = conn.execute(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'main'"
                ).fetchall()
                if views:
                    fixes_applied.append(f"Found views: {[v[0] for v in views]}")
                    for (view_name,) in views:
                        try:
                            view_def = conn.execute(
                                f"SELECT view_definition FROM information_schema.views WHERE table_name = '{view_name}'"
                            ).fetchone()
                            if view_def and "_new" in str(view_def[0]):
                                issues_found.append(
                                    f"View {view_name} references _new table!"
                                )
                                conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                                fixes_applied.append(
                                    f"Dropped problematic view: {view_name}"
                                )
                        except Exception as exc:
                            fixes_applied.append(f"Error checking view {view_name}: {exc}")
                else:
                    fixes_applied.append("No views found")
            except Exception as exc:
                fixes_applied.append(f"Could not check views: {exc}")

            try:
                deps = conn.execute("SELECT * FROM duckdb_dependencies()").fetchall()
                new_deps = [dep for dep in deps if "_new" in str(dep)]
                if new_deps:
                    issues_found.append(f"Found dependencies with _new: {new_deps}")
            except Exception as exc:
                fixes_applied.append(f"Could not check dependencies: {exc}")

            try:
                seqs = conn.execute(
                    "SELECT sequence_name FROM duckdb_sequences()"
                ).fetchall()
                fixes_applied.append(f"Sequences: {[s[0] for s in seqs]}")
            except Exception as exc:
                fixes_applied.append(f"Could not list sequences: {exc}")

            try:
                conn.execute("DROP TABLE IF EXISTS UserBillCAP_new CASCADE")
                fixes_applied.append(
                    "Executed DROP TABLE IF EXISTS UserBillCAP_new CASCADE"
                )
            except Exception as exc:
                fixes_applied.append(f"DROP UserBillCAP_new: {exc}")

            try:
                cols = conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'UserBillCAP' ORDER BY ordinal_position"
                ).fetchall()
                fixes_applied.append(f"UserBillCAP columns: {[c[0] for c in cols]}")
            except Exception as exc:
                issues_found.append(f"Could not read UserBillCAP structure: {exc}")

            try:
                constraints = conn.execute(
                    "SELECT constraint_name, constraint_type "
                    "FROM information_schema.table_constraints "
                    "WHERE table_name = 'UserBillCAP'"
                ).fetchall()
                fixes_applied.append(f"UserBillCAP constraints: {constraints}")
            except Exception as exc:
                fixes_applied.append(f"Could not check constraints: {exc}")

            try:
                conn.execute("CHECKPOINT")
                conn.execute("VACUUM")
                fixes_applied.append("CHECKPOINT and VACUUM completed")
            except Exception as exc:
                fixes_applied.append(f"CHECKPOINT/VACUUM: {exc}")

            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
                ).fetchone()
                count = int(row[0]) if row else 0
                fixes_applied.append(f"✅ Test query succeeded (count={count})")
            except Exception as exc:
                issues_found.append(f"❌ Test query FAILED: {exc}")

            try:
                triggers = conn.execute(
                    "SELECT * FROM duckdb_constraints() WHERE constraint_type = 'TRIGGER'"
                ).fetchall()
                if triggers:
                    fixes_applied.append(f"Triggers found: {triggers}")
                else:
                    fixes_applied.append("No triggers found")
            except Exception as exc:
                fixes_applied.append(f"Could not check triggers: {exc}")

            try:
                all_objects = conn.execute(
                    """
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_name LIKE '%UserBillCAP%' OR table_name LIKE '%userbillcap%'
                    """
                ).fetchall()
                fixes_applied.append(f"Objects matching UserBillCAP: {all_objects}")
            except Exception as exc:
                fixes_applied.append(f"Could not list UserBillCAP objects: {exc}")

            try:
                conn.execute("FORCE CHECKPOINT")
                fixes_applied.append("FORCE CHECKPOINT completed")
            except Exception as exc:
                fixes_applied.append(f"FORCE CHECKPOINT: {exc}")

            try:
                table_exists = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
                ).fetchone()

                if table_exists:
                    row = conn.execute("SELECT COUNT(*) FROM UserBillCAP").fetchone()
                    row_count = int(row[0]) if row else 0
                    has_data = row_count > 0

                    if has_data:
                        conn.execute("CREATE TABLE UserBillCAP_backup AS SELECT * FROM UserBillCAP")
                        fixes_applied.append(
                            f"Backed up UserBillCAP data ({row_count} rows)"
                        )

                    conn.execute("DROP TABLE IF EXISTS UserBillCAP CASCADE")
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
                            UNIQUE(BillID, ResearcherID)
                        )
                        """
                    )

                    if has_data:
                        conn.execute(
                            """
                            INSERT INTO UserBillCAP
                            (AnnotationID, BillID, ResearcherID, CAPMinorCode,
                             AssignedDate, Confidence, Notes, Source, SubmissionDate)
                            SELECT AnnotationID, BillID, ResearcherID, CAPMinorCode,
                                   AssignedDate, Confidence, Notes, Source, SubmissionDate
                            FROM UserBillCAP_backup
                            """
                        )
                        conn.execute("DROP TABLE UserBillCAP_backup")
                        fixes_applied.append(
                            f"✅ Rebuilt UserBillCAP table with {row_count} rows restored"
                        )
                    else:
                        fixes_applied.append("✅ Rebuilt empty UserBillCAP table")

                    conn.execute("FORCE CHECKPOINT")
                    fixes_applied.append("Final CHECKPOINT after rebuild")
                else:
                    fixes_applied.append(
                        "UserBillCAP table does not exist - nothing to rebuild"
                    )
            except Exception as exc:
                import traceback

                fixes_applied.append(f"❌ Table rebuild failed: {exc}")
                fixes_applied.append(f"Traceback: {traceback.format_exc()[:500]}")

            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
                ).fetchone()
                count = int(row[0]) if row else 0
                fixes_applied.append(
                    f"✅ Final test query after rebuild succeeded (count={count})"
                )
            except Exception as exc:
                issues_found.append(f"❌ Final test query FAILED after rebuild: {exc}")
        finally:
            conn.close()

        if issues_found:
            st.warning("**Issues found:**")
            for issue in issues_found:
                st.write(f"- {issue}")
        else:
            st.success("**No issues found!**")

        st.info("**Diagnostic info:**")
        for fix in fixes_applied:
            st.write(f"- {fix}")

        st.info("Please try your operation again.")

        st.markdown("---")
        st.markdown("**Sync repaired database to cloud?**")
        st.caption(
            "If you're on Streamlit Cloud, sync the repaired database to GCS "
            "so the fix persists across reboots."
        )
        if st.button("☁️ Sync to Cloud", key="btn_sync_repair_to_cloud"):
            sync_repaired_db_to_cloud(renderer)
    except Exception as exc:
        import traceback

        st.error(f"Database repair failed: {exc}")
        st.code(traceback.format_exc())
        renderer.logger.error(f"Database repair error: {exc}", exc_info=True)


def sync_repaired_db_to_cloud(renderer: Any) -> None:
    """Upload repaired database to cloud storage."""
    try:
        from config.settings import Settings
        from data.services.storage_sync_service import StorageSyncService

        sync_service = StorageSyncService(logger_obj=renderer.logger)
        if not sync_service.is_enabled():
            st.warning("Cloud storage is not enabled. No sync needed for local development.")
            return
        if sync_service.gcs_manager is None:
            st.error("Cloud storage manager is unavailable.")
            return

        with st.spinner("Uploading repaired database to cloud..."):
            success = sync_service.gcs_manager.upload_file(
                Settings.DEFAULT_DB_PATH, "data/warehouse.duckdb"
            )

        if success:
            st.success(
                "✅ Repaired database synced to cloud! The fix will persist across reboots."
            )
        else:
            st.error("❌ Failed to sync to cloud. Check logs for details.")
    except Exception as exc:
        st.error(f"Cloud sync failed: {exc}")
        renderer.logger.error(f"Cloud sync error: {exc}", exc_info=True)
