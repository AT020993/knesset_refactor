"""Storage synchronization service for cloud persistence."""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Callable

from config.settings import Settings
from data.storage.cloud_storage import CloudStorageManager, create_gcs_manager_from_streamlit_secrets
from data.storage.credential_resolver import GCSCredentialResolver


class StorageSyncService:
    """
    Service for synchronizing data between local storage and cloud storage.

    Handles bidirectional sync of DuckDB database, Parquet files, CSV files,
    and resume state between local filesystem and Google Cloud Storage.

    Enhanced features:
    - Credential resolution from multiple sources (Streamlit, env vars, .env)
    - Sync metadata tracking for intelligent sync decisions
    - Backup before download to prevent data loss
    - Freshness comparison for smart sync direction
    """

    def __init__(
        self,
        gcs_manager: Optional[CloudStorageManager] = None,
        logger_obj: Optional[logging.Logger] = None
    ):
        """
        Initialize storage sync service.

        Args:
            gcs_manager: CloudStorageManager instance (if None, will try to create from various sources)
            logger_obj: Logger instance
        """
        self.logger = logger_obj or logging.getLogger(__name__)

        # Try to initialize GCS manager
        if gcs_manager:
            self.gcs_manager = gcs_manager
        else:
            self.gcs_manager = self._create_gcs_manager()

        self.enabled = self.gcs_manager is not None

        if self.enabled:
            self.logger.info("Storage sync service initialized with GCS")
        else:
            self.logger.info("Storage sync service initialized without GCS (disabled)")

    def _create_gcs_manager(self) -> Optional[CloudStorageManager]:
        """
        Create GCS manager from available credential sources.

        Tries multiple sources in priority order:
        1. Streamlit secrets (for cloud deployment)
        2. Environment variables (for local development)
        3. .env file (for convenience)

        Returns:
            CloudStorageManager instance or None if no credentials available
        """
        # First try the traditional Streamlit secrets path
        manager = create_gcs_manager_from_streamlit_secrets(self.logger)
        if manager:
            return manager

        # Fall back to credential resolver for env vars and .env file
        credentials, bucket_name = GCSCredentialResolver.resolve(self.logger)
        if credentials and bucket_name:
            try:
                return CloudStorageManager(
                    bucket_name=bucket_name,
                    credentials_dict=credentials,
                    logger_obj=self.logger
                )
            except Exception as e:
                self.logger.warning(f"Failed to create GCS manager from resolved credentials: {e}")

        return None

    def is_enabled(self) -> bool:
        """Check if cloud storage sync is enabled."""
        return self.enabled

    def download_all_data(
        self,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Dict[str, bool]:
        """
        Download all data from cloud storage to local filesystem.

        Downloads:
        - DuckDB database (warehouse.duckdb)
        - All Parquet files (parquet/*.parquet)
        - Faction coalition CSV (faction_coalition_status.csv)
        - Resume state (.resume_state.json)

        Args:
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with download results for each file type
        """
        if not self.enabled:
            self.logger.info("Cloud storage sync disabled, skipping download")
            return {}

        self.logger.info("Starting download from cloud storage...")
        results = {}

        try:
            # 1. Download DuckDB database
            if progress_callback:
                progress_callback("Downloading database...")

            db_success = self.gcs_manager.download_file(
                gcs_path="data/warehouse.duckdb",
                local_path=Settings.DEFAULT_DB_PATH
            )
            results['database'] = db_success

            # 2. Download Parquet files
            if progress_callback:
                progress_callback("Downloading Parquet files...")

            parquet_results = self.gcs_manager.download_directory(
                gcs_prefix="data/parquet",
                local_dir=Settings.PARQUET_DIR,
                include_patterns=['*.parquet']
            )
            results['parquet_files'] = parquet_results
            results['parquet_count'] = len([v for v in parquet_results.values() if v])

            # 3. Download faction coalition CSV
            if progress_callback:
                progress_callback("Downloading faction coalition data...")

            csv_success = self.gcs_manager.download_file(
                gcs_path="data/faction_coalition_status.csv",
                local_path=Settings.FACTION_COALITION_STATUS_FILE
            )
            results['faction_csv'] = csv_success

            # 4. Download resume state
            if progress_callback:
                progress_callback("Downloading resume state...")

            resume_success = self.gcs_manager.download_file(
                gcs_path="data/.resume_state.json",
                local_path=Settings.RESUME_STATE_FILE
            )
            results['resume_state'] = resume_success

            # Summary
            total_success = sum([
                db_success,
                len([v for v in parquet_results.values() if v]) > 0,
                csv_success,
                resume_success
            ])

            self.logger.info(f"Download complete: {total_success}/4 categories successful")

            if progress_callback:
                progress_callback(f"Download complete: {total_success}/4 categories synced")

            return results

        except Exception as e:
            self.logger.error(f"Error during download: {e}", exc_info=True)
            return {'error': str(e)}

    def upload_all_data(
        self,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Dict[str, bool]:
        """
        Upload all data from local filesystem to cloud storage.

        Uploads:
        - DuckDB database (warehouse.duckdb)
        - All Parquet files (parquet/*.parquet)
        - Faction coalition CSV (faction_coalition_status.csv)
        - Resume state (.resume_state.json)

        Args:
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with upload results for each file type
        """
        if not self.enabled:
            self.logger.info("Cloud storage sync disabled, skipping upload")
            return {}

        self.logger.info("Starting upload to cloud storage...")
        results = {}

        try:
            # 1. Upload DuckDB database
            if progress_callback:
                progress_callback("Uploading database...")

            db_success = self.gcs_manager.upload_file(
                local_path=Settings.DEFAULT_DB_PATH,
                gcs_path="data/warehouse.duckdb"
            )
            results['database'] = db_success

            # 2. Upload Parquet files
            if progress_callback:
                progress_callback("Uploading Parquet files...")

            parquet_results = self.gcs_manager.upload_directory(
                local_dir=Settings.PARQUET_DIR,
                gcs_prefix="data/parquet",
                include_patterns=['*.parquet']
            )
            results['parquet_files'] = parquet_results
            results['parquet_count'] = len([v for v in parquet_results.values() if v])

            # 3. Upload faction coalition CSV (if exists)
            if progress_callback:
                progress_callback("Uploading faction coalition data...")

            csv_success = False
            if Settings.FACTION_COALITION_STATUS_FILE.exists():
                csv_success = self.gcs_manager.upload_file(
                    local_path=Settings.FACTION_COALITION_STATUS_FILE,
                    gcs_path="data/faction_coalition_status.csv"
                )
            results['faction_csv'] = csv_success

            # 4. Upload resume state (if exists)
            if progress_callback:
                progress_callback("Uploading resume state...")

            resume_success = False
            if Settings.RESUME_STATE_FILE.exists():
                resume_success = self.gcs_manager.upload_file(
                    local_path=Settings.RESUME_STATE_FILE,
                    gcs_path="data/.resume_state.json"
                )
            results['resume_state'] = resume_success

            # Summary
            total_success = sum([
                db_success,
                len([v for v in parquet_results.values() if v]) > 0,
                csv_success,
                resume_success
            ])

            self.logger.info(f"Upload complete: {total_success}/4 categories successful")

            if progress_callback:
                progress_callback(f"Upload complete: {total_success}/4 categories synced")

            return results

        except Exception as e:
            self.logger.error(f"Error during upload: {e}", exc_info=True)
            return {'error': str(e)}

    def upload_database_only(self) -> bool:
        """
        Upload only the database file to cloud storage.

        Used for quick sync after admin operations (user creation, etc.)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            self.logger.debug("Cloud storage sync disabled, skipping database upload")
            return False

        try:
            success = self.gcs_manager.upload_file(
                local_path=Settings.DEFAULT_DB_PATH,
                gcs_path="data/warehouse.duckdb"
            )
            if success:
                self.logger.info("Database synced to cloud storage")
            return success
        except Exception as e:
            self.logger.warning(f"Failed to sync database to cloud: {e}")
            return False

    def sync_after_refresh(
        self,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> bool:
        """
        Convenience method to upload data after a refresh operation.

        Args:
            progress_callback: Optional callback for progress updates

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return True  # Not an error, just disabled

        results = self.upload_all_data(progress_callback)
        return 'error' not in results

    def check_cloud_data_exists(self) -> bool:
        """
        Check if data exists in cloud storage.

        Returns:
            True if at least the database file exists in cloud storage
        """
        if not self.enabled:
            return False

        return self.gcs_manager.file_exists("data/warehouse.duckdb")

    def get_cloud_file_info(self) -> Dict[str, Any]:
        """
        Get information about files in cloud storage.

        Returns:
            Dictionary with file metadata
        """
        if not self.enabled:
            return {}

        info = {}

        try:
            # Check database
            db_meta = self.gcs_manager.get_file_metadata("data/warehouse.duckdb")
            if db_meta:
                info['database'] = {
                    'exists': True,
                    'size_mb': db_meta['size'] / (1024 * 1024),
                    'updated': db_meta['updated']
                }
            else:
                info['database'] = {'exists': False}

            # List parquet files
            parquet_files = self.gcs_manager.list_files(prefix="data/parquet/")
            info['parquet_files'] = {
                'count': len(parquet_files),
                'files': parquet_files
            }

            # Check CSV
            csv_meta = self.gcs_manager.get_file_metadata("data/faction_coalition_status.csv")
            info['faction_csv'] = {'exists': csv_meta is not None}

            # Check resume state
            resume_meta = self.gcs_manager.get_file_metadata("data/.resume_state.json")
            info['resume_state'] = {'exists': resume_meta is not None}

        except Exception as e:
            self.logger.error(f"Error getting cloud file info: {e}", exc_info=True)
            info['error'] = str(e)

        return info

    def get_local_last_modified(self) -> Optional[datetime]:
        """
        Get timestamp of last database modification.

        Reads from _SyncMetadata table if available, otherwise uses file mtime.

        Returns:
            Datetime of last modification or None if unknown
        """
        try:
            if not Settings.DEFAULT_DB_PATH.exists():
                return None

            # Try to get from metadata table
            import duckdb
            conn = duckdb.connect(str(Settings.DEFAULT_DB_PATH), read_only=True)
            try:
                result = conn.execute("""
                    SELECT Value FROM _SyncMetadata WHERE Key = 'last_modified'
                """).fetchone()
                if result:
                    return datetime.fromisoformat(result[0])
            except Exception:
                # Table doesn't exist or other error
                pass
            finally:
                conn.close()

            # Fall back to file mtime
            mtime = Settings.DEFAULT_DB_PATH.stat().st_mtime
            return datetime.fromtimestamp(mtime)

        except Exception as e:
            self.logger.debug(f"Error getting local last modified: {e}")
            return None

    def update_last_modified(self) -> bool:
        """
        Update the last_modified timestamp to now.

        Returns:
            True if successful, False otherwise
        """
        try:
            if not Settings.DEFAULT_DB_PATH.exists():
                return False

            import duckdb
            conn = duckdb.connect(str(Settings.DEFAULT_DB_PATH), read_only=False)
            try:
                now = datetime.now().isoformat()
                conn.execute("""
                    INSERT OR REPLACE INTO _SyncMetadata (Key, Value, UpdatedAt)
                    VALUES ('last_modified', ?, CURRENT_TIMESTAMP)
                """, [now])
                return True
            except Exception as e:
                self.logger.debug(f"Could not update _SyncMetadata (table may not exist): {e}")
                return False
            finally:
                conn.close()

        except Exception as e:
            self.logger.debug(f"Error updating last modified: {e}")
            return False

    def get_cloud_last_modified(self) -> Optional[datetime]:
        """
        Get last modified time of cloud database file.

        Returns:
            Datetime of cloud file's last update or None if unavailable
        """
        if not self.enabled:
            return None

        try:
            meta = self.gcs_manager.get_file_metadata("data/warehouse.duckdb")
            if meta and meta.get('updated'):
                return meta['updated']
            return None
        except Exception as e:
            self.logger.debug(f"Error getting cloud last modified: {e}")
            return None

    def compare_freshness(self) -> str:
        """
        Compare local vs cloud database freshness.

        Returns:
            "local_newer" - local changes need upload
            "cloud_newer" - cloud changes need download
            "up_to_date" - no sync needed
            "unknown" - cannot determine (use existence check)
        """
        local_modified = self.get_local_last_modified()
        cloud_modified = self.get_cloud_last_modified()

        if local_modified is None and cloud_modified is None:
            return "unknown"

        if local_modified is None:
            return "cloud_newer"

        if cloud_modified is None:
            return "local_newer"

        # Compare timestamps (with 60 second tolerance for clock skew)
        diff = (local_modified - cloud_modified).total_seconds()

        if diff > 60:
            return "local_newer"
        elif diff < -60:
            return "cloud_newer"
        else:
            return "up_to_date"

    def _backup_local_database(self) -> Optional[Path]:
        """
        Create a backup of the local database before overwriting.

        Returns:
            Path to backup file or None if backup not created
        """
        if not Settings.DEFAULT_DB_PATH.exists():
            return None

        try:
            backup_path = Settings.DEFAULT_DB_PATH.with_suffix('.duckdb.backup')
            shutil.copy2(Settings.DEFAULT_DB_PATH, backup_path)
            self.logger.info(f"Created database backup at {backup_path}")
            return backup_path
        except Exception as e:
            self.logger.warning(f"Failed to create backup: {e}")
            return None

    def smart_sync_on_startup(
        self,
        force_download: bool = False,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> bool:
        """
        Intelligently sync data on app startup.

        Logic:
        - If local data exists and is recent: do nothing
        - If local data missing: download from cloud if available
        - If force_download: always download from cloud
        - If cloud is newer: backup local and download

        Args:
            force_download: Force download even if local data exists
            progress_callback: Optional callback for progress updates

        Returns:
            True if local data is available (either existed or downloaded), False otherwise
        """
        if not self.enabled:
            self.logger.info("Cloud storage disabled, checking local data only")
            return Settings.DEFAULT_DB_PATH.exists()

        # Check if local database exists
        local_db_exists = Settings.DEFAULT_DB_PATH.exists()

        if local_db_exists and not force_download:
            # Check if cloud is newer
            freshness = self.compare_freshness()
            if freshness == "cloud_newer":
                self.logger.info("Cloud database is newer, downloading...")
                if progress_callback:
                    progress_callback("Cloud has newer data, downloading...")
                self._backup_local_database()
                results = self.download_all_data(progress_callback)
                return results.get('database', False)
            elif freshness == "local_newer":
                self.logger.info("Local database is newer than cloud")
            else:
                self.logger.info("Local database exists, skipping cloud download")
            return True

        # Check if cloud data exists
        if progress_callback:
            progress_callback("Checking cloud storage...")

        cloud_exists = self.check_cloud_data_exists()

        if not cloud_exists:
            self.logger.info("No data in cloud storage, local refresh required")
            return False

        # Backup local before downloading (if exists)
        if local_db_exists:
            self._backup_local_database()

        # Download from cloud
        self.logger.info("Downloading data from cloud storage...")
        results = self.download_all_data(progress_callback)

        success = results.get('database', False)
        if success:
            self.logger.info("Successfully synced data from cloud storage")
        else:
            self.logger.warning("Failed to download database from cloud storage")

        return success
