"""Data refresh service for coordinating OData downloads and storage."""

import asyncio
from pathlib import Path
from typing import List, Optional, Callable
import logging

from config.database import DatabaseConfig
from config.settings import Settings
from api.odata_client import ODataClient
from data.repositories.database_repository import DatabaseRepository
from data.services.resume_state_service import ResumeStateService
from data.services.storage_sync_service import StorageSyncService


class DataRefreshService:
    """Service for coordinating data refresh operations."""
    
    def __init__(
        self,
        db_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None
    ):
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)

        # Initialize components
        self.odata_client = ODataClient(self.logger)
        self.db_repository = DatabaseRepository(self.db_path, self.logger)
        self.resume_service = ResumeStateService(logger_obj=self.logger)
        self.storage_sync = StorageSyncService(logger_obj=self.logger)
    
    async def refresh_single_table(
        self,
        table_name: str,
        progress_callback: Optional[Callable[[str, int], None]] = None
    ) -> bool:
        """Refresh a single table from OData API."""
        try:
            self.logger.info(f"Starting refresh for table: {table_name}")
            
            # Get resume state if cursor table
            resume_state = None
            if DatabaseConfig.is_cursor_table(table_name):
                resume_state = self.resume_service.get_table_state(table_name)
            
            # Download data
            df = await self.odata_client.download_table(table_name, resume_state)
            
            if df.empty:
                self.logger.info(f"No data downloaded for table: {table_name}")
                return True
            
            # Store data
            success = self.db_repository.store_table(df, table_name)
            
            if success:
                # Clear resume state if cursor table (download completed)
                if DatabaseConfig.is_cursor_table(table_name):
                    self.resume_service.clear_table_state(table_name)
                
                # Call progress callback
                if progress_callback:
                    progress_callback(table_name, len(df))
                
                self.logger.info(f"Successfully refreshed table: {table_name} ({len(df):,} rows)")
                return True
            else:
                self.logger.error(f"Failed to store table: {table_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error refreshing table {table_name}: {e}", exc_info=True)
            return False
    
    async def refresh_tables(
        self,
        tables: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[str, int], None]] = None
    ) -> bool:
        """Refresh multiple tables from OData API."""
        tables_to_refresh = tables or DatabaseConfig.get_all_tables()
        
        # Validate table names
        valid_tables = DatabaseConfig.get_all_tables()
        invalid_tables = [t for t in tables_to_refresh if t not in valid_tables]
        if invalid_tables:
            self.logger.error(f"Invalid table names: {invalid_tables}")
            raise ValueError(f"Invalid table names: {invalid_tables}")
        
        self.logger.info(f"Starting refresh for {len(tables_to_refresh)} tables")
        
        success_count = 0
        for table_name in tables_to_refresh:
            success = await self.refresh_single_table(table_name, progress_callback)
            if success:
                success_count += 1
        
        # Also refresh faction coalition status
        self.logger.info("Loading faction coalition status from CSV...")
        faction_success = self.db_repository.load_faction_coalition_status()
        
        total_success = success_count == len(tables_to_refresh) and faction_success

        if total_success:
            self.logger.info("All data refresh tasks completed successfully")

            # Sync to cloud storage if enabled
            if self.storage_sync.is_enabled():
                self.logger.info("Syncing data to cloud storage...")
                try:
                    if progress_callback:
                        progress_callback("Syncing to cloud storage", 0)

                    sync_success = self.storage_sync.sync_after_refresh(
                        progress_callback=lambda msg: self.logger.info(f"Cloud sync: {msg}")
                    )

                    if sync_success:
                        self.logger.info("Successfully synced data to cloud storage")
                    else:
                        self.logger.warning("Cloud storage sync completed with some errors")
                except Exception as e:
                    self.logger.error(f"Error during cloud sync: {e}", exc_info=True)
                    # Don't fail the entire refresh if cloud sync fails
        else:
            self.logger.warning(f"Refresh completed with {success_count}/{len(tables_to_refresh)} table successes")

        return total_success
    
    def refresh_tables_sync(
        self,
        tables: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[str, int], None]] = None
    ) -> bool:
        """Synchronous wrapper for refresh_tables.

        Handles both CLI context (no event loop) and Streamlit context
        (existing event loop running).

        Note: In Streamlit context (threaded execution), progress callbacks
        may not work reliably due to thread safety issues with Streamlit components.
        """
        try:
            # Check if there's already a running event loop (e.g., Streamlit)
            try:
                loop = asyncio.get_running_loop()
                # We're inside an existing event loop (Streamlit context)
                self.logger.info("Detected existing event loop (Streamlit context)")

                # Use nest_asyncio if available, otherwise create new loop in thread
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    self.logger.info("Using nest_asyncio for nested event loop")
                    return asyncio.run(self.refresh_tables(tables, progress_callback))
                except ImportError:
                    # Run in a separate thread with its own event loop
                    # NOTE: Progress callbacks are disabled in thread context because
                    # Streamlit components are not thread-safe
                    import concurrent.futures
                    self.logger.info("Running async refresh in separate thread (nest_asyncio not available)")
                    self.logger.info("Progress callbacks disabled in threaded mode for thread safety")

                    # Capture self for thread
                    service = self
                    tables_to_refresh = tables

                    # Thread-safe logging callback (just logs, doesn't update UI)
                    def logging_callback(table_name: str, row_count: int):
                        service.logger.info(f"Downloaded {table_name}: {row_count:,} rows")

                    def run_in_thread():
                        service.logger.info("Thread started, creating new event loop")
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        try:
                            service.logger.info(f"Running refresh_tables for {len(tables_to_refresh or [])} tables")
                            result = new_loop.run_until_complete(
                                service.refresh_tables(tables_to_refresh, logging_callback)
                            )
                            service.logger.info(f"Thread completed with result: {result}")
                            return result
                        except Exception as thread_error:
                            service.logger.error(f"Error in thread: {thread_error}", exc_info=True)
                            return False
                        finally:
                            new_loop.close()

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        self.logger.info("Submitting task to thread pool")
                        future = executor.submit(run_in_thread)
                        result = future.result(timeout=600)  # 10 minute timeout
                        self.logger.info(f"Thread pool returned: {result}")
                        return result

            except RuntimeError:
                # No running loop - we're in CLI context, use asyncio.run()
                self.logger.info("No existing event loop (CLI context)")
                return asyncio.run(self.refresh_tables(tables, progress_callback))

        except Exception as e:
            self.logger.error(f"Error during synchronous refresh: {e}", exc_info=True)
            return False
    
    def refresh_faction_status_only(self) -> bool:
        """Refresh only the faction coalition status from CSV."""
        self.logger.info("Refreshing faction coalition status from CSV")
        return self.db_repository.load_faction_coalition_status()