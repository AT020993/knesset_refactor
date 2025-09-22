"""OData API client for Knesset data."""

import asyncio
import logging
from math import ceil
from typing import Dict, List, Optional

import aiohttp
import backoff
import pandas as pd
from tqdm import tqdm

from config.api import APIConfig
from config.database import DatabaseConfig

from .circuit_breaker import circuit_breaker_manager
from .error_handling import categorize_error


class ODataClient:
    """Client for fetching data from Knesset OData API."""

    def __init__(self, logger_obj: Optional[logging.Logger] = None):
        self.logger = logger_obj or logging.getLogger(__name__)
        self.config = APIConfig()

    def _backoff_handler(self, details):
        """Handler for logging backoff attempts with error categorization."""
        exception = details["exception"]
        error_category = categorize_error(exception)
        self.logger.warning(
            f"Backing off {details['wait']:.1f}s after {error_category.value} error "
            f"(attempt {details['tries']}/{self.config.MAX_RETRIES}): {exception}"
        )

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError),
        max_tries=APIConfig.MAX_RETRIES,
        on_backoff=lambda details: None,  # Will be set by instance
        jitter=backoff.full_jitter,
        base=APIConfig.RETRY_BASE_DELAY,
        max_value=APIConfig.RETRY_MAX_DELAY,
    )
    async def fetch_json(self, session: aiohttp.ClientSession, url: str) -> dict:
        """Fetch JSON data from a URL with retries and circuit breaker."""
        # Get base URL for circuit breaker
        base_url = f"{url.split('/', 3)[0]}//{url.split('/', 3)[2]}"

        # Check circuit breaker
        if not circuit_breaker_manager.can_attempt(base_url):
            raise aiohttp.ClientError(f"Circuit breaker is open for {base_url}")

        try:
            timeout = aiohttp.ClientTimeout(total=self.config.REQUEST_TIMEOUT)
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                result = await resp.json(content_type=None)

                # Record success
                circuit_breaker_manager.record_success(base_url)
                return result

        except Exception as e:
            # Record failure
            circuit_breaker_manager.record_failure(base_url)

            # Log detailed error information
            error_category = categorize_error(e)
            self.logger.error(f"Request failed with {error_category.value} error: {e}")

            # Re-raise to trigger backoff retry
            raise

    async def download_table(self, table: str, resume_state: Optional[Dict] = None) -> pd.DataFrame:
        """Download a specific table from the OData API."""
        entity = f"{table}()"
        self.logger.info(f"Starting download for table: {table}")

        async with aiohttp.ClientSession() as session:
            if DatabaseConfig.is_cursor_table(table):
                return await self._download_cursor_table(session, table, entity, resume_state)
            else:
                return await self._download_skip_table(session, table, entity)

    async def _download_cursor_table(
        self, session: aiohttp.ClientSession, table: str, entity: str, resume_state: Optional[Dict] = None
    ) -> pd.DataFrame:
        """Download table using cursor-based paging."""
        pk, chunk_size = DatabaseConfig.get_cursor_config(table)
        last_val = resume_state.get("last_pk", -1) if resume_state else -1
        total_rows_fetched = resume_state.get("total_rows", 0) if resume_state else 0

        if last_val > -1:
            self.logger.info(f"Resuming {table} from PK {last_val} (previously fetched {total_rows_fetched:,} rows)")

        dfs: List[pd.DataFrame] = []

        with tqdm(desc=f"Fetching {table} (cursor)", unit=" rows", initial=total_rows_fetched, leave=False) as pbar:
            while True:
                url = (
                    f"{self.config.BASE_URL}/{entity}"
                    f"?$format=json&$top={chunk_size}"
                    f"&$filter={pk}%20gt%20{last_val}"
                    f"&$orderby={pk}%20asc"
                )

                try:
                    data = await self.fetch_json(session, url)
                except Exception as e:
                    self.logger.error(f"Error fetching chunk for {table} (PK > {last_val}): {e}")
                    await asyncio.sleep(5)
                    continue

                rows = data.get("value", [])
                if not rows:
                    break

                current_df = pd.DataFrame.from_records(rows)
                dfs.append(current_df)

                # Update last_val
                last_val = int(current_df[pk].iloc[-1])
                total_rows_fetched += len(rows)
                pbar.update(len(rows))

        self.logger.info(f"Fetched {total_rows_fetched:,} rows for {table}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    async def _download_skip_table(self, session: aiohttp.ClientSession, table: str, entity: str) -> pd.DataFrame:
        """Download table using skip-based paging with parallel requests."""
        try:
            # Get total count
            count_url = self.config.get_count_url(entity)
            total_records_resp = await session.get(count_url, timeout=aiohttp.ClientTimeout(total=30))
            total_records_resp.raise_for_status()
            total_records = int(await total_records_resp.text())
        except Exception as e:
            self.logger.warning(f"Could not get count for {table}: {e}. Using sequential download.")
            return await self._download_sequential(session, entity)

        if total_records == 0:
            self.logger.info(f"Table {table} has 0 records.")
            return pd.DataFrame()

        num_pages = ceil(total_records / self.config.PAGE_SIZE)

        with tqdm(total=total_records, desc=f"Fetching {table} (skip)", unit="rows", leave=False) as pbar:
            semaphore = asyncio.Semaphore(self.config.CONCURRENCY_LIMIT)

            async def fetch_page(page_index: int):
                async with semaphore:
                    skip_val = page_index * self.config.PAGE_SIZE
                    page_url = (
                        f"{self.config.BASE_URL}/{entity}?$format=json&$skip={skip_val}&$top={self.config.PAGE_SIZE}"
                    )

                    try:
                        page_data = await self.fetch_json(session, page_url)
                        page_rows = page_data.get("value", [])
                        if page_rows:
                            pbar.update(len(page_rows))
                            return page_index, pd.DataFrame.from_records(page_rows)
                    except Exception as e:
                        error_category = categorize_error(e)
                        self.logger.error(f"Error fetching page {page_index}: {error_category.value} - {e}")
                        return page_index, None

                    return page_index, None

            # Fetch all pages
            tasks = [fetch_page(i) for i in range(num_pages)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        valid_dfs = []
        for res in results:
            if isinstance(res, Exception):
                self.logger.error(f"Page fetch failed: {res}")
            elif res is not None and res[1] is not None:
                valid_dfs.append(res)

        valid_dfs.sort(key=lambda x: x[0])  # Sort by page index
        final_dfs = [df for _, df in valid_dfs]

        return pd.concat(final_dfs, ignore_index=True) if final_dfs else pd.DataFrame()

    async def _download_sequential(self, session: aiohttp.ClientSession, entity: str) -> pd.DataFrame:
        """Fallback sequential download method."""
        table_name = entity.replace("()", "")
        self.logger.info(f"Using sequential download for {table_name}")

        dfs: List[pd.DataFrame] = []
        page_index = 0

        with tqdm(desc=f"Fetching {table_name} (sequential)", unit="rows", leave=False) as pbar:
            while True:
                skip_val = page_index * self.config.PAGE_SIZE
                url = f"{self.config.BASE_URL}/{entity}?$format=json&$skip={skip_val}&$top={self.config.PAGE_SIZE}"

                try:
                    data = await self.fetch_json(session, url)
                except Exception as e:
                    error_category = categorize_error(e)
                    self.logger.error(f"Sequential fetch failed for page {page_index}: {error_category.value} - {e}")
                    break

                rows = data.get("value", [])
                if not rows:
                    break

                dfs.append(pd.DataFrame.from_records(rows))
                pbar.update(len(rows))
                page_index += 1

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
