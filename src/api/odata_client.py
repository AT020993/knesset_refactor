"""OData API client for Knesset data."""

import asyncio
import sys
import inspect
from contextlib import contextmanager
from math import ceil
from typing import Any, Callable, Dict, List, Optional, Tuple, cast
from urllib.parse import urlparse
import logging

import aiohttp
import backoff
from backoff._typing import Details
import pandas as pd
from tqdm import tqdm

from config.api import APIConfig
from config.database import DatabaseConfig
from .error_handling import categorize_error, ErrorCategory
from .circuit_breaker import circuit_breaker_manager

# Module-level logger for backoff handler (can't use self in decorator)
_module_logger = logging.getLogger(__name__)


def _backoff_handler(details: Details) -> None:
    """Module-level handler for logging backoff attempts with error categorization.

    Note: This is at module level because backoff decorators are evaluated at
    class definition time, not instance creation time, so we can't use self.
    """
    exception = details.get('exception')
    if isinstance(exception, Exception):
        error_category = categorize_error(exception)
        _module_logger.warning(
            f"Backing off {details['wait']:.1f}s after {error_category.value} error "
            f"(attempt {details['tries']}/{APIConfig.MAX_RETRIES}): {exception}"
        )
    else:
        _module_logger.warning(
            f"Backing off {details['wait']:.1f}s (attempt {details['tries']}/{APIConfig.MAX_RETRIES})"
        )


async def _maybe_await(value: Any) -> Any:
    """Await value when it is awaitable, otherwise return as-is."""
    if inspect.isawaitable(value):
        return await value
    return value


async def _raise_for_status(response: aiohttp.ClientResponse) -> None:
    """Call ``raise_for_status`` for sync and async-compatible response mocks."""
    raise_for_status = cast(Any, response.raise_for_status)
    result = raise_for_status()
    if inspect.isawaitable(result):
        await result


class _DummyProgressBar:
    """Dummy progress bar that does nothing (for use when tqdm is disabled)."""

    def __init__(self, *args, **kwargs):
        self.total = kwargs.get('total', 0)
        self.n = kwargs.get('initial', 0)

    def update(self, n=1):
        self.n += n

    def set_postfix_str(self, s):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class ODataClient:
    """Client for fetching data from Knesset OData API."""

    def __init__(self, logger_obj: Optional[logging.Logger] = None, disable_progress: bool = False):
        self.logger = logger_obj or logging.getLogger(__name__)
        self.config = APIConfig()
        self._disable_progress = disable_progress or not self._is_tty_available()

    def _is_tty_available(self) -> bool:
        """Check if stderr is available for tqdm output."""
        try:
            # Check if stderr is a TTY (terminal)
            if hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
                return True
            # Also try flushing to detect broken pipes early
            if hasattr(sys.stderr, 'flush'):
                sys.stderr.flush()
            return True
        except (BrokenPipeError, OSError, AttributeError):
            return False

    def _get_progress_bar(self, desc: str, total: Optional[int] = None, initial: int = 0) -> Any:
        """Get a progress bar (real or dummy based on context)."""
        if self._disable_progress:
            return _DummyProgressBar(total=total, desc=desc, initial=initial)
        try:
            return tqdm(total=total, desc=desc, unit="rows", leave=False, initial=initial)
        except (BrokenPipeError, OSError):
            self.logger.debug("tqdm unavailable, using dummy progress bar")
            return _DummyProgressBar(total=total, desc=desc, initial=initial)
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError),
        max_tries=APIConfig.MAX_RETRIES,
        on_backoff=_backoff_handler,  # Use module-level handler for proper logging
        jitter=backoff.full_jitter,
        base=APIConfig.RETRY_BASE_DELAY,
        max_value=APIConfig.RETRY_MAX_DELAY,
    )
    async def fetch_json(
        self, session: aiohttp.ClientSession, url: str
    ) -> dict[str, Any]:
        """Fetch JSON data from a URL with retries and circuit breaker."""
        # Get base URL for circuit breaker - use proper URL parsing for safety
        try:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            # Fallback to original method if URL parsing fails
            base_url = url.split('/')[0] + '//' + url.split('/')[2] if '/' in url else url
        
        # Check circuit breaker
        if not circuit_breaker_manager.can_attempt(base_url):
            raise aiohttp.ClientError(f"Circuit breaker is open for {base_url}")
        
        try:
            timeout = aiohttp.ClientTimeout(total=self.config.REQUEST_TIMEOUT)
            async with session.get(url, timeout=timeout) as resp:
                await _raise_for_status(resp)
                result = await resp.json(content_type=None)
                if not isinstance(result, dict):
                    raise ValueError("Unexpected JSON payload type: expected object")
                
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
        self, 
        session: aiohttp.ClientSession, 
        table: str, 
        entity: str,
        resume_state: Optional[Dict] = None
    ) -> pd.DataFrame:
        """Download table using cursor-based paging."""
        pk, chunk_size = DatabaseConfig.get_cursor_config(table)
        last_val = resume_state.get("last_pk", -1) if resume_state else -1
        total_rows_fetched = resume_state.get("total_rows", 0) if resume_state else 0
        
        if last_val > -1:
            self.logger.info(f"Resuming {table} from PK {last_val} (previously fetched {total_rows_fetched:,} rows)")
        
        dfs: List[pd.DataFrame] = []
        
        with self._get_progress_bar(desc=f"Fetching {table} (cursor)", initial=total_rows_fetched) as pbar:
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

                # Check for empty DataFrame before accessing iloc[-1]
                if current_df.empty:
                    self.logger.warning(f"Empty DataFrame received for {table}, ending pagination")
                    break

                dfs.append(current_df)

                # Update last_val - safe now because we checked for empty
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
            await _raise_for_status(total_records_resp)
            total_records = int(await total_records_resp.text())
        except Exception as e:
            self.logger.warning(f"Could not get count for {table}: {e}. Using sequential download.")
            return await self._download_sequential(session, entity)
        
        if total_records == 0:
            self.logger.info(f"Table {table} has 0 records.")
            return pd.DataFrame()
        
        num_pages = ceil(total_records / self.config.PAGE_SIZE)

        with self._get_progress_bar(desc=f"Fetching {table} (skip)", total=total_records) as pbar:
            semaphore = asyncio.Semaphore(self.config.CONCURRENCY_LIMIT)
            
            async def fetch_page(page_index: int) -> Tuple[int, Optional[pd.DataFrame]]:
                async with semaphore:
                    skip_val = page_index * self.config.PAGE_SIZE
                    page_url = f"{self.config.BASE_URL}/{entity}?$format=json&$skip={skip_val}&$top={self.config.PAGE_SIZE}"
                    
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
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            results = cast(List[Tuple[int, Optional[pd.DataFrame]] | BaseException], gathered)
        
        # Process results
        valid_dfs: List[Tuple[int, pd.DataFrame]] = []
        for res in results:
            if isinstance(res, BaseException):
                self.logger.error(f"Page fetch failed: {res}")
            elif res[1] is not None:
                valid_dfs.append((res[0], res[1]))
        
        valid_dfs.sort(key=lambda x: x[0])  # Sort by page index
        final_dfs = [df for _, df in valid_dfs]
        
        return pd.concat(final_dfs, ignore_index=True) if final_dfs else pd.DataFrame()
    
    async def _download_sequential(self, session: aiohttp.ClientSession, entity: str) -> pd.DataFrame:
        """Fallback sequential download method."""
        table_name = entity.replace('()', '')
        self.logger.info(f"Using sequential download for {table_name}")
        
        dfs: List[pd.DataFrame] = []
        page_index = 0
        
        with self._get_progress_bar(desc=f"Fetching {table_name} (sequential)") as pbar:
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
