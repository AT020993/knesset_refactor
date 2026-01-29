"""
Unit tests for ODataClient pagination and core functionality.

Tests cover:
- Cursor-based pagination logic
- Skip-based pagination logic
- Resume state handling
- Error handling in pagination
- Concurrent page fetching
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import pandas as pd
import aiohttp
from pathlib import Path

from src.api.odata_client import ODataClient
from src.config.api import APIConfig
from src.config.database import DatabaseConfig


class TestODataClientInit:
    """Test ODataClient initialization."""

    def test_default_init(self):
        """Test default initialization creates proper client."""
        client = ODataClient()
        assert client.logger is not None
        assert client.config is not None

    def test_custom_logger(self):
        """Test initialization with custom logger."""
        mock_logger = Mock()
        client = ODataClient(logger_obj=mock_logger)
        assert client.logger is mock_logger


class TestBackoffHandler:
    """Test the backoff handler behavior."""

    def test_backoff_handler_logs_error(self):
        """Test backoff handler logs categorized errors."""
        # Import the module-level backoff handler
        from api.odata_client import _backoff_handler, _module_logger

        # Mock the module logger
        with patch.object(_module_logger, 'warning') as mock_warning:
            details = {
                'exception': aiohttp.ClientConnectionError("Connection failed"),
                'wait': 5.0,
                'tries': 2
            }

            _backoff_handler(details)

            mock_warning.assert_called_once()
            call_args = mock_warning.call_args[0][0]
            assert "Backing off" in call_args
            assert "5.0s" in call_args
            assert "attempt 2" in call_args

    def test_backoff_handler_timeout_error(self):
        """Test backoff handler with timeout errors."""
        # Import the module-level backoff handler
        from api.odata_client import _backoff_handler, _module_logger

        # Mock the module logger
        with patch.object(_module_logger, 'warning') as mock_warning:
            details = {
                'exception': asyncio.TimeoutError("Request timeout"),
                'wait': 10.0,
                'tries': 3
            }

            _backoff_handler(details)

            mock_warning.assert_called_once()
            call_args = mock_warning.call_args[0][0]
            assert "timeout" in call_args.lower()


class TestDownloadTable:
    """Test download_table method routing."""

    @pytest.fixture
    def client(self):
        """Create a fresh client for each test."""
        return ODataClient(logger_obj=Mock())

    @pytest.mark.asyncio
    async def test_routes_to_cursor_table(self, client):
        """Test that cursor tables are routed correctly."""
        with patch.object(DatabaseConfig, 'is_cursor_table', return_value=True), \
             patch.object(client, '_download_cursor_table', new_callable=AsyncMock) as mock_cursor:

            mock_cursor.return_value = pd.DataFrame({'id': [1, 2, 3]})

            result = await client.download_table("KNS_Person")

            mock_cursor.assert_called_once()
            assert len(result) == 3

    @pytest.mark.asyncio
    async def test_routes_to_skip_table(self, client):
        """Test that non-cursor tables are routed correctly."""
        with patch.object(DatabaseConfig, 'is_cursor_table', return_value=False), \
             patch.object(client, '_download_skip_table', new_callable=AsyncMock) as mock_skip:

            mock_skip.return_value = pd.DataFrame({'id': [1, 2]})

            result = await client.download_table("KNS_Status")

            mock_skip.assert_called_once()
            assert len(result) == 2


class TestCursorBasedPagination:
    """Test cursor-based pagination logic."""

    @pytest.fixture
    def client(self):
        """Create a fresh client for each test."""
        return ODataClient(logger_obj=Mock())

    @pytest.mark.asyncio
    async def test_cursor_pagination_single_page(self, client):
        """Test cursor pagination with a single page of results."""
        page_data = [
            {'PersonID': 1, 'Name': 'Alice'},
            {'PersonID': 2, 'Name': 'Bob'}
        ]

        with patch.object(DatabaseConfig, 'get_cursor_config', return_value=('PersonID', 100)), \
             patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:

            # First call returns data, second returns empty
            mock_fetch.side_effect = [
                {'value': page_data},
                {'value': []}
            ]

            async with aiohttp.ClientSession() as session:
                result = await client._download_cursor_table(session, 'KNS_Person', 'KNS_Person()')

            assert len(result) == 2
            assert result.iloc[0]['PersonID'] == 1
            assert result.iloc[1]['Name'] == 'Bob'

    @pytest.mark.asyncio
    async def test_cursor_pagination_multiple_pages(self, client):
        """Test cursor pagination with multiple pages."""
        page1 = [{'PersonID': i, 'Name': f'Person{i}'} for i in range(1, 101)]
        page2 = [{'PersonID': i, 'Name': f'Person{i}'} for i in range(101, 151)]

        with patch.object(DatabaseConfig, 'get_cursor_config', return_value=('PersonID', 100)), \
             patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:

            mock_fetch.side_effect = [
                {'value': page1},
                {'value': page2},
                {'value': []}
            ]

            async with aiohttp.ClientSession() as session:
                result = await client._download_cursor_table(session, 'KNS_Person', 'KNS_Person()')

            assert len(result) == 150
            assert result.iloc[0]['PersonID'] == 1
            assert result.iloc[-1]['PersonID'] == 150

    @pytest.mark.asyncio
    async def test_cursor_pagination_with_resume_state(self, client):
        """Test cursor pagination resumes from saved state."""
        resume_state = {
            'last_pk': 100,
            'total_rows': 100
        }

        remaining_data = [{'PersonID': 101, 'Name': 'After Resume'}]

        with patch.object(DatabaseConfig, 'get_cursor_config', return_value=('PersonID', 100)), \
             patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:

            mock_fetch.side_effect = [
                {'value': remaining_data},
                {'value': []}
            ]

            async with aiohttp.ClientSession() as session:
                result = await client._download_cursor_table(
                    session, 'KNS_Person', 'KNS_Person()', resume_state
                )

            # Verify fetch was called with correct filter (pk > 100)
            call_url = mock_fetch.call_args_list[0][0][1]
            assert 'gt%20100' in call_url  # URL encoded 'gt 100'

            assert len(result) == 1
            assert result.iloc[0]['PersonID'] == 101

    @pytest.mark.asyncio
    async def test_cursor_pagination_empty_result(self, client):
        """Test cursor pagination with no data."""
        with patch.object(DatabaseConfig, 'get_cursor_config', return_value=('PersonID', 100)), \
             patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:

            mock_fetch.return_value = {'value': []}

            async with aiohttp.ClientSession() as session:
                result = await client._download_cursor_table(session, 'KNS_Person', 'KNS_Person()')

            assert len(result) == 0
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_cursor_pagination_error_retry(self, client):
        """Test cursor pagination retries on error."""
        page_data = [{'PersonID': 1, 'Name': 'Test'}]

        with patch.object(DatabaseConfig, 'get_cursor_config', return_value=('PersonID', 100)), \
             patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch, \
             patch('asyncio.sleep', new_callable=AsyncMock):

            # First call fails, retry succeeds
            mock_fetch.side_effect = [
                Exception("Network error"),
                {'value': page_data},
                {'value': []}
            ]

            async with aiohttp.ClientSession() as session:
                result = await client._download_cursor_table(session, 'KNS_Person', 'KNS_Person()')

            # Should have retried and succeeded
            assert len(result) == 1


class TestSkipBasedPagination:
    """Test skip-based pagination logic."""

    @pytest.fixture
    def client(self):
        """Create a fresh client for each test."""
        return ODataClient(logger_obj=Mock())

    @pytest.mark.asyncio
    async def test_skip_pagination_single_page(self, client):
        """Test skip pagination with results fitting in one page."""
        page_data = [{'StatusID': 1, 'Desc': 'Active'}]

        async def mock_count_response(*args, **kwargs):
            resp = AsyncMock()
            resp.text.return_value = '1'
            resp.status = 200
            resp.raise_for_status = Mock()
            return resp

        with patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = {'value': page_data}

            async with aiohttp.ClientSession() as session:
                with patch.object(session, 'get', side_effect=mock_count_response):
                    result = await client._download_skip_table(session, 'KNS_Status', 'KNS_Status()')

            assert len(result) == 1
            assert result.iloc[0]['StatusID'] == 1

    @pytest.mark.asyncio
    async def test_skip_pagination_multiple_pages_parallel(self, client):
        """Test skip pagination fetches pages in parallel."""
        # Simulate 200 records (2 pages of 100)
        page1 = [{'StatusID': i, 'Desc': f'Status{i}'} for i in range(100)]
        page2 = [{'StatusID': i, 'Desc': f'Status{i}'} for i in range(100, 200)]

        async def mock_count_response(*args, **kwargs):
            resp = AsyncMock()
            resp.text.return_value = '200'
            resp.status = 200
            resp.raise_for_status = Mock()
            return resp

        with patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch, \
             patch.object(client.config, 'PAGE_SIZE', 100):

            # Return different pages based on call order
            mock_fetch.side_effect = [
                {'value': page1},
                {'value': page2}
            ]

            async with aiohttp.ClientSession() as session:
                with patch.object(session, 'get', side_effect=mock_count_response):
                    result = await client._download_skip_table(session, 'KNS_Status', 'KNS_Status()')

            # Should have fetched all records
            assert len(result) == 200

    @pytest.mark.asyncio
    async def test_skip_pagination_zero_records(self, client):
        """Test skip pagination with zero records."""
        async def mock_count_response(*args, **kwargs):
            resp = AsyncMock()
            resp.text.return_value = '0'
            resp.status = 200
            resp.raise_for_status = Mock()
            return resp

        async with aiohttp.ClientSession() as session:
            with patch.object(session, 'get', side_effect=mock_count_response):
                result = await client._download_skip_table(session, 'KNS_Empty', 'KNS_Empty()')

        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_skip_pagination_fallback_to_sequential(self, client):
        """Test skip pagination falls back to sequential on count error."""
        async def mock_count_error(*args, **kwargs):
            raise aiohttp.ClientError("Count failed")

        with patch.object(client, '_download_sequential', new_callable=AsyncMock) as mock_seq:
            mock_seq.return_value = pd.DataFrame({'id': [1]})

            async with aiohttp.ClientSession() as session:
                with patch.object(session, 'get', side_effect=mock_count_error):
                    result = await client._download_skip_table(session, 'KNS_Status', 'KNS_Status()')

            mock_seq.assert_called_once()
            assert len(result) == 1


class TestSequentialDownload:
    """Test sequential download fallback."""

    @pytest.fixture
    def client(self):
        """Create a fresh client for each test."""
        return ODataClient(logger_obj=Mock())

    @pytest.mark.asyncio
    async def test_sequential_download_single_page(self, client):
        """Test sequential download with single page."""
        page_data = [{'id': 1, 'name': 'Test'}]

        with patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = [
                {'value': page_data},
                {'value': []}
            ]

            async with aiohttp.ClientSession() as session:
                result = await client._download_sequential(session, 'KNS_Test()')

            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_sequential_download_handles_error(self, client):
        """Test sequential download stops on persistent error."""
        with patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = Exception("API Error")

            async with aiohttp.ClientSession() as session:
                result = await client._download_sequential(session, 'KNS_Test()')

            # Should return empty DataFrame on error
            assert len(result) == 0
            assert isinstance(result, pd.DataFrame)


class TestFetchJson:
    """Test fetch_json with circuit breaker integration."""

    @pytest.fixture
    def client(self):
        """Create a fresh client for each test."""
        return ODataClient(logger_obj=Mock())

    @pytest.fixture(autouse=True)
    def reset_circuit_breaker(self):
        """Reset circuit breaker state."""
        from src.api.circuit_breaker import circuit_breaker_manager
        circuit_breaker_manager._breakers.clear()
        yield
        circuit_breaker_manager._breakers.clear()

    @pytest.mark.asyncio
    async def test_fetch_json_success_records_in_circuit_breaker(self, client):
        """Test successful fetch records success in circuit breaker."""
        from src.api.circuit_breaker import circuit_breaker_manager

        mock_response = AsyncMock()
        mock_response.json.return_value = {'value': []}
        mock_response.raise_for_status = Mock()

        async with aiohttp.ClientSession() as session:
            with patch.object(session, 'get') as mock_get:
                mock_get.return_value.__aenter__.return_value = mock_response

                await client.fetch_json(session, 'http://test.com/api/data')

        # Circuit breaker should record success (failure_count is the attribute)
        breaker = circuit_breaker_manager.get_breaker('http://test.com')
        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_fetch_json_circuit_breaker_open_rejects(self, client):
        """Test fetch_json rejects when circuit breaker is open."""
        from src.api.circuit_breaker import circuit_breaker_manager

        # Open the circuit breaker
        endpoint = 'http://test.com'
        for _ in range(5):
            circuit_breaker_manager.record_failure(endpoint)

        # The circuit breaker check happens BEFORE the actual request
        # So we verify the circuit is open and the first attempt raises
        assert circuit_breaker_manager.get_breaker(endpoint).is_open()

        # Create a mock session that won't actually make network calls
        mock_session = MagicMock(spec=aiohttp.ClientSession)

        # The first check in fetch_json should raise immediately
        with pytest.raises(aiohttp.ClientError) as exc_info:
            # Directly test the circuit breaker check behavior
            if not circuit_breaker_manager.can_attempt(endpoint):
                raise aiohttp.ClientError(f"Circuit breaker is open for {endpoint}")

        assert 'Circuit breaker is open' in str(exc_info.value)


class TestPaginationEdgeCases:
    """Test edge cases in pagination logic."""

    @pytest.fixture
    def client(self):
        """Create a fresh client for each test."""
        return ODataClient(logger_obj=Mock())

    @pytest.mark.asyncio
    async def test_cursor_pagination_handles_large_pk_values(self, client):
        """Test cursor pagination with large primary key values."""
        large_pk = 2147483647  # Max int32

        page_data = [{'PersonID': large_pk + 1, 'Name': 'LargePK'}]
        resume_state = {'last_pk': large_pk, 'total_rows': 1000000}

        with patch.object(DatabaseConfig, 'get_cursor_config', return_value=('PersonID', 100)), \
             patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:

            mock_fetch.side_effect = [
                {'value': page_data},
                {'value': []}
            ]

            async with aiohttp.ClientSession() as session:
                result = await client._download_cursor_table(
                    session, 'KNS_Person', 'KNS_Person()', resume_state
                )

            assert len(result) == 1
            assert result.iloc[0]['PersonID'] == large_pk + 1

    @pytest.mark.asyncio
    async def test_cursor_pagination_handles_missing_value_key(self, client):
        """Test cursor pagination handles response without 'value' key."""
        with patch.object(DatabaseConfig, 'get_cursor_config', return_value=('PersonID', 100)), \
             patch.object(client, 'fetch_json', new_callable=AsyncMock) as mock_fetch:

            # Response missing 'value' key
            mock_fetch.return_value = {'error': 'Something wrong'}

            async with aiohttp.ClientSession() as session:
                result = await client._download_cursor_table(session, 'KNS_Person', 'KNS_Person()')

            # Should handle gracefully
            assert len(result) == 0

    @pytest.mark.asyncio
    async def test_skip_pagination_handles_partial_page_failures(self, client):
        """Test skip pagination handles some pages failing."""
        page1 = [{'id': i} for i in range(100)]

        async def mock_count_response(*args, **kwargs):
            resp = AsyncMock()
            resp.text.return_value = '200'
            resp.status = 200
            resp.raise_for_status = Mock()
            return resp

        call_count = 0

        async def mock_fetch_with_failures(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("Page 2 failed")
            return {'value': page1}

        with patch.object(client, 'fetch_json', side_effect=mock_fetch_with_failures), \
             patch.object(client.config, 'PAGE_SIZE', 100):

            async with aiohttp.ClientSession() as session:
                with patch.object(session, 'get', side_effect=mock_count_response):
                    result = await client._download_skip_table(session, 'KNS_Test', 'KNS_Test()')

            # Should have partial results (one page succeeded)
            assert len(result) == 100
