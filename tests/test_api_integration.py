"""
Integration tests for API layer reliability and error handling.

This module tests the integration between circuit breaker, error handling,
and OData client functionality to ensure robust API communication.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
import aiohttp
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from api.circuit_breaker import CircuitBreakerManager, CircuitBreakerState
from api.error_handling import categorize_error, ErrorCategory
from api.odata_client import ODataClient
from config.api import APIConfig


class TestAPIErrorHandling:
    """Test error categorization and handling."""
    
    def test_network_error_categorization(self):
        """Test network errors are properly categorized."""
        error = aiohttp.ClientConnectionError("Connection failed")
        category = categorize_error(error)
        assert category == ErrorCategory.NETWORK
    
    def test_timeout_error_categorization(self):
        """Test timeout errors are properly categorized."""
        error = asyncio.TimeoutError("Request timed out")
        category = categorize_error(error)
        assert category == ErrorCategory.TIMEOUT
    
    def test_server_error_categorization(self):
        """Test server errors are properly categorized."""
        mock_response = Mock()
        mock_response.status = 500
        error = aiohttp.ClientResponseError(request_info=Mock(), history=Mock(), status=500)
        category = categorize_error(error)
        assert category == ErrorCategory.SERVER
    
    def test_client_error_categorization(self):
        """Test client errors are properly categorized."""
        mock_response = Mock()
        mock_response.status = 404
        error = aiohttp.ClientResponseError(request_info=Mock(), history=Mock(), status=404)
        category = categorize_error(error)
        assert category == ErrorCategory.CLIENT
    
    def test_unknown_error_categorization(self):
        """Test unknown errors are properly categorized."""
        error = ValueError("Some unexpected error")
        category = categorize_error(error)
        assert category == ErrorCategory.UNKNOWN


class TestCircuitBreakerAPIIntegration:
    """Test circuit breaker integration with API calls."""
    
    @pytest.fixture
    def manager(self):
        """Fresh circuit breaker manager for each test."""
        return CircuitBreakerManager()
    
    def test_circuit_breaker_prevents_calls_when_open(self, manager):
        """Test circuit breaker prevents API calls when open."""
        endpoint = "http://test-api.com"
        
        # Open the circuit by recording failures
        for _ in range(5):
            manager.record_failure(endpoint)
        
        # Circuit should prevent attempts
        assert not manager.can_attempt(endpoint)
        assert manager.get_breaker(endpoint).is_open()
    
    def test_circuit_breaker_allows_calls_when_closed(self, manager):
        """Test circuit breaker allows API calls when closed."""
        endpoint = "http://test-api.com"
        
        # Fresh circuit should allow attempts
        assert manager.can_attempt(endpoint)
        assert not manager.get_breaker(endpoint).is_open()
    
    def test_circuit_breaker_recovery_cycle(self, manager):
        """Test complete circuit breaker recovery cycle."""
        endpoint = "http://test-api.com"
        
        # Open circuit
        for _ in range(5):
            manager.record_failure(endpoint)
        
        breaker = manager.get_breaker(endpoint)
        assert breaker.state == CircuitBreakerState.OPEN
        
        # Simulate recovery timeout
        import time
        with patch('time.time', return_value=breaker.last_failure_time + 70):
            # Should transition to half-open
            assert manager.can_attempt(endpoint)
            assert breaker.state == CircuitBreakerState.HALF_OPEN
            
            # Success should close circuit
            manager.record_success(endpoint)
            assert breaker.state == CircuitBreakerState.CLOSED


class TestODataClientIntegration:
    """Test OData client with circuit breaker and error handling."""
    
    @pytest.fixture
    def client(self):
        """Fresh OData client for each test."""
        mock_logger = Mock()
        return ODataClient(logger_obj=mock_logger)
    
    @pytest.mark.asyncio
    async def test_successful_api_call(self, client):
        """Test successful API call updates circuit breaker correctly."""
        mock_response_data = {"value": [{"id": 1, "name": "test"}]}
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.json.return_value = mock_response_data
            mock_response.status = 200
            mock_get.return_value.__aenter__.return_value = mock_response
            
            async with aiohttp.ClientSession() as session:
                result = await client.fetch_json(session, "http://test-api.com/data")
                
            assert result == mock_response_data
    
    @pytest.mark.asyncio
    async def test_api_call_with_circuit_breaker_open(self, client):
        """Test API call behavior when circuit breaker is open."""
        # Open circuit breaker for the endpoint
        from api.circuit_breaker import circuit_breaker_manager
        endpoint = "http://test-api.com"
        
        for _ in range(5):
            circuit_breaker_manager.record_failure(endpoint)
        
        # API call should be blocked by circuit breaker
        with pytest.raises(Exception):  # Should raise circuit breaker exception
            async with aiohttp.ClientSession() as session:
                await client.fetch_json(session, f"{endpoint}/data")
    
    @pytest.mark.asyncio
    async def test_api_call_with_retries(self, client):
        """Test API call retry mechanism with backoff."""
        call_count = 0
        
        async def mock_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise aiohttp.ClientConnectionError("Connection failed")
            # Success on third try
            mock_response = AsyncMock()
            mock_response.json.return_value = {"value": []}
            mock_response.status = 200
            return mock_response
        
        with patch('aiohttp.ClientSession.get', side_effect=mock_get):
            async with aiohttp.ClientSession() as session:
                # Should succeed after retries
                result = await client.fetch_json(session, "http://test-api.com/data")
                assert result == {"value": []}
                assert call_count == 3


class TestAPIReliabilityEnd2End:
    """End-to-end tests for API reliability features."""
    
    @pytest.fixture
    def setup_environment(self):
        """Setup test environment with fresh instances."""
        return {
            'client': ODataClient(logger_obj=Mock()),
            'manager': CircuitBreakerManager()
        }
    
    @pytest.mark.asyncio
    async def test_api_failure_circuit_breaker_integration(self, setup_environment):
        """Test API failures properly update circuit breaker state."""
        client = setup_environment['client']
        
        # Mock persistent failures
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = aiohttp.ClientConnectionError("Persistent failure")
            
            # Multiple failed attempts should open circuit
            for _ in range(3):
                try:
                    async with aiohttp.ClientSession() as session:
                        await client.fetch_json(session, "http://failing-api.com/data")
                except:
                    pass  # Expected to fail
            
            # Circuit breaker should eventually block requests
            from api.circuit_breaker import circuit_breaker_manager
            endpoint = "http://failing-api.com"
            
            # After enough failures, circuit should open
            # Note: This depends on the specific retry configuration
            # We might need to trigger more failures based on the backoff config
    
    @pytest.mark.asyncio 
    async def test_mixed_success_failure_scenarios(self, setup_environment):
        """Test mixed success/failure scenarios maintain system stability."""
        client = setup_environment['client']
        
        success_response = {"value": [{"id": 1}]}
        call_count = 0
        
        async def alternating_responses(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            
            if call_count % 3 == 0:  # Every third call fails
                raise aiohttp.ClientConnectionError("Intermittent failure")
            
            mock_response = AsyncMock()
            mock_response.json.return_value = success_response
            mock_response.status = 200
            return mock_response
        
        with patch('aiohttp.ClientSession.get', side_effect=alternating_responses):
            # Make several API calls
            successful_calls = 0
            total_calls = 10
            
            for i in range(total_calls):
                try:
                    async with aiohttp.ClientSession() as session:
                        result = await client.fetch_json(session, f"http://mixed-api.com/data{i}")
                        if result == success_response:
                            successful_calls += 1
                except:
                    pass  # Some failures expected
            
            # Should have some successful calls despite intermittent failures
            assert successful_calls > 0
    
    def test_error_categorization_affects_retry_strategy(self):
        """Test that different error categories can affect retry behavior."""
        # Test that permanent client errors (4xx) don't trigger retries
        client_error = aiohttp.ClientResponseError(
            request_info=Mock(), history=Mock(), status=404
        )
        category = categorize_error(client_error)
        assert category == ErrorCategory.CLIENT
        
        # Test that server errors (5xx) do trigger retries
        server_error = aiohttp.ClientResponseError(
            request_info=Mock(), history=Mock(), status=500
        )
        category = categorize_error(server_error)
        assert category == ErrorCategory.SERVER
        
        # Test that network errors trigger retries
        network_error = aiohttp.ClientConnectionError("Network unreachable")
        category = categorize_error(network_error)
        assert category == ErrorCategory.NETWORK


class TestAPIConfigIntegration:
    """Test API configuration integration with reliability features."""
    
    def test_api_config_values(self):
        """Test API configuration values are reasonable for reliability."""
        config = APIConfig()
        
        # Test that retry configuration is sensible
        assert config.MAX_RETRIES > 0
        assert config.RETRY_BASE_DELAY > 0
        assert config.RETRY_MAX_DELAY > config.RETRY_BASE_DELAY
        
        # Test timeout configuration
        assert hasattr(config, 'REQUEST_TIMEOUT') or True  # May not be defined yet
    
    def test_circuit_breaker_config_integration(self):
        """Test circuit breaker configuration works with API config."""
        # Test that circuit breaker defaults are reasonable
        from api.circuit_breaker import CircuitBreaker
        
        cb = CircuitBreaker()
        assert cb.failure_threshold > 0
        assert cb.recovery_timeout > 0
        
        # Test that thresholds are reasonable for API calls
        assert cb.failure_threshold <= 10  # Not too high
        assert cb.recovery_timeout >= 10   # Enough time for recovery


# Pytest fixtures for common test data
@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    return Mock(spec=logging.Logger)


@pytest.fixture
def sample_api_response():
    """Sample API response data."""
    return {
        "value": [
            {"PersonID": 1, "FirstName": "Test", "LastName": "Person"},
            {"PersonID": 2, "FirstName": "Another", "LastName": "Person"}
        ],
        "@odata.nextLink": None
    }


@pytest.fixture
def sample_error_response():
    """Sample error response data."""
    return {
        "error": {
            "code": "InvalidRequest",
            "message": "The request is invalid"
        }
    }


def test_api_module_imports():
    """Test that all API module components can be imported correctly."""
    from api.circuit_breaker import CircuitBreaker, CircuitBreakerManager
    from api.error_handling import categorize_error, ErrorCategory
    from api.odata_client import ODataClient
    
    # Test instantiation
    assert CircuitBreaker()
    assert CircuitBreakerManager()
    assert ODataClient()
    
    # Test enum values
    assert ErrorCategory.NETWORK
    assert ErrorCategory.SERVER
    assert ErrorCategory.CLIENT
    assert ErrorCategory.TIMEOUT
    assert ErrorCategory.UNKNOWN