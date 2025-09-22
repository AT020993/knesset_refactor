"""API configuration for OData endpoints."""

from enum import Enum


class ErrorCategory(Enum):
    """Categories for different types of API errors."""

    NETWORK = "network"
    SERVER = "server"
    CLIENT = "client"
    TIMEOUT = "timeout"
    DATA = "data"
    UNKNOWN = "unknown"


class CircuitBreakerState(Enum):
    """States for the circuit breaker pattern."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class APIConfig:
    """API configuration and settings."""

    # OData endpoint
    BASE_URL = "http://knesset.gov.il/Odata/ParliamentInfo.svc"

    # Request settings
    PAGE_SIZE = 100
    MAX_RETRIES = 8
    REQUEST_TIMEOUT = 60
    CONCURRENCY_LIMIT = 8

    # Circuit breaker settings
    CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT = 60

    # Retry settings
    RETRY_BASE_DELAY = 2
    RETRY_MAX_DELAY = 60
    RETRY_JITTER = True

    @classmethod
    def get_entity_url(cls, entity: str) -> str:
        """Get the full URL for an OData entity."""
        return f"{cls.BASE_URL}/{entity}"

    @classmethod
    def get_count_url(cls, entity: str) -> str:
        """Get the count URL for an OData entity."""
        return f"{cls.BASE_URL}/{entity}/$count"
