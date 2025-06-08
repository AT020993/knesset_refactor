"""Error handling and categorization for API operations."""

import asyncio
import json
from enum import Enum

import aiohttp


class ErrorCategory(Enum):
    """Categories for different types of API errors."""
    NETWORK = "network"
    SERVER = "server"
    CLIENT = "client"
    TIMEOUT = "timeout"
    DATA = "data"
    UNKNOWN = "unknown"


def categorize_error(exception: Exception) -> ErrorCategory:
    """Categorize an exception into error types for better handling."""
    if isinstance(exception, asyncio.TimeoutError):
        return ErrorCategory.TIMEOUT
    elif isinstance(exception, aiohttp.ClientConnectorError):
        return ErrorCategory.NETWORK
    elif isinstance(exception, aiohttp.ClientResponseError):
        if 400 <= exception.status < 500:
            return ErrorCategory.CLIENT
        elif 500 <= exception.status < 600:
            return ErrorCategory.SERVER
        else:
            return ErrorCategory.UNKNOWN
    elif isinstance(exception, aiohttp.ClientError):
        return ErrorCategory.NETWORK
    elif isinstance(exception, (json.JSONDecodeError, ValueError)):
        return ErrorCategory.DATA
    else:
        return ErrorCategory.UNKNOWN


class CircuitBreakerOpenException(Exception):
    """Exception raised when an operation is attempted while the circuit breaker is open."""
    def __init__(self, message="Circuit breaker is open and cannot accept new calls"):
        self.message = message
        super().__init__(self.message)