"""API clients and communication modules.

Note: ODataClient is NOT eagerly imported here because it depends on aiohttp,
which may not be installed on Streamlit Cloud (only needed for data refresh).
Use: from api.odata_client import ODataClient
"""

from .circuit_breaker import CircuitBreaker
from .error_handling import ErrorCategory, categorize_error

__all__ = ["CircuitBreaker", "ErrorCategory", "categorize_error"]