"""API clients and communication modules.

Note: ODataClient and error_handling are NOT eagerly imported here because they
depend on aiohttp, which may not be installed on Streamlit Cloud.
Import directly when needed:
    from api.odata_client import ODataClient
    from api.error_handling import ErrorCategory, categorize_error
"""

from .circuit_breaker import CircuitBreaker

__all__ = ["CircuitBreaker"]