"""Circuit breaker implementation for API endpoints."""

import time
import threading
from typing import Any, Callable, Dict, Optional, TypeVar
import logging

from config.api import CircuitBreakerState
from .error_handling import CircuitBreakerOpenException

T = TypeVar("T")


class CircuitBreaker:
    """Circuit breaker implementation for API endpoints."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        max_retries: int = 3,
        backoff_factor: int = 2,
    ):
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitBreakerState.CLOSED
        self._logger = logging.getLogger(__name__)
        self.successful_calls = 0
        self.failed_calls = 0
        self.total_response_time = 0.0
        self.avg_response_time = 0.0

    def record_success(self):
        """Record a successful operation."""
        old_state = self.state
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED
        self.last_failure_time = None
        self.successful_calls += 1
        if old_state != CircuitBreakerState.CLOSED:
            self._logger.info(f"Circuit breaker state changed to {self.state}")

    def record_failure(self):
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        self.failed_calls += 1

        if self.failure_count >= self.failure_threshold and self.state != CircuitBreakerState.OPEN:
            self.state = CircuitBreakerState.OPEN
            self._logger.warning(f"Circuit breaker opened after {self.failure_count} failures. State changed to {self.state}")

    def can_attempt(self) -> bool:
        """Check if we can attempt a request."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time is None:
                return True
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                old_state = self.state
                self.state = CircuitBreakerState.HALF_OPEN
                self._logger.info(f"Circuit breaker transitioning to half-open. State changed from {old_state} to {self.state}")
                return True
            return False
        # HALF_OPEN
        return True

    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        return self.state == CircuitBreakerState.OPEN

    def execute(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute the function with circuit breaker and retry logic."""
        if not self.can_attempt():
            self._logger.warning("Circuit breaker is open. Call skipped.")
            raise CircuitBreakerOpenException()

        for attempt in range(1, self.max_retries + 1):
            try:
                start_time = time.time()
                result = func(*args, **kwargs)
                end_time = time.time()
                response_time = end_time - start_time

                # total_response_time accumulates only for successful calls
                self.total_response_time += response_time
                self.record_success()  # Increments successful_calls
                # Calculate average (successful_calls is always > 0 after record_success)
                self.avg_response_time = self.total_response_time / self.successful_calls

                self._logger.info(f"Call successful on attempt {attempt}. Response time: {response_time:.4f}s. Average response time: {self.avg_response_time:.4f}s. Total successful calls: {self.successful_calls}")
                return result
            except Exception as e:
                self._logger.error(f"Attempt {attempt} failed with exception: {e}")
                if attempt == self.max_retries:
                    self.record_failure() # Increments failed_calls
                    self._logger.error(f"All {self.max_retries} retries failed. Final failure recorded. Total failed calls for this breaker: {self.failed_calls}")
                    raise  # Re-raise the last exception

                if not self.can_attempt(): # Check again, state might have changed due to record_failure by another thread
                    self._logger.warning("Circuit breaker is open after failure. Skipping further retries.")
                    raise CircuitBreakerOpenException("Circuit breaker opened during retry attempts") from e

                delay = self.backoff_factor * (2 ** (attempt - 1))
                self._logger.info(f"Retrying in {delay} seconds... (Attempt {attempt}/{self.max_retries})")
                time.sleep(delay)
        raise RuntimeError("Circuit breaker execution exited without result")


class CircuitBreakerManager:
    """Manages circuit breakers for different endpoints."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_breaker(
        self,
        endpoint: str,
        failure_threshold: int | None = None,
        recovery_timeout: int | None = None,
        max_retries: int | None = None,
        backoff_factor: int | None = None,
    ) -> CircuitBreaker:
        """Get or create a circuit breaker for an endpoint.

        Optional parameters are only used if a new CircuitBreaker instance is created.
        If a breaker for the endpoint already exists, it's returned with its current configuration.
        """
        with self._lock:
            if endpoint not in self._breakers:
                kwargs = {}
                if failure_threshold is not None:
                    kwargs['failure_threshold'] = failure_threshold
                if recovery_timeout is not None:
                    kwargs['recovery_timeout'] = recovery_timeout
                if max_retries is not None:
                    kwargs['max_retries'] = max_retries
                if backoff_factor is not None:
                    kwargs['backoff_factor'] = backoff_factor

                self._breakers[endpoint] = CircuitBreaker(**kwargs)
            return self._breakers[endpoint]

    def record_success(self, endpoint: str):
        """Record a successful operation for an endpoint."""
        self.get_breaker(endpoint).record_success()

    def record_failure(self, endpoint: str):
        """Record a failed operation for an endpoint."""
        self.get_breaker(endpoint).record_failure()

    def can_attempt(self, endpoint: str) -> bool:
        """Check if we can attempt a request to an endpoint."""
        return self.get_breaker(endpoint).can_attempt()


# Global circuit breaker manager
circuit_breaker_manager = CircuitBreakerManager()
