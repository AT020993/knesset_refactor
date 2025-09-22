"""Circuit breaker implementation for API endpoints."""

import logging
import threading
import time
from typing import Dict, Optional

from config.api import CircuitBreakerState
from src.api.error_handling import CircuitBreakerOpenException


class CircuitBreaker:
    """Circuit breaker implementation for API endpoints."""

    def __init__(
        self, failure_threshold: int = 5, recovery_timeout: int = 60, max_retries: int = 3, backoff_factor: int = 2
    ):
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.failure_count = 0
        self.last_failure_time = None
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
            self._logger.warning(
                f"Circuit breaker opened after {self.failure_count} failures. State changed to {self.state}"
            )

    def can_attempt(self) -> bool:
        """Check if we can attempt a request."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time is not None and time.time() - self.last_failure_time >= self.recovery_timeout:
                old_state = self.state
                self.state = CircuitBreakerState.HALF_OPEN
                self._logger.info(
                    f"Circuit breaker transitioning to half-open. State changed from {old_state} to {self.state}"
                )
                return True
            return False
        else:  # HALF_OPEN
            return True

    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        return self.state == CircuitBreakerState.OPEN

    def execute(self, func, *args, **kwargs):
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
                if self.successful_calls > 0:
                    self.avg_response_time = self.total_response_time / self.successful_calls
                else:
                    self.avg_response_time = 0.0  # Should not happen if successful_calls is incremented first

                self._logger.info(
                    f"Call successful on attempt {attempt}. Response time: {response_time:.4f}s. Average response time: {self.avg_response_time:.4f}s. Total successful calls: {self.successful_calls}"
                )
                return result
            except Exception as e:
                self._logger.error(f"Attempt {attempt} failed with exception: {e}")
                if attempt == self.max_retries:
                    self.record_failure()  # Increments failed_calls
                    self._logger.error(
                        f"All {self.max_retries} retries failed. Final failure recorded. Total failed calls for this breaker: {self.failed_calls}"
                    )
                    raise  # Re-raise the last exception

                if (
                    not self.can_attempt()
                ):  # Check again, state might have changed due to record_failure by another thread
                    self._logger.warning("Circuit breaker is open after failure. Skipping further retries.")
                    raise CircuitBreakerOpenException("Circuit breaker opened during retry attempts") from e

                delay = self.backoff_factor * (2 ** (attempt - 1))
                self._logger.info(f"Retrying in {delay} seconds... (Attempt {attempt}/{self.max_retries})")
                time.sleep(delay)
        # This part should ideally not be reached if max_retries > 0
        # If max_retries is 0, it will fall through here after the first failed attempt.
        # However, the loop structure for attempt in range(1, self.max_retries + 1) handles max_retries=0 correctly (loop doesn't run).
        # For safety, if it's reached, it means all retries (if any) failed or no retries were configured.
        # This logic is now inside the loop for the last attempt.
        # If max_retries is 0, it means no retries, so a single failure is recorded.
        # This case needs to be handled if max_retries can be 0.
        # Current loop `range(1, self.max_retries + 1)` means at least one attempt if max_retries >=0.
        # If max_retries = 0, range(1,1) means no iterations.
        # Let's adjust max_retries default to 1 if 0 retries means 1 attempt.
        # Or, more clearly, let max_retries be the number of *retry* attempts. So 0 means 1 attempt total.
        # The current code implies max_retries is total attempts. Let's stick to that.
        # If max_retries = 3, it means 3 total attempts.

        # Consider the case where max_retries = 0 (or negative, though type hint says int).
        # If max_retries = 0, the loop range(1,1) doesn't run.
        # This means the function needs to be called at least once outside or before the loop.
        # Let's reconsider the loop and retry logic slightly for clarity if max_retries means *re*-tries.
        # If max_retries is the number of *additional* attempts:
        # So, total attempts = 1 (initial) + self.max_retries.
        # The loop should then be range(self.max_retries).

        # Let's assume self.max_retries is the total number of attempts.
        # So, if self.max_retries = 1, one attempt. If 0, no attempts (which is weird).
        # Let's assume max_retries = 3 means 1 initial + 2 retries. So loop for retries.

        # Re-evaluating the loop: for attempt in range(1, self.max_retries + 1)
        # If max_retries = 3: attempts 1, 2, 3. This is good.
        # If max_retries = 1: attempt 1. Good.
        # If max_retries = 0: loop range(1,1) is empty. This is a bug. It should attempt once.
        # Let's adjust the loop to be `for attempt in range(self.max_retries)` and handle initial attempt.

        # Simpler: let max_retries be the number of *re*-tries. Initial attempt + max_retries.
        # The current loop structure is fine if max_retries is total attempts and >= 1.
        # Let's ensure max_retries in __init__ defaults to a minimum of 1 for "total attempts".
        # Or, if max_retries is "number of retries", then it can be 0.
        # The prompt says "max_retries=3", let's assume this means 1 initial + 2 retries.
        # So the loop should run `self.max_retries` times for retries, after an initial attempt.

        # Let's stick to `max_retries` as total attempts for now, as the loop is written that way.
        # And ensure `max_retries` is at least 1. The default is 3, which is fine.
        # The `execute` method needs to be robust if `max_retries` is set to 0 or 1 by the user.
        # If `max_retries` is 0, `range(1, 1)` is empty. The call to `func` would not happen.
        # This is a definite bug with the current loop structure if `max_retries` can be 0.
        # Let's modify the loop and initial call.

        # Corrected structure:
        # initial_attempt = True
        # for attempt_num in range(self.max_retries): # if max_retries = 3, attempts 0, 1, 2
        #   if not initial_attempt: time.sleep(delay)
        #   try: ...
        #   except: ... if attempt_num == self.max_retries -1: record_failure()
        #   initial_attempt = False

        # The current loop `for attempt in range(1, self.max_retries + 1)` is actually fine
        # if `self.max_retries` means "total number of attempts allowed".
        # If `self.max_retries` is 0, it means 0 attempts. If 1, 1 attempt.
        # The prompt's "max_retries=3" likely means 3 total attempts.
        # Let's assume `max_retries` in `__init__` means total attempts and must be >= 1.
        # The default of 3 is fine. If user passes 0, it's an issue.
        # We should add a check for `self.max_retries >= 1` or adjust logic.
        # For now, assume `self.max_retries >= 1`.

        # A note on logging: structured logging usually means JSON or key-value pairs.
        # Current logging is string formatting. This might be okay depending on project standards.
        # I'll keep it as is for now but note it.

        # Add specific exception for circuit breaker open, as noted in TODO.
        # This can be done in `error_handling.py` if it exists, or defined here.
        # For now, a generic Exception is used.


class CircuitBreakerManager:
    """Manages circuit breakers for different endpoints."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_breaker(
        self,
        endpoint: str,
        failure_threshold: Optional[int] = None,
        recovery_timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[int] = None,
    ) -> CircuitBreaker:
        """Get or create a circuit breaker for an endpoint.

        Optional parameters are only used if a new CircuitBreaker instance is created.
        If a breaker for the endpoint already exists, it's returned with its current configuration.
        """
        with self._lock:
            if endpoint not in self._breakers:
                kwargs = {}
                if failure_threshold is not None:
                    kwargs["failure_threshold"] = failure_threshold
                if recovery_timeout is not None:
                    kwargs["recovery_timeout"] = recovery_timeout
                if max_retries is not None:
                    kwargs["max_retries"] = max_retries
                if backoff_factor is not None:
                    kwargs["backoff_factor"] = backoff_factor

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
