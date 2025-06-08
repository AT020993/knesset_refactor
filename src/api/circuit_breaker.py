"""Circuit breaker implementation for API endpoints."""

import time
import threading
from enum import Enum
from typing import Dict
import logging

from config.api import CircuitBreakerState


class CircuitBreaker:
    """Circuit breaker implementation for API endpoints."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED
        self._logger = logging.getLogger(__name__)
    
    def record_success(self):
        """Record a successful operation."""
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED
        self.last_failure_time = None
    
    def record_failure(self):
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self._logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
    
    def can_attempt(self) -> bool:
        """Check if we can attempt a request."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self._logger.info("Circuit breaker transitioning to half-open")
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        return self.state == CircuitBreakerState.OPEN


class CircuitBreakerManager:
    """Manages circuit breakers for different endpoints."""
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()
    
    def get_breaker(self, endpoint: str) -> CircuitBreaker:
        """Get or create a circuit breaker for an endpoint."""
        with self._lock:
            if endpoint not in self._breakers:
                self._breakers[endpoint] = CircuitBreaker()
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