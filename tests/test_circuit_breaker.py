import unittest
from unittest.mock import patch, MagicMock, call
import time
import logging

# Assuming src is in PYTHONPATH or accessible
from src.api.circuit_breaker import CircuitBreaker
from src.api.error_handling import CircuitBreakerOpenException
from config.api import CircuitBreakerState # Assuming this path is correct for CircuitBreakerState

# Disable logging for tests unless specifically testing log output
logging.disable(logging.CRITICAL)

class TestCircuitBreaker(unittest.TestCase):

    def setUp(self):
        # Basic setup for most tests
        self.mock_logger = MagicMock()
        # Patching 'src.api.circuit_breaker.logging' assumes the logger in CircuitBreaker is obtained via logging.getLogger(__name__)
        # and __name__ for that module is 'src.api.circuit_breaker'
        self.logger_patch = patch('src.api.circuit_breaker.logging.getLogger', return_value=self.mock_logger)
        self.addCleanup(self.logger_patch.stop) # Stop patch after test method
        self.logger_patch.start() # Start patch before test method

        self.failure_threshold = 2
        self.recovery_timeout = 10  # seconds
        self.max_retries = 3
        self.backoff_factor = 0.01 # Use small backoff for faster tests

        self.cb = CircuitBreaker(
            failure_threshold=self.failure_threshold,
            recovery_timeout=self.recovery_timeout,
            max_retries=self.max_retries,
            backoff_factor=self.backoff_factor
        )

    def test_initialization_defaults(self):
        cb = CircuitBreaker()
        self.assertEqual(cb.failure_threshold, 5)
        self.assertEqual(cb.recovery_timeout, 60)
        self.assertEqual(cb.max_retries, 3)
        self.assertEqual(cb.backoff_factor, 2)
        self.assertEqual(cb.state, CircuitBreakerState.CLOSED)

    def test_initialization_custom_values(self):
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=120, max_retries=5, backoff_factor=1)
        self.assertEqual(cb.failure_threshold, 10)
        self.assertEqual(cb.recovery_timeout, 120)
        self.assertEqual(cb.max_retries, 5)
        self.assertEqual(cb.backoff_factor, 1)

    def test_initialization_invalid_max_retries(self):
        with self.assertRaisesRegex(ValueError, "max_retries must be at least 1"):
            CircuitBreaker(max_retries=0)
        with self.assertRaisesRegex(ValueError, "max_retries must be at least 1"):
            CircuitBreaker(max_retries=-1)

    def test_successful_execution_first_attempt(self):
        mock_function = MagicMock(return_value="success")

        result = self.cb.execute(mock_function, "arg1", kwarg1="kwarg_val")

        self.assertEqual(result, "success")
        mock_function.assert_called_once_with("arg1", kwarg1="kwarg_val")
        self.assertEqual(self.cb.state, CircuitBreakerState.CLOSED)
        self.assertEqual(self.cb.successful_calls, 1)
        self.assertEqual(self.cb.failure_count, 0)
        self.assertGreater(self.cb.avg_response_time, 0)

        # Check logs
        self.mock_logger.info.assert_any_call(unittest.mock.ANY) # Successful call log
        # Example of more specific log check:
        # self.mock_logger.info.assert_any_call(
        #     f"Call successful on attempt 1. Response time: {self.cb.avg_response_time:.4f}s. "
        #     f"Average response time: {self.cb.avg_response_time:.4f}s. Total successful calls: 1"
        # )


    @patch('src.api.circuit_breaker.time.sleep', return_value=None) # Mock time.sleep
    def test_failure_then_success_within_retries(self, mock_sleep):
        mock_function = MagicMock()
        mock_function.side_effect = [Exception("Fail1"), "success"]

        result = self.cb.execute(mock_function)

        self.assertEqual(result, "success")
        self.assertEqual(mock_function.call_count, 2)
        self.assertEqual(self.cb.state, CircuitBreakerState.CLOSED)
        self.assertEqual(self.cb.successful_calls, 1)
        self.assertEqual(self.cb.failure_count, 0) # Resets on success

        # Check logs for retry and success
        self.mock_logger.error.assert_any_call("Attempt 1 failed with exception: Fail1")
        self.mock_logger.info.assert_any_call(f"Retrying in {self.backoff_factor * (2**(1-1))} seconds... (Attempt 1/{self.max_retries})")
        mock_sleep.assert_called_once_with(self.backoff_factor * (2**(1-1)))
        self.mock_logger.info.assert_any_call(unittest.mock.ANY) # Successful call log on 2nd attempt


    @patch('src.api.circuit_breaker.time.sleep', return_value=None)
    def test_failure_exhausts_retries_and_opens_circuit(self, mock_sleep):
        mock_function = MagicMock(side_effect=Exception("Persistent failure"))

        # First set of failures to reach threshold
        for _ in range(self.failure_threshold): # Will call record_failure this many times
            with self.assertRaisesRegex(Exception, "Persistent failure"):
                self.cb.execute(mock_function)

        self.assertEqual(self.cb.state, CircuitBreakerState.OPEN)
        self.assertEqual(self.cb.failure_count, self.failure_threshold)
        self.assertEqual(self.cb.failed_calls, self.failure_threshold)
        self.assertEqual(mock_function.call_count, self.failure_threshold * self.max_retries)

        # Check logs
        self.mock_logger.warning.assert_any_call(
            f"Circuit breaker opened after {self.failure_threshold} failures. State changed to {CircuitBreakerState.OPEN}"
        )
        # Check error logs for each attempt
        for i in range(1, self.max_retries + 1):
            self.mock_logger.error.assert_any_call(f"Attempt {i} failed with exception: Persistent failure")
        self.mock_logger.error.assert_any_call(
            f"All {self.max_retries} retries failed. Final failure recorded. Total failed calls for this breaker: {self.cb.failed_calls}"
        )

        # Further calls should be blocked
        with self.assertRaises(CircuitBreakerOpenException):
            self.cb.execute(mock_function)
        self.mock_logger.warning.assert_any_call("Circuit breaker is open. Call skipped.")


    @patch('src.api.circuit_breaker.time.sleep', return_value=None)
    @patch('src.api.circuit_breaker.time.time')
    def test_open_to_half_open_and_then_closed(self, mock_time, mock_sleep):
        # Start with a consistent time - set BEFORE any operations
        current_time = 1000.0
        mock_time.return_value = current_time

        # 1. Open the circuit
        # mock_time is already set to current_time, so record_failure will use 1000.0
        mock_function_fail = MagicMock(side_effect=Exception("Fail"))
        for _ in range(self.failure_threshold):
            with self.assertRaises(Exception):
                self.cb.execute(mock_function_fail)
        self.assertEqual(self.cb.state, CircuitBreakerState.OPEN)
        self.mock_logger.warning.assert_any_call(
            f"Circuit breaker opened after {self.failure_threshold} failures. State changed to {CircuitBreakerState.OPEN}"
        )

        # 2. Verify last_failure_time was set correctly
        initial_failure_time = self.cb.last_failure_time
        self.assertIsNotNone(initial_failure_time)
        self.assertEqual(initial_failure_time, current_time)

        # 3. Advance time past recovery_timeout
        mock_time.return_value = current_time + self.recovery_timeout + 1

        # 4. Attempt a call - should go to HALF_OPEN
        self.assertTrue(self.cb.can_attempt()) # This transitions to HALF_OPEN
        self.assertEqual(self.cb.state, CircuitBreakerState.HALF_OPEN)
        self.mock_logger.info.assert_any_call(
            f"Circuit breaker transitioning to half-open. State changed from {CircuitBreakerState.OPEN} to {CircuitBreakerState.HALF_OPEN}"
        )

        # 5. Successful call in HALF_OPEN should close the circuit
        # Advance time slightly for the successful call
        mock_time.return_value = current_time + self.recovery_timeout + 2
        mock_function_success = MagicMock(return_value="Half-open success")
        result = self.cb.execute(mock_function_success) # Uses the same cb instance

        self.assertEqual(result, "Half-open success")
        self.assertEqual(self.cb.state, CircuitBreakerState.CLOSED)
        self.assertEqual(self.cb.failure_count, 0)
        self.mock_logger.info.assert_any_call(f"Circuit breaker state changed to {CircuitBreakerState.CLOSED}") # From record_success
        self.mock_logger.info.assert_any_call(unittest.mock.ANY) # Successful call log


    @patch('src.api.circuit_breaker.time.sleep', return_value=None)
    @patch('src.api.circuit_breaker.time.time')
    def test_open_to_half_open_and_then_back_to_open(self, mock_time, mock_sleep):
        # Start with a consistent time - set BEFORE any operations
        current_time = 1000.0
        mock_time.return_value = current_time

        # 1. Open the circuit
        # mock_time is already set to current_time, so record_failure will use 1000.0
        mock_function_fail = MagicMock(side_effect=Exception("Fail"))
        for _ in range(self.failure_threshold):
            with self.assertRaises(Exception):
                self.cb.execute(mock_function_fail)
        self.assertEqual(self.cb.state, CircuitBreakerState.OPEN)

        # 2. Verify last_failure_time was set correctly
        initial_failure_time = self.cb.last_failure_time
        self.assertEqual(initial_failure_time, current_time)

        # 3. Advance time past recovery_timeout
        mock_time.return_value = current_time + self.recovery_timeout + 1

        # 4. Attempt a call - should go to HALF_OPEN
        self.assertTrue(self.cb.can_attempt())
        self.assertEqual(self.cb.state, CircuitBreakerState.HALF_OPEN)
        self.mock_logger.info.assert_any_call(
            f"Circuit breaker transitioning to half-open. State changed from {CircuitBreakerState.OPEN} to {CircuitBreakerState.HALF_OPEN}"
        )

        # 5. Failed call in HALF_OPEN should re-open the circuit
        # Advance time slightly for the failed call
        mock_time.return_value = current_time + self.recovery_timeout + 2
        mock_function_fail_again = MagicMock(side_effect=Exception("Half-open fail"))

        # In HALF_OPEN, a failure will trigger record_failure which will re-open the circuit
        # because failure_count is already at threshold (from step 1)
        with self.assertRaisesRegex(Exception, "Half-open fail"):
            self.cb.execute(mock_function_fail_again)

        self.assertEqual(self.cb.state, CircuitBreakerState.OPEN)
        # failure_count increments on each record_failure call
        # After opening the first time, it's at threshold (2)
        # After the HALF_OPEN failure, it's at threshold + 1 (3)
        self.assertGreaterEqual(self.cb.failure_count, self.failure_threshold)
        self.mock_logger.warning.assert_any_call(
             f"Circuit breaker opened after {self.cb.failure_count} failures. State changed to {CircuitBreakerState.OPEN}"
        )


    @patch('src.api.circuit_breaker.time.sleep', return_value=None)
    def test_exponential_backoff_delays(self, mock_sleep):
        mock_function = MagicMock(side_effect=Exception("Fail"))

        with self.assertRaises(Exception):
            self.cb.execute(mock_function)

        expected_delays = []
        for i in range(1, self.max_retries): # Retries happen max_retries - 1 times
            delay = self.backoff_factor * (2**(i-1))
            expected_delays.append(call(delay))

        # mock_sleep.assert_has_calls(expected_delays)
        # The calls are (backoff_factor * 1), (backoff_factor * 2)
        # For attempt = 1 (first call), if fails, sleep for backoff_factor * (2**0) = backoff_factor * 1
        # For attempt = 2 (second call), if fails, sleep for backoff_factor * (2**1) = backoff_factor * 2
        # Loop is range(1, max_retries + 1). Sleep happens if attempt < max_retries.

        call_list = []
        if self.max_retries > 1: # sleep is only called if there are retries
            for attempt_num in range(1, self.max_retries): # attempts 1 to max_retries-1 will sleep
                 delay = self.backoff_factor * (2 ** (attempt_num - 1))
                 call_list.append(call(delay))
            mock_sleep.assert_has_calls(call_list)
        self.assertEqual(mock_sleep.call_count, self.max_retries -1 if self.max_retries > 0 else 0)


    def test_execute_respects_can_attempt_false(self):
        self.cb.state = CircuitBreakerState.OPEN
        # Ensure it's not in recovery window
        self.cb.last_failure_time = time.time()

        mock_function = MagicMock()
        with self.assertRaises(CircuitBreakerOpenException):
            self.cb.execute(mock_function)

        mock_function.assert_not_called()
        self.mock_logger.warning.assert_called_with("Circuit breaker is open. Call skipped.")

    @patch('src.api.circuit_breaker.time.sleep', return_value=None)
    def test_circuit_opens_during_retries_if_threshold_met_concurrently(self, mock_sleep):
        # Scenario: CB is CLOSED. A call starts, fails once.
        # Before it retries, other "concurrent" calls cause failures that open the CB.
        # The retrying call should then not proceed with further retries.

        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10, max_retries=3, backoff_factor=0.01)
        # Must re-patch logger for this new instance if we want to inspect its logs specifically
        # For simplicity, we'll rely on the setUp mock_logger if CircuitBreaker internally uses a fixed logger name
        # Or, pass the mock_logger into the CB if its __init__ allowed, which it doesn't.
        # The global patch should still work.

        mock_function = MagicMock(side_effect=Exception("Fail"))

        original_can_attempt = cb.can_attempt
        call_count_can_attempt = 0

        def side_effect_can_attempt():
            nonlocal call_count_can_attempt
            call_count_can_attempt += 1
            if call_count_can_attempt == 2: # Second check of can_attempt (before retry)
                # Simulate circuit opened by another thread
                cb.state = CircuitBreakerState.OPEN
                cb.last_failure_time = time.time()
            return original_can_attempt()

        with patch.object(cb, 'can_attempt', side_effect=side_effect_can_attempt) as mock_can_attempt:
            with self.assertRaises(CircuitBreakerOpenException) as context: # Expecting the specific exception
                 cb.execute(mock_function)

        self.assertTrue("Circuit breaker opened during retry attempts" in str(context.exception) or \
                        "Circuit breaker is open" in str(context.exception)) # Check for specific message from execute

        mock_function.assert_called_once() # Only the first attempt should happen
        self.assertEqual(cb.state, CircuitBreakerState.OPEN)
        # Check logs for the warning
        self.mock_logger.warning.assert_any_call("Circuit breaker is open after failure. Skipping further retries.")


if __name__ == '__main__':
    logging.disable(logging.NOTSET) # Enable logging for direct script run if needed for debugging
    # Example: python -m unittest tests.test_circuit_breaker.TestCircuitBreaker.test_successful_execution_first_attempt
    unittest.main()
