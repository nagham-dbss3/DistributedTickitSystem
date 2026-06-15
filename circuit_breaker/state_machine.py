"""
Circuit Breaker state machine implementation.

States:
- CLOSED: normal operation, requests allowed. Failures increment a counter.
- OPEN: requests are short-circuited. After `recovery_timeout` expires the
  breaker moves to HALF_OPEN to probe for recovery.
- HALF_OPEN: allow a limited number of trial requests; successes move back
  to CLOSED, failures return to OPEN.

Usage:
    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=10, half_open_successes=3)
    try:
        result = cb.call(my_request_function, *args, **kwargs)
    except CircuitOpenError:
        # handle short-circuit

"""
from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Callable, Optional


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """Raised when a call is attempted while the circuit breaker is OPEN."""


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 10.0,
        half_open_successes: int = 3,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ) -> None:
        """
        Args:
            failure_threshold: Number of consecutive failures required to open
            recovery_timeout: Seconds to wait while OPEN before probing in HALF_OPEN
            half_open_successes: Consecutive successes in HALF_OPEN required to close
            on_state_change: Optional callback(old_state, new_state)
        """
        self._failure_threshold = int(failure_threshold)
        self._recovery_timeout = float(recovery_timeout)
        self._half_open_successes = int(half_open_successes)
        self._on_state_change = on_state_change

        self._lock = threading.RLock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_success_count = 0

    @property
    def state(self) -> CircuitState:
        with self._lock:
            # If in OPEN, check timeout and possibly transit to HALF_OPEN
            if self._state == CircuitState.OPEN:
                if (time.time() - self._last_failure_time) >= self._recovery_timeout:
                    # transition to HALF_OPEN
                    self._transition(CircuitState.HALF_OPEN)
            return self._state

    def _transition(self, new_state: CircuitState) -> None:
        old = self._state
        self._state = new_state
        # reset counters appropriately
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._half_open_success_count = 0
        elif new_state == CircuitState.OPEN:
            self._half_open_success_count = 0
            self._last_failure_time = time.time()
        elif new_state == CircuitState.HALF_OPEN:
            self._half_open_success_count = 0

        if self._on_state_change:
            try:
                self._on_state_change(old, new_state)
            except Exception:
                pass

    def allow_request(self) -> bool:
        """Return True if a request should be attempted (not short-circuited)."""
        with self._lock:
            # Evaluate the OPEN timeout while holding the same re-entrant lock.
            if self._state == CircuitState.OPEN:
                if (time.time() - self._last_failure_time) >= self._recovery_timeout:
                    self._transition(CircuitState.HALF_OPEN)
                else:
                    return False
            return True

    def record_success(self) -> None:
        """Record a successful call; may close the circuit if in HALF_OPEN."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_success_count += 1
                if self._half_open_success_count >= self._half_open_successes:
                    self._transition(CircuitState.CLOSED)
            else:
                # success in CLOSED resets failure counter
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call; may open the circuit if threshold exceeded."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                # failure in probing returns to OPEN
                self._transition(CircuitState.OPEN)
                self._last_failure_time = time.time()
                return

            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self._failure_threshold:
                self._transition(CircuitState.OPEN)

    def call(self, fn: Callable, *args, **kwargs):
        """Execute `fn` under circuit breaker protection.

        If the circuit is OPEN the call will be short-circuited by raising
        `CircuitOpenError`.
        """
        if not self.allow_request():
            raise CircuitOpenError("Circuit is OPEN; call short-circuited")

        try:
            result = fn(*args, **kwargs)
        except Exception:
            # treat any exception as a failure
            self.record_failure()
            raise
        else:
            # success
            self.record_success()
            return result
