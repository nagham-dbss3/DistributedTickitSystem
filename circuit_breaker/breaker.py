"""
Higher-level Circuit Breaker that combines the `CircuitBreaker` state machine
with a `FailureTracker` sliding window to implement transition rules:

- OPEN if:
  - `failure_threshold_consecutive` consecutive failures occur (default 3)
  - OR failure rate over the sliding window exceeds `failure_rate_threshold` (%) (default 50%)
- HALF_OPEN after `recovery_timeout` seconds (default 5s)
- CLOSED after successful probe in HALF_OPEN

This module exposes `Breaker` as the convenience wrapper.
"""
from typing import Callable, Optional
import time

from .state_machine import CircuitBreaker, CircuitState, CircuitOpenError
from .failure_tracker import FailureTracker


class Breaker:
    def __init__(
        self,
        failure_threshold_consecutive: int = 3,
        failure_rate_threshold: float = 50.0,
        window_seconds: float = 60.0,
        recovery_timeout: float = 5.0,
        half_open_successes: int = 1,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ):
        """Create a Breaker wrapper.

        Args:
            failure_threshold_consecutive: consecutive failures to open circuit
            failure_rate_threshold: percent (0-100) to open circuit based on window
            window_seconds: sliding window for failure rate
            recovery_timeout: seconds in OPEN before HALF_OPEN probe
            half_open_successes: successes needed in HALF_OPEN to close
            on_state_change: optional callback(old_state, new_state)
        """
        self.failure_rate_threshold = float(failure_rate_threshold)
        self.tracker = FailureTracker(window_seconds=window_seconds)
        # underlying state machine also tracks consecutive failures
        self.cb = CircuitBreaker(
            failure_threshold=int(failure_threshold_consecutive),
            recovery_timeout=float(recovery_timeout),
            half_open_successes=int(half_open_successes),
            on_state_change=on_state_change,
        )

    def get_state(self) -> CircuitState:
        return self.cb.state

    def allow_request(self) -> bool:
        return self.cb.allow_request()

    def record_success(self) -> None:
        # record in tracker and inform state machine
        self.tracker.record_success()
        self.cb.record_success()

    def record_failure(self) -> None:
        # update tracker and state machine; then evaluate rate-based opening
        self.tracker.record_failure()
        self.cb.record_failure()
        # check failure percentage
        fp = self.tracker.failure_percentage()
        if fp > self.failure_rate_threshold:
            # force-open the circuit
            # use internal transition for immediacy
            try:
                self.cb._transition(CircuitState.OPEN)
            except Exception:
                # best-effort
                pass

    def failure_percentage(self) -> float:
        return self.tracker.failure_percentage()

    def get_counts(self):
        return self.tracker.get_counts()

    def call(self, fn: Callable, *args, **kwargs):
        """Execute function under breaker control.

        Raises `CircuitOpenError` if the breaker is OPEN.
        """
        if not self.allow_request():
            raise CircuitOpenError("Circuit is OPEN; call short-circuited")

        try:
            res = fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        else:
            self.record_success()
            return res


__all__ = ["Breaker"]
