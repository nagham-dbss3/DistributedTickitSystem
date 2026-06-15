"""
Failure tracker using a sliding time window to count successes and failures.

Features:
- record_success() / record_failure()
- sliding window by time (window_seconds)
- compute counts and failure percentage within window
- thread-safe
"""
from collections import deque
from datetime import datetime, timedelta
from threading import Lock
from typing import Deque, Tuple


class FailureTracker:
    """Tracks successes and failures in a sliding time window.

    The tracker stores timestamps for success and failure events and prunes
    events older than `window_seconds` on each query or record operation.
    """

    def __init__(self, window_seconds: float = 60.0):
        self.window = timedelta(seconds=float(window_seconds))
        # store tuples (timestamp, is_failure)
        self._events: Deque[Tuple[datetime, bool]] = deque()
        self._lock = Lock()

    def _prune(self, now: datetime) -> None:
        cutoff = now - self.window
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def record_success(self) -> None:
        now = datetime.now()
        with self._lock:
            self._events.append((now, False))
            self._prune(now)

    def record_failure(self) -> None:
        now = datetime.now()
        with self._lock:
            self._events.append((now, True))
            self._prune(now)

    def get_counts(self) -> Tuple[int, int]:
        """Return (success_count, failure_count) within the window."""
        now = datetime.now()
        with self._lock:
            self._prune(now)
            successes = sum(1 for _, is_fail in self._events if not is_fail)
            failures = sum(1 for _, is_fail in self._events if is_fail)
            return successes, failures

    def total_requests(self) -> int:
        now = datetime.now()
        with self._lock:
            self._prune(now)
            return len(self._events)

    def failure_percentage(self) -> float:
        """Return failure percentage (0.0-100.0) over window. Returns 0.0 if no requests."""
        s, f = self.get_counts()
        total = s + f
        if total == 0:
            return 0.0
        return (f / total) * 100.0

    def reset(self) -> None:
        with self._lock:
            self._events.clear()


__all__ = ["FailureTracker"]
