"""
RetryManager: configurable retry helper with exponential backoff and jitter.

Features:
- `call(fn, *args, **kwargs)` executes `fn`, retrying on specified exceptions
- Configurable `max_attempts`, `base_delay`, `backoff_factor`, `max_delay`, `jitter`
- Optional callbacks: `on_retry(attempt, exc)`, `on_giveup(exc)`
- `decorator()` returns a retrying decorator for convenience
"""
from __future__ import annotations

import random
import time
from typing import Callable, Iterable, Optional, Tuple, Type


class RetryError(Exception):
    """Raised when retries are exhausted."""


class RetryManager:
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 0.2,
        backoff_factor: float = 2.0,
        max_delay: float = 10.0,
        jitter: bool = True,
        retry_exceptions: Optional[Tuple[Type[BaseException], ...]] = (Exception,),
        on_retry: Optional[Callable[[int, BaseException, float], None]] = None,
        on_giveup: Optional[Callable[[BaseException], None]] = None,
        logger: Optional[Callable[[str], None]] = print,
    ) -> None:
        """Create a RetryManager.

        Args:
            max_attempts: total attempts including the first call (>=1)
            base_delay: initial delay in seconds before first retry
            backoff_factor: multiplier applied to delay after each failure
            max_delay: cap for delay between retries
            jitter: if True, randomize delay by ±20% around the computed delay
            retry_exceptions: tuple of exception types to retry on; defaults to all Exceptions
            on_retry: optional callback(attempt_number, exception, delay) called before next attempt
            on_giveup: optional callback(exception) called when giving up
            logger: optional callable to log retry events; defaults to print
        """
        self.max_attempts = max(1, int(max_attempts))
        self.base_delay = float(base_delay)
        self.backoff_factor = float(backoff_factor)
        self.max_delay = float(max_delay)
        self.jitter = bool(jitter)
        self.retry_exceptions = retry_exceptions or (Exception,)
        self.on_retry = on_retry
        self.on_giveup = on_giveup
        self.logger = logger

    def _compute_delay(self, attempt: int) -> float:
        # attempt starts at 1 for first retry
        delay = min(self.max_delay, self.base_delay * (self.backoff_factor ** (attempt - 1)))
        if self.jitter:
            low = max(0.0, delay * 0.8)
            high = delay * 1.2
            return random.uniform(low, high)
        return delay

    def call(self, fn: Callable, *args, **kwargs):
        """Call `fn` with retries.

        Returns the result of `fn` or raises `RetryError` after exhausting retries.
        """
        last_exc = None
        attempt = 0
        while attempt < self.max_attempts:
            try:
                return fn(*args, **kwargs)
            except self.retry_exceptions as exc:
                attempt += 1
                last_exc = exc
                if attempt >= self.max_attempts:
                    if self.on_giveup:
                        try:
                            self.on_giveup(exc)
                        except Exception:
                            pass
                    self.logger(f"RetryManager: giving up after {attempt} attempts due to {exc}")
                    raise RetryError(f"Retries exhausted after {attempt} attempts") from exc

                delay = self._compute_delay(attempt)
                self.logger(f"RetryManager: attempt {attempt} failed with {exc}; retrying in {delay:.2f}s")

                if self.on_retry:
                    try:
                        self.on_retry(attempt, exc, delay)
                    except Exception:
                        pass

                time.sleep(delay)
                continue
        # Should not reach here
        raise RetryError("Retries exhausted")

    def decorator(self, *, max_attempts: Optional[int] = None):
        """Return a decorator that wraps functions with retries.

        Optional `max_attempts` overrides the manager value for the decorated function.
        """
        def _wrap(fn: Callable):
            mgr = self
            def _wrapped(*args, **kwargs):
                if max_attempts is not None:
                    old = mgr.max_attempts
                    mgr.max_attempts = max_attempts
                    try:
                        return mgr.call(fn, *args, **kwargs)
                    finally:
                        mgr.max_attempts = old
                return mgr.call(fn, *args, **kwargs)
            return _wrapped
        return _wrap


__all__ = ["RetryManager", "RetryError"]
