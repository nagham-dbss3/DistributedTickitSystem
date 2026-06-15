"""
Exponential backoff delay generator.

Produces a sequence of retry delays: 1s, 2s, 4s, 8s, ...
"""
from __future__ import annotations

from typing import Iterator, Optional


def exponential_backoff_delays(
    max_attempts: int = 4,
    base_delay: float = 1.0,
    factor: float = 2.0,
    max_delay: Optional[float] = None,
) -> Iterator[float]:
    """Yield exponential retry delays.

    Args:
        max_attempts: Number of delay values to generate.
        base_delay: Delay for the first retry.
        factor: Exponential multiplier.
        max_delay: Optional cap for delay values.

    Yields:
        Delay in seconds for each retry attempt.
    """
    delay = float(base_delay)
    for _ in range(max_attempts):
        if max_delay is not None:
            yield min(delay, float(max_delay))
        else:
            yield delay
        delay *= float(factor)


class ExponentialBackoff:
    """Simple exponential backoff iterator."""

    def __init__(
        self,
        max_attempts: int = 4,
        base_delay: float = 1.0,
        factor: float = 2.0,
        max_delay: Optional[float] = None,
    ) -> None:
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.factor = factor
        self.max_delay = max_delay

    def __iter__(self) -> Iterator[float]:
        return exponential_backoff_delays(
            max_attempts=self.max_attempts,
            base_delay=self.base_delay,
            factor=self.factor,
            max_delay=self.max_delay,
        )


__all__ = ["ExponentialBackoff", "exponential_backoff_delays"]
