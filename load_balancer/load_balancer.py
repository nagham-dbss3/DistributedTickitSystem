"""Load balancer coordinator.

This module exposes the `LoadBalancer` class which holds a strategy and
delegates selection to it. Strategies can be swapped at runtime.

No routing or algorithm implementation is included here; that will be provided
by concrete `LoadBalancingStrategy` implementations.
"""
from __future__ import annotations

from typing import List, Optional

from .strategy import LoadBalancingStrategy
from .metrics import ServerMetrics


class LoadBalancer:
    """Coordinator for load balancing.

    Responsibilities:
    - Hold a reference to the active `LoadBalancingStrategy`.
    - Allow swapping strategies at runtime via `set_strategy`.
    - Provide a simple `select` facade that delegates to the strategy.

    Note: this component purposely does not implement actual routing; it
    only encapsulates the strategy selection mechanism (Clean Architecture: use-case layer).
    """

    def __init__(self, strategy: LoadBalancingStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: LoadBalancingStrategy) -> None:
        """Switch the active load balancing strategy at runtime."""
        self._strategy = strategy

    def get_strategy(self) -> LoadBalancingStrategy:
        return self._strategy

    def select(self, metrics: List[ServerMetrics], routing_key: Optional[str] = None) -> Optional[str]:
        """Return the chosen `server_id` according to the active strategy.

        Raises a `RuntimeError` if no strategy is configured.
        """
        if self._strategy is None:
            raise RuntimeError("No load balancing strategy configured")
        return self._strategy.select_server(metrics, routing_key=routing_key)
