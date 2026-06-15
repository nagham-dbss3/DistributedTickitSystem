"""Load balancer strategy interfaces.

Defines the abstract interface that all load balancing algorithms must implement.
Implementations live outside this module and are injected into the LoadBalancer.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from .metrics import ServerMetrics


class LoadBalancingStrategy(ABC):
    """Abstract interface for load balancing strategies.

    Concrete strategies must implement `select_server` and return either the
    chosen server identifier (matching `ServerMetrics.server_id`) or ``None``
    when no server can be chosen.
    """

    @abstractmethod
    def select_server(self, metrics: List[ServerMetrics], routing_key: Optional[str] = None) -> Optional[str]:
        """Select a server given the current list of server metrics.

        Args:
            metrics: List of `ServerMetrics` describing current server state.
            routing_key: Optional stable key used by strategies such as
                consistent hashing. Strategies that do not need it may ignore it.

        Returns:
            The `server_id` of the selected server, or `None` if no
            suitable server is available.
        """
        raise NotImplementedError()
