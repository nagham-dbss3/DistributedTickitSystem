"""Round Robin load balancing strategy.

This strategy distributes requests sequentially across the provided list of
servers, skipping servers that are not `online`. It maintains internal index
state so selections continue from the last chosen server.

It logs every routing decision via the provided logger (or module logger).
"""
from __future__ import annotations

import logging
from typing import List, Optional

from .strategy import LoadBalancingStrategy
from .metrics import ServerMetrics


class RoundRobinStrategy(LoadBalancingStrategy):
    """Simple round-robin strategy.

    Args:
        logger: Optional logger to record routing decisions. If not provided,
            the module logger will be used.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        # _rr_index stores the index (into the most-recent metrics list) of
        # the last-chosen server. Initialized to -1 so the first call picks
        # the first available server.
        self._rr_index = -1

    def select_server(self, metrics: List[ServerMetrics], routing_key: Optional[str] = None) -> Optional[str]:
        """Select the next online server using round-robin order.

        The order of servers is determined by the order of items passed in
        `metrics`. Offline servers (status != 'online') are skipped.
        """
        n = len(metrics)
        if n == 0:
            self._logger.info("RoundRobin: no servers provided")
            return None

        # Try up to n entries to find the next online server
        for offset in range(n):
            idx = (self._rr_index + 1 + offset) % n
            m = metrics[idx]
            if m.status == "online":
                self._rr_index = idx
                self._logger.info(
                    "RoundRobin selected server %s (index=%d)", m.server_id, idx
                )
                return m.server_id

        # No online servers found
        self._logger.info("RoundRobin: no online servers available")
        return None
