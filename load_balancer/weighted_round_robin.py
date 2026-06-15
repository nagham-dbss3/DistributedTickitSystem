"""Weighted Round Robin strategy.

This implementation follows a smooth weighted round-robin approach: each
server has a configured weight and the algorithm maintains internal current
weights so selection is distributed proportionally over time.

The strategy skips servers whose `status` is not 'online'. Decisions are
logged via the provided logger.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from .strategy import LoadBalancingStrategy
from .metrics import ServerMetrics


class WeightedRoundRobinStrategy(LoadBalancingStrategy):
    """Weighted Round Robin strategy using smooth algorithm.

    Args:
        weights: Optional mapping from `server_id` to integer weight. If a
            server is missing from the mapping it defaults to weight 1.
        logger: Optional logger; module logger used if omitted.
    """

    def __init__(self, weights: Optional[Dict[str, int]] = None, logger: Optional[logging.Logger] = None) -> None:
        self._weights: Dict[str, int] = dict(weights or {})
        self._current: Dict[str, int] = {}
        self._logger = logger or logging.getLogger(__name__)

    def _weight_for(self, server_id: str) -> int:
        w = self._weights.get(server_id, 1)
        try:
            iw = int(w)
        except Exception:
            iw = 1
        return max(0, iw)

    def select_server(self, metrics: List[ServerMetrics], routing_key: Optional[str] = None) -> Optional[str]:
        # Filter online servers
        online = [m for m in metrics if m.status == "online"]
        if not online:
            self._logger.info("WeightedRR: no online servers available")
            return None

        # Ensure current entries exist for online servers
        total_weight = 0
        for m in online:
            sid = m.server_id
            if sid not in self._current:
                self._current[sid] = 0
            w = self._weight_for(sid)
            total_weight += w

        if total_weight <= 0:
            self._logger.info("WeightedRR: all weights are zero")
            return None

        # Increase each server's current by its weight
        for m in online:
            sid = m.server_id
            self._current[sid] += self._weight_for(sid)

        # Choose server with max current weight
        chosen = max(online, key=lambda m: self._current.get(m.server_id, 0))
        chosen_id = chosen.server_id

        # Decrease chosen current by total weight
        self._current[chosen_id] -= total_weight

        self._logger.info(
            "WeightedRR selected server %s (weight=%d) total_weight=%d",
            chosen_id,
            self._weight_for(chosen_id),
            total_weight,
        )

        return chosen_id
