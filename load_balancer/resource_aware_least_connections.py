"""Resource-Aware Least Connections strategy.

This strategy computes a load score using multiple metrics (active connections,
CPU usage and response time) and selects the server with the lowest score.
It skips servers that are not online. Decisions are logged.

Default score formula:
    score = (active_connections * w_conn) + (cpu_usage * w_cpu) + (response_time * w_resp)

Weights are configurable via the constructor.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from .strategy import LoadBalancingStrategy
from .metrics import ServerMetrics


class ResourceAwareLeastConnectionsStrategy(LoadBalancingStrategy):
    """Selects server with lowest resource-aware load score.

    Args:
        w_conn: Weight applied to `active_connections` (default 0.5).
        w_cpu: Weight applied to `cpu_usage` (default 0.3).
        w_resp: Weight applied to `response_time` (default 0.2).
        logger: Optional logger; module logger used if omitted.
    """

    def __init__(
        self,
        w_conn: float = 0.5,
        w_cpu: float = 0.3,
        w_resp: float = 0.2,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.w_conn = float(w_conn)
        self.w_cpu = float(w_cpu)
        self.w_resp = float(w_resp)
        self._logger = logger or logging.getLogger(__name__)

    def _compute_score(self, m: ServerMetrics) -> float:
        return (m.active_connections * self.w_conn) + (m.cpu_usage * self.w_cpu) + (m.response_time * self.w_resp)

    def select_server(self, metrics: List[ServerMetrics], routing_key: Optional[str] = None) -> Optional[str]:
        # Filter only online servers
        online = [m for m in metrics if m.status == "online"]
        if not online:
            self._logger.info("ResourceAware: no online servers available")
            return None

        # Compute scores
        scores = []
        for m in online:
            score = self._compute_score(m)
            scores.append((m, score))
            # log per-server computed score at debug level
            self._logger.debug(
                "ResourceAware score server=%s conn=%d cpu=%.2f resp=%.2f => score=%.4f",
                m.server_id,
                m.active_connections,
                m.cpu_usage,
                m.response_time,
                score,
            )

        # select server with lowest score
        chosen_m, chosen_score = min(scores, key=lambda it: it[1])
        reason = (
            "lowest load score using weighted combination (connections/cpu/response)"
        )
        self._logger.info(
            "ResourceAware selected server=%s score=%.4f reason=%s",
            chosen_m.server_id,
            chosen_score,
            reason,
        )

        return chosen_m.server_id
