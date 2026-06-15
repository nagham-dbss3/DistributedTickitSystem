"""Adaptive feedback-controlled load balancing strategy.

This strategy uses real-time telemetry metrics to compute a dynamic health score
and selects the server with the highest health score. It adapts automatically
when telemetry changes.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from .strategy import LoadBalancingStrategy
from .metrics import ServerMetrics


class AdaptiveFeedbackStrategy(LoadBalancingStrategy):
    """Adaptive feedback strategy.

    Health score formula:
        100
        - cpu_usage
        - (active_connections * 2)
        - (response_time * 0.5)
        - (error_rate * 5)
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def _health_score(self, metrics: ServerMetrics) -> float:
        return (
            100
            - metrics.cpu_usage
            - (metrics.active_connections * 2)
            - (metrics.response_time * 0.5)
            - (metrics.error_rate * 5)
        )

    def select_server(self, metrics: List[ServerMetrics], routing_key: Optional[str] = None) -> Optional[str]:
        online_servers = [m for m in metrics if m.status == "online"]
        if not online_servers:
            self._logger.info("AdaptiveFeedback: no online servers available")
            return None

        evaluated = []
        for m in online_servers:
            score = self._health_score(m)
            evaluated.append((m, score))
            self._logger.info(
                "AdaptiveFeedback telemetry server=%s cpu=%.2f conn=%d resp=%.2f err=%.2f score=%.2f",
                m.server_id,
                m.cpu_usage,
                m.active_connections,
                m.response_time,
                m.error_rate,
                score,
            )

        chosen, chosen_score = max(evaluated, key=lambda item: item[1])
        reason = "highest dynamic health score based on real-time telemetry"
        self._logger.info(
            "AdaptiveFeedback selected server=%s score=%.2f reason=%s",
            chosen.server_id,
            chosen_score,
            reason,
        )
        return chosen.server_id