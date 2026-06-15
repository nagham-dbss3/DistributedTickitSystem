"""Failover manager for server health and routing resilience.

This module provides a `FailoverManager` that tracks server health,
maintains an active server list, and chooses failover targets when servers
become unhealthy.

It is designed to integrate with the existing heartbeat and load balancer
subsystems, allowing the API gateway to route around unhealthy or offline
servers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Callable, Dict, Iterable, List, Optional

from load_balancer.metrics import ServerMetrics


@dataclass
class ServerFailoverState:
    server_id: str
    healthy: bool = True
    last_seen: float = 0.0
    fail_count: int = 0
    recover_count: int = 0
    reason: str = "ok"


class FailoverManager:
    """Manage server health status and provide failover-safe server selection."""

    def __init__(
        self,
        healthy_threshold: int = 2,
        unhealthy_threshold: int = 2,
        logger: Optional[Callable[[str], None]] = print,
    ) -> None:
        self._states: Dict[str, ServerFailoverState] = {}
        self._lock = Lock()
        self._healthy_threshold = max(1, healthy_threshold)
        self._unhealthy_threshold = max(1, unhealthy_threshold)
        self._logger = logger or (lambda msg: None)
        self._failure_callbacks: List[Callable[[str], None]] = []
        self._recovery_callbacks: List[Callable[[str], None]] = []

    def register_failure_callback(self, callback: Callable[[str], None]) -> None:
        self._failure_callbacks.append(callback)

    def register_recovery_callback(self, callback: Callable[[str], None]) -> None:
        self._recovery_callbacks.append(callback)

    def update_health(self, server_id: str, healthy: bool, timestamp: float, reason: str = "") -> None:
        """Update health state for a server and trigger failover events."""
        with self._lock:
            state = self._states.setdefault(server_id, ServerFailoverState(server_id=server_id))
            state.last_seen = timestamp
            state.reason = reason or state.reason

            if healthy:
                state.recover_count += 1
                state.fail_count = 0
                if not state.healthy and state.recover_count >= self._healthy_threshold:
                    state.healthy = True
                    state.reason = "recovered"
                    self._logger(f"FailoverManager: server {server_id} recovered")
                    for callback in list(self._recovery_callbacks):
                        callback(server_id)
            else:
                state.fail_count += 1
                state.recover_count = 0
                if state.healthy and state.fail_count >= self._unhealthy_threshold:
                    state.healthy = False
                    state.reason = reason or "unhealthy"
                    self._logger(f"FailoverManager: server {server_id} marked unhealthy")
                    for callback in list(self._failure_callbacks):
                        callback(server_id)

    def get_state(self, server_id: str) -> ServerFailoverState:
        return self._states.setdefault(server_id, ServerFailoverState(server_id=server_id))

    def get_healthy_servers(self, metrics: Iterable[ServerMetrics]) -> List[str]:
        """Return server IDs that are both healthy and considered online."""
        healthy = []
        with self._lock:
            for metric in metrics:
                state = self._states.get(metric.server_id)
                if metric.status == "online" and (state is None or state.healthy):
                    healthy.append(metric.server_id)
        return healthy

    def choose_failover_target(self, metrics: List[ServerMetrics]) -> Optional[str]:
        """Choose a backup server when the preferred candidate is unhealthy."""
        healthy_servers = self.get_healthy_servers(metrics)
        if not healthy_servers:
            return None
        return healthy_servers[0]

    def update_from_heartbeat_snapshot(self, snapshot: Dict[str, Dict[str, object]]) -> None:
        """Apply a heartbeat health snapshot to internal failover state."""
        now = time.time()
        for server_id, info in snapshot.items():
            healthy = bool(info.get("is_alive", False))
            reason = info.get("reason", "heartbeat")
            self.update_health(server_id, healthy=healthy, timestamp=now, reason=reason)


__all__ = ["FailoverManager", "ServerFailoverState"]
