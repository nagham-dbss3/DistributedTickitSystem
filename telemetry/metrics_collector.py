"""Telemetry collector for server resource and request metrics.

This module defines a simple collector that aggregates CPU usage, active
connections, response time, error rate, and request count for a set of
server metrics.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Callable, Dict, Iterable, List, Optional

from load_balancer.metrics import ServerMetrics


@dataclass
class TelemetrySummary:
    average_cpu: float
    total_connections: int
    average_response_time: float
    average_error_rate: float
    total_request_count: int
    server_count: int


@dataclass
class TelemetryEvent:
    event_type: str
    message: str
    timestamp: str
    server_id: str = ""
    latency_ms: float = 0.0
    circuit_state: str = ""


class MetricsCollector:
    """Collects and summarizes telemetry from server metrics."""

    def __init__(self, logger: Optional[Callable[[str], None]] = None) -> None:
        self._logger = logger or (lambda msg: None)
        self._metrics: Dict[str, ServerMetrics] = {}
        self._events: List[TelemetryEvent] = []

    def ingest(self, metric: ServerMetrics) -> None:
        """Ingest a new ServerMetrics sample."""
        self._metrics[metric.server_id] = metric
        self._logger(f"MetricsCollector: ingested metrics for {metric.server_id}")

    def ingest_batch(self, metrics: Iterable[ServerMetrics]) -> None:
        """Ingest a batch of server metrics."""
        for metric in metrics:
            self.ingest(metric)

    def get_latest(self, server_id: str) -> Optional[ServerMetrics]:
        """Get the latest metrics for a given server."""
        return self._metrics.get(server_id)

    def summary(self) -> TelemetrySummary:
        """Return a summary of the currently ingested metrics."""
        values = list(self._metrics.values())
        server_count = len(values)
        if server_count == 0:
            return TelemetrySummary(
                average_cpu=0.0,
                total_connections=0,
                average_response_time=0.0,
                average_error_rate=0.0,
                total_request_count=0,
                server_count=0,
            )

        avg_cpu = mean([m.cpu_usage for m in values])
        avg_response = mean([m.response_time for m in values])
        avg_error = mean([m.error_rate for m in values])
        total_conns = sum(m.active_connections for m in values)
        total_requests = sum(m.request_count for m in values)

        return TelemetrySummary(
            average_cpu=round(avg_cpu, 2),
            total_connections=total_conns,
            average_response_time=round(avg_response, 2),
            average_error_rate=round(avg_error, 4),
            total_request_count=total_requests,
            server_count=server_count,
        )

    def record_event(
        self,
        event_type: str,
        message: str,
        server_id: str = "",
        latency_ms: float = 0.0,
        circuit_state: str = "",
    ) -> TelemetryEvent:
        """Record a routing, failure, latency, or circuit-breaker event."""
        event = TelemetryEvent(
            event_type=event_type,
            message=message,
            timestamp=datetime.utcnow().isoformat(),
            server_id=server_id,
            latency_ms=round(float(latency_ms), 2),
            circuit_state=circuit_state,
        )
        self._events.append(event)
        self._logger(f"TelemetryCollector: {event_type} {message}")
        return event

    def list_events(self, limit: Optional[int] = None) -> List[TelemetryEvent]:
        if limit is None:
            return list(self._events)
        return self._events[-int(limit):]

    def clear(self) -> None:
        """Clear all stored metrics."""
        self._metrics.clear()
        self._events.clear()


__all__ = ["MetricsCollector", "TelemetryEvent", "TelemetrySummary"]
