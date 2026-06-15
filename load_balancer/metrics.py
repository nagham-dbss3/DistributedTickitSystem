"""Server metrics model for the load balancer subsystem.

This module defines the `ServerMetrics` dataclass which represents the
observability information the Load Balancer uses to make decisions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Status = Literal["online", "offline", "degraded"]


@dataclass
class ServerMetrics:
    server_id: str
    cpu_usage: float
    active_connections: int
    response_time: float
    error_rate: float
    request_count: int
    status: Status
