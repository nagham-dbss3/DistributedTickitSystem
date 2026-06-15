"""Consistent hashing strategy for load balancing.

Uses a hash ring to map routing keys (user_id, session_id, seat_id) to servers.
When servers are added/removed the ring minimizes remapping.

Logs routing key, hash value and selected server.
"""
from __future__ import annotations

import hashlib
import logging
from bisect import bisect_left
from typing import List, Optional, Tuple

from .strategy import LoadBalancingStrategy
from .metrics import ServerMetrics


class ConsistentHashingStrategy(LoadBalancingStrategy):
    """Consistent hashing implementation with virtual nodes.

    Args:
        vnode_count: Number of virtual nodes (replicas) per server.
        logger: Optional logger.
    """

    def __init__(self, vnode_count: int = 100, logger: Optional[logging.Logger] = None) -> None:
        self.vnode_count = max(1, int(vnode_count))
        self._logger = logger or logging.getLogger(__name__)
        # ring is a sorted list of (hash, server_id)
        self._ring: List[Tuple[int, str]] = []

    def _hash(self, key: str) -> int:
        # Use MD5 and convert to integer
        h = hashlib.md5(key.encode("utf-8")).hexdigest()
        return int(h, 16)

    def _build_ring(self, metrics: List[ServerMetrics]) -> None:
        """Build the ring from online servers only."""
        ring: List[Tuple[int, str]] = []
        for m in metrics:
            if m.status != "online":
                continue
            sid = m.server_id
            for v in range(self.vnode_count):
                vnode_key = f"{sid}-vn-{v}"
                ring.append((self._hash(vnode_key), sid))
        ring.sort(key=lambda t: t[0])
        self._ring = ring

    def select_server(self, metrics: List[ServerMetrics], routing_key: Optional[str] = None) -> Optional[str]:
        """Select server for the given routing key.

        If `routing_key` is None, falls back to selecting the first online server.
        """
        # Build ring fresh from the provided metrics (online servers only)
        self._build_ring(metrics)

        if not self._ring:
            self._logger.info("ConsistentHashing: no online servers available")
            return None

        if not routing_key:
            # fallback: pick the server at first ring position
            chosen = self._ring[0][1]
            self._logger.info("ConsistentHashing fallback selected server=%s (no key provided)", chosen)
            return chosen

        # hash the routing key
        key_hash = self._hash(routing_key)

        # find first vnode with hash >= key_hash
        hashes = [h for h, _ in self._ring]
        idx = bisect_left(hashes, key_hash)
        if idx == len(self._ring):
            idx = 0

        chosen_server = self._ring[idx][1]
        self._logger.info(
            "ConsistentHashing routed key=%s hash=%s selected=%s",
            routing_key,
            hex(key_hash),
            chosen_server,
        )
        return chosen_server
