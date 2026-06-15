"""Eventual consistency simulator for distributed seat replication.

Simulates network latency and replication delays to demonstrate eventual
consistency patterns in a distributed system. When a seat state changes,
the update is scheduled for delivery after a random delay.
"""
from __future__ import annotations

import logging
import random
import threading
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

from .replicator import StateReplicator
from .seat_state import SeatState


class EventualConsistencySimulator:
    """Wraps a StateReplicator to add configurable replication delays.

    When a seat change is broadcast, each peer receives the update after a
    simulated network delay. This demonstrates eventual consistency where
    all replicas eventually converge but may be temporarily inconsistent.

    Args:
        replicator: The underlying StateReplicator instance.
        min_delay_ms: Minimum network latency in milliseconds (default 100).
        max_delay_ms: Maximum network latency in milliseconds (default 1000).
        logger: Optional logger.
    """

    def __init__(
        self,
        replicator: StateReplicator,
        min_delay_ms: int = 100,
        max_delay_ms: int = 1000,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.replicator = replicator
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms
        self._logger = logger or logging.getLogger(__name__)
        # Track pending replication events
        self._pending: Dict[int, List[threading.Timer]] = {}
        self._lock = threading.Lock()
        # Callback to notify GUI of sync lag
        self._on_sync_callback: Optional[Callable[[str, SeatState, int], None]] = None

    def set_sync_callback(
        self, callback: Callable[[str, SeatState, int], None]
    ) -> None:
        """Register callback for sync completion events.

        Callback receives (peer_id, seat_state, delay_ms).
        """
        self._on_sync_callback = callback

    def broadcast_with_delay(self, seat: SeatState) -> None:
        """Broadcast a seat update to all peers with simulated network delay.

        Each peer receives the update after a random delay between
        min_delay_ms and max_delay_ms.

        Args:
            seat: The SeatState to broadcast.
        """
        self._logger.info(
            "Seat %d booked on %s (state=%s)",
            seat.seat_id,
            self.replicator.local_store.server_id,
            seat.state,
        )

        for peer_id in self.replicator.peers:
            delay_ms = random.randint(self.min_delay_ms, self.max_delay_ms)
            delay_sec = delay_ms / 1000.0

            # Schedule delayed replication
            timer = threading.Timer(
                delay_sec,
                self._deliver_update,
                args=(peer_id, seat, delay_ms),
            )
            timer.daemon = True
            timer.start()

            # Track pending timers for cleanup
            with self._lock:
                if seat.seat_id not in self._pending:
                    self._pending[seat.seat_id] = []
                self._pending[seat.seat_id].append(timer)

            self._logger.debug(
                "Scheduled replication of Seat %d to %s after %d ms",
                seat.seat_id,
                peer_id,
                delay_ms,
            )

    def _deliver_update(self, peer_id: str, seat: SeatState, delay_ms: int) -> None:
        """Deliver a seat update to a peer after the delay has elapsed.

        Args:
            peer_id: Target peer server.
            seat: The SeatState to deliver.
            delay_ms: The simulated delay in milliseconds.
        """
        try:
            self._logger.info(
                "SYNC: Seat %d synchronized to %s after %d ms",
                seat.seat_id,
                peer_id,
                delay_ms,
            )

            # Notify callback with sync lag
            if self._on_sync_callback:
                self._on_sync_callback(peer_id, seat, delay_ms)

            # Simulate applying the update on the peer
            # In a real system, the peer would receive this via RPC/message queue
            self.replicator._logger.debug(
                "Peer %s received update for Seat %d", peer_id, seat.seat_id
            )

        except Exception as e:
            self._logger.error(
                "Error delivering update for Seat %d to %s: %s",
                seat.seat_id,
                peer_id,
                str(e),
            )
        finally:
            # Clean up timer reference
            with self._lock:
                if seat.seat_id in self._pending:
                    # Remove this timer from pending list
                    for timer in list(self._pending[seat.seat_id]):
                        if not timer.is_alive():
                            self._pending[seat.seat_id].remove(timer)

    def cancel_pending_replication(self, seat_id: int) -> None:
        """Cancel all pending replication for a seat.

        Args:
            seat_id: The seat to cancel replication for.
        """
        with self._lock:
            if seat_id in self._pending:
                for timer in self._pending[seat_id]:
                    timer.cancel()
                del self._pending[seat_id]
                self._logger.info(
                    "Cancelled pending replication for Seat %d", seat_id
                )

    def get_pending_count(self) -> int:
        """Return the number of pending replication events."""
        with self._lock:
            total = sum(len(timers) for timers in self._pending.values())
        return total

    def get_consistency_lag_ms(self) -> Dict[str, int]:
        """Return the current max lag for each peer.

        Returns:
            Dictionary mapping peer_id to max pending delay in milliseconds.
        """
        # This is a simplified view; in production you'd track actual latencies
        return {peer: self.max_delay_ms for peer in self.replicator.peers}
