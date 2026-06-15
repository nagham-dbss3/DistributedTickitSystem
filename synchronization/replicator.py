"""State replication coordinator for distributed seat synchronization.

When a seat changes state on one server, the replicator broadcasts the update
to all peer servers and applies remote updates to the local store.
"""
from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional

from .seat_state import SeatState
from .state_store import StateStore


class StateReplicator:
    """Coordinates replication of seat state changes across booking servers.

    When a seat's state is updated locally, the replicator broadcasts the
    change to all peer servers. It also receives updates from peers and
    applies them to the local store.

    Args:
        local_store: The local StateStore instance.
        peers: List of peer server identifiers (e.g., ["Server 1", "Server 2"]).
        logger: Optional logger.
    """

    def __init__(
        self,
        local_store: StateStore,
        peers: List[str],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.local_store = local_store
        self.peers = [p for p in peers if p != local_store.server_id]
        self._logger = logger or logging.getLogger(__name__)
        # Callback to notify external listeners (e.g., GUI)
        self._on_replicate_callback: Optional[Callable[[str, SeatState], None]] = None

    def set_replicate_callback(
        self, callback: Callable[[str, SeatState], None]
    ) -> None:
        """Register a callback to notify when replication occurs.

        The callback receives (server_id, seat_state).
        """
        self._on_replicate_callback = callback

    def broadcast_update(self, seat: SeatState) -> None:
        """Broadcast a local seat state change to all peer servers.

        Args:
            seat: The updated SeatState to broadcast.
        """
        for peer_id in self.peers:
            self._replicate_to_peer(peer_id, seat)

    def _replicate_to_peer(self, peer_id: str, seat: SeatState) -> None:
        """Replicate a seat update to a single peer.

        Args:
            peer_id: The target peer server identifier.
            seat: The SeatState to replicate.
        """
        try:
            # Simulate sending the update to the peer
            # In a real system, this would use RPC, HTTP, or a message queue
            self._logger.info(
                "SYNC: Seat %d replicated to %s (state=%s, version=%d)",
                seat.seat_id,
                peer_id,
                seat.state,
                seat.version,
            )

            # Invoke callback if registered
            if self._on_replicate_callback:
                self._on_replicate_callback(peer_id, seat)

        except Exception as e:
            self._logger.error(
                "Failed to replicate Seat %d to %s: %s",
                seat.seat_id,
                peer_id,
                str(e),
            )

    def apply_remote_update(self, seat: SeatState, source_server: str) -> None:
        """Apply a seat state update received from a peer server.

        Uses version numbers to detect and handle conflicts. Higher version
        wins; equal versions default to the local copy.

        Args:
            seat: The SeatState received from the peer.
            source_server: The peer server identifier.
        """
        local_seat = self.local_store.get_seat(seat.seat_id)

        # If local seat doesn't exist, create it from remote
        if local_seat is None:
            self.local_store._seats[seat.seat_id] = seat
            self._logger.info(
                "SYNC: Applied remote Seat %d from %s (state=%s, version=%d)",
                seat.seat_id,
                source_server,
                seat.state,
                seat.version,
            )
            return

        # Version-based conflict resolution
        if seat.version > local_seat.version:
            # Remote is newer
            self.local_store._seats[seat.seat_id] = seat
            self._logger.info(
                "SYNC: Updated Seat %d from %s (remote v%d > local v%d)",
                seat.seat_id,
                source_server,
                seat.version,
                local_seat.version,
            )
        elif seat.version == local_seat.version:
            # Same version: keep local (first-write-wins)
            self._logger.debug(
                "SYNC: Conflicting versions for Seat %d from %s (keeping local)",
                seat.seat_id,
                source_server,
            )
        else:
            # Local is newer, ignore remote
            self._logger.debug(
                "SYNC: Ignoring older version of Seat %d from %s (local v%d > remote v%d)",
                seat.seat_id,
                source_server,
                local_seat.version,
                seat.version,
            )

    def get_replication_status(self) -> Dict[str, int]:
        """Return replication statistics.

        Returns:
            Dictionary with peer_id -> last_replicated_version mappings.
        """
        return {peer: self.local_store._version_clock for peer in self.peers}
