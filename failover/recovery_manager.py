"""Recovery manager for recovered cluster members.

The recovery manager tracks servers that come back online, allows them to
rejoin the cluster, and delivers synchronization updates so recovered servers
can catch up with the rest of the replica set.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Set

from synchronization.seat_state import SeatState


@dataclass
class ServerRecoveryState:
    server_id: str
    recovered: bool = False
    joined_cluster: bool = False
    last_recovery_time: float = 0.0
    last_reason: str = ""


class RecoveryManager:
    """Coordinate server recovery, cluster rejoin, and sync update delivery."""

    def __init__(
        self,
        logger: Optional[Callable[[str], None]] = print,
    ) -> None:
        self._logger = logger or (lambda message: None)
        self._states: Dict[str, ServerRecoveryState] = {}
        self._cluster_members: Set[str] = set()
        self._recovery_callbacks: List[Callable[[str], None]] = []
        self._join_callbacks: List[Callable[[str], None]] = []
        self._sync_callbacks: List[Callable[[str, SeatState], None]] = []

    def register_recovery_callback(self, callback: Callable[[str], None]) -> None:
        self._recovery_callbacks.append(callback)

    def register_join_callback(self, callback: Callable[[str], None]) -> None:
        self._join_callbacks.append(callback)

    def register_sync_callback(self, callback: Callable[[str, SeatState], None]) -> None:
        self._sync_callbacks.append(callback)

    def mark_recovered(self, server_id: str, reason: str = "recovery") -> None:
        state = self._states.setdefault(server_id, ServerRecoveryState(server_id=server_id))
        state.recovered = True
        state.last_recovery_time = time.time()
        state.last_reason = reason
        self._logger(f"RecoveryManager: Server {server_id} recovered ({reason})")

        for callback in list(self._recovery_callbacks):
            try:
                callback(server_id)
            except Exception:
                pass

    def rejoin_cluster(self, server_id: str, peer_ids: Optional[Iterable[str]] = None) -> None:
        state = self._states.setdefault(server_id, ServerRecoveryState(server_id=server_id))
        if not state.recovered:
            self.mark_recovered(server_id, reason="rejoin requested")

        state.joined_cluster = True
        self._cluster_members.add(server_id)
        self._logger(f"RecoveryManager: Server {server_id} rejoined cluster")

        if peer_ids:
            peer_list = ", ".join(str(peer) for peer in peer_ids)
            self._logger(f"RecoveryManager: requesting sync updates for {server_id} from peers: {peer_list}")

        for callback in list(self._join_callbacks):
            try:
                callback(server_id)
            except Exception:
                pass

    def receive_sync_updates(self, server_id: str, updates: Iterable[SeatState]) -> None:
        state = self._states.setdefault(server_id, ServerRecoveryState(server_id=server_id))
        if not state.joined_cluster:
            self.rejoin_cluster(server_id)

        count = 0
        for seat in updates:
            for callback in list(self._sync_callbacks):
                try:
                    callback(server_id, seat)
                except Exception:
                    pass
            count += 1

        self._logger(
            f"RecoveryManager: Delivered {count} synchronization updates to {server_id}"
        )

    def is_in_cluster(self, server_id: str) -> bool:
        return server_id in self._cluster_members

    def get_cluster_members(self) -> List[str]:
        return sorted(self._cluster_members)

    def get_recovery_state(self, server_id: str) -> ServerRecoveryState:
        return self._states.setdefault(server_id, ServerRecoveryState(server_id=server_id))


__all__ = ["RecoveryManager", "ServerRecoveryState"]
