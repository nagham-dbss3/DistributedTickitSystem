"""Server health checker based on heartbeat freshness."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import threading
import time
from typing import Callable, Dict, List, Optional


class ServerState(str, Enum):
    """Health states used by the monitoring subsystem."""

    ONLINE = "ONLINE"
    DEGRADED = "DEGRADED"
    OFFLINE = "OFFLINE"

    @property
    def metric_status(self) -> str:
        """Return the lowercase status used by load balancer metrics."""
        return self.value.lower()


@dataclass(frozen=True)
class ServerHealth:
    """Snapshot of one server's health."""

    server_id: str
    state: ServerState
    last_heartbeat_age_sec: Optional[float]

    @property
    def metric_status(self) -> str:
        return self.state.metric_status


StatusCallback = Callable[[str, ServerState], None]


class HealthChecker:
    """Tracks server state from heartbeat freshness.

    States are derived from the age of the latest heartbeat:
        ONLINE: heartbeat age <= degraded_after_sec
        DEGRADED: heartbeat age <= offline_after_sec
        OFFLINE: no heartbeat, or heartbeat age > offline_after_sec
    """

    def __init__(
        self,
        degraded_after_sec: float = 10.0,
        offline_after_sec: float = 30.0,
        check_interval_sec: float = 1.0,
        auto_start: bool = False,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        if degraded_after_sec < 0:
            raise ValueError("degraded_after_sec must be non-negative")
        if offline_after_sec <= degraded_after_sec:
            raise ValueError("offline_after_sec must be greater than degraded_after_sec")
        if check_interval_sec <= 0:
            raise ValueError("check_interval_sec must be greater than zero")

        self.degraded_after_sec = float(degraded_after_sec)
        self.offline_after_sec = float(offline_after_sec)
        self.check_interval_sec = float(check_interval_sec)
        self._time_fn = time_fn

        self._last_heartbeat: Dict[str, float] = {}
        self._states: Dict[str, ServerState] = {}
        self._callbacks: List[StatusCallback] = []
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        if auto_start:
            self.start()

    def register_server(self, server_id: str) -> None:
        """Register a server before its first heartbeat."""
        with self._lock:
            self._states.setdefault(server_id, ServerState.OFFLINE)

    def record_heartbeat(self, server_id: str) -> ServerState:
        """Record a heartbeat and return the server's updated state."""
        callbacks: List[StatusCallback] = []
        with self._lock:
            self._last_heartbeat[server_id] = self._time_fn()
            previous = self._states.get(server_id)
            self._states[server_id] = ServerState.ONLINE
            if previous != ServerState.ONLINE:
                callbacks = list(self._callbacks)

        self._notify(callbacks, server_id, ServerState.ONLINE)
        return ServerState.ONLINE

    def update_states(self) -> Dict[str, ServerState]:
        """Refresh and return all known server states."""
        notifications: List[tuple[str, ServerState]] = []
        now = self._time_fn()

        with self._lock:
            known_servers = set(self._states) | set(self._last_heartbeat)
            for server_id in known_servers:
                new_state = self._state_for(server_id, now)
                previous = self._states.get(server_id)
                self._states[server_id] = new_state
                if previous != new_state:
                    notifications.append((server_id, new_state))

            callbacks = list(self._callbacks)
            states = dict(self._states)

        for server_id, state in notifications:
            self._notify(callbacks, server_id, state)

        return states

    def get_state(self, server_id: str) -> ServerState:
        """Return a server's current state."""
        self.update_states()
        with self._lock:
            return self._states.get(server_id, ServerState.OFFLINE)

    def get_health(self, server_id: str) -> ServerHealth:
        """Return a detailed health snapshot for one server."""
        self.update_states()
        with self._lock:
            heartbeat_at = self._last_heartbeat.get(server_id)
            age = None if heartbeat_at is None else max(0.0, self._time_fn() - heartbeat_at)
            return ServerHealth(
                server_id=server_id,
                state=self._states.get(server_id, ServerState.OFFLINE),
                last_heartbeat_age_sec=age,
            )

    def get_all_health(self) -> Dict[str, ServerHealth]:
        """Return health snapshots for every known server."""
        self.update_states()
        with self._lock:
            now = self._time_fn()
            return {
                server_id: ServerHealth(
                    server_id=server_id,
                    state=state,
                    last_heartbeat_age_sec=(
                        None
                        if server_id not in self._last_heartbeat
                        else max(0.0, now - self._last_heartbeat[server_id])
                    ),
                )
                for server_id, state in self._states.items()
            }

    def get_servers_by_state(self, state: ServerState) -> List[str]:
        """Return server IDs currently in the requested state."""
        self.update_states()
        with self._lock:
            return [server_id for server_id, current in self._states.items() if current == state]

    def register_callback(self, callback: StatusCallback) -> None:
        """Register a callback called as callback(server_id, state)."""
        with self._lock:
            self._callbacks.append(callback)

    def unregister_callback(self, callback: StatusCallback) -> None:
        """Remove a previously registered callback."""
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)

    def start(self) -> None:
        """Start automatic periodic state updates."""
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="HealthChecker",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        """Stop automatic periodic state updates."""
        with self._lock:
            thread = self._thread
            self._stop_event.set()

        if thread:
            thread.join(timeout)

    def reset(self) -> None:
        """Clear all tracked heartbeat and state data."""
        with self._lock:
            self._last_heartbeat.clear()
            self._states.clear()

    def _run(self) -> None:
        while not self._stop_event.wait(self.check_interval_sec):
            self.update_states()

    def _state_for(self, server_id: str, now: float) -> ServerState:
        heartbeat_at = self._last_heartbeat.get(server_id)
        if heartbeat_at is None:
            return ServerState.OFFLINE

        age = max(0.0, now - heartbeat_at)
        if age <= self.degraded_after_sec:
            return ServerState.ONLINE
        if age <= self.offline_after_sec:
            return ServerState.DEGRADED
        return ServerState.OFFLINE

    def _notify(
        self,
        callbacks: List[StatusCallback],
        server_id: str,
        state: ServerState,
    ) -> None:
        for callback in callbacks:
            callback(server_id, state)
