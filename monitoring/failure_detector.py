"""
Failure detector for heartbeat monitor.

Checks heartbeats periodically and logs/timeouts when servers go offline.
When a server is detected as down (no heartbeat within timeout), the detector
marks it offline via the monitor and emits a log line:

[FAILURE DETECTOR]
Server 2 heartbeat timeout

The detector runs in a background thread and supports registering callbacks
for failure and recovery events.
"""
from datetime import datetime
from typing import Callable, List, Optional, Set
import threading
import time

from .heartbeat import HeartbeatMonitor


class FailureDetector:
    """Monitors HeartbeatMonitor for timeouts and reports failures.

    The detector periodically calls `monitor.check_server_health()` and
    detects servers that have transitioned from alive -> down. On failure
    detection it logs a standardized message and invokes registered
    callbacks.
    """

    def __init__(self, monitor: HeartbeatMonitor, check_interval: float = 1.0,
                 logger: Optional[Callable[[str], None]] = None):
        """
        Args:
            monitor: HeartbeatMonitor instance to observe
            check_interval: Seconds between consecutive health checks
            logger: Optional callable to receive textual log messages
        """
        self.monitor = monitor
        self.check_interval = check_interval
        self._logger = logger or print

        # Callbacks for failure and recovery: Callable[[server_id], None]
        self._failure_callbacks: List[Callable[[str], None]] = []
        self._recovery_callbacks: List[Callable[[str], None]] = []

        # Internal thread control
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Track previously-known down servers to detect transitions
        self._known_down: Set[str] = set()

    def start(self) -> None:
        """Start the background monitoring thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        """Stop the background thread and wait (optionally) for it to finish."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                # Force monitor to evaluate health based on last heartbeats
                self.monitor.check_server_health()

                # Determine current down servers
                current_down = set(self.monitor.get_down_servers())

                # Newly-detected failures
                new_failures = current_down - self._known_down
                for srv in sorted(new_failures):
                    # Log standardized message
                    self._logger("[FAILURE DETECTOR]")
                    self._logger(f"Server {srv} heartbeat timeout")

                    # Invoke callbacks
                    for cb in list(self._failure_callbacks):
                        try:
                            cb(srv)
                        except Exception:
                            # Do not let callbacks kill detector loop
                            pass

                # Recoveries (servers that were down but now up)
                recovered = self._known_down - current_down
                for srv in sorted(recovered):
                    self._logger("[FAILURE DETECTOR]")
                    self._logger(f"Server {srv} recovered")
                    for cb in list(self._recovery_callbacks):
                        try:
                            cb(srv)
                        except Exception:
                            pass

                # Update known state
                self._known_down = current_down

            except Exception:
                # Swallow exceptions to keep the detector running
                pass

            # Wait before next check
            self._stop_event.wait(self.check_interval)

    def register_failure_callback(self, callback: Callable[[str], None]) -> None:
        """Register a callback invoked when a server fails."""
        self._failure_callbacks.append(callback)

    def register_recovery_callback(self, callback: Callable[[str], None]) -> None:
        """Register a callback invoked when a server recovers."""
        self._recovery_callbacks.append(callback)

    def unregister_failure_callback(self, callback: Callable[[str], None]) -> None:
        if callback in self._failure_callbacks:
            self._failure_callbacks.remove(callback)

    def unregister_recovery_callback(self, callback: Callable[[str], None]) -> None:
        if callback in self._recovery_callbacks:
            self._recovery_callbacks.remove(callback)

    def get_known_down(self) -> Set[str]:
        """Return a snapshot of servers currently considered down."""
        return set(self._known_down)
