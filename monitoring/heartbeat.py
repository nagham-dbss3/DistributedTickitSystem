"""
Heartbeat monitoring subsystem for server health tracking.

Provides heartbeat collection, server status detection, and health callbacks.
Each server sends a heartbeat every 5 seconds. Servers are considered down if
no heartbeat is received within the timeout window (default 15 seconds).
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, List
import threading
import time


class Heartbeat:
    """Represents a single server heartbeat event."""
    
    def __init__(self, server_id: str, timestamp: Optional[datetime] = None):
        """
        Initialize a heartbeat.
        
        Args:
            server_id: Unique identifier for the server
            timestamp: When heartbeat was received (default: now)
        """
        self.server_id = server_id
        self.timestamp = timestamp or datetime.now()
    
    def __repr__(self) -> str:
        return f"Heartbeat({self.server_id}, {self.timestamp.strftime('%H:%M:%S')})"


class HeartbeatMonitor:
    """
    Monitors server heartbeats to track health status.
    
    - Records heartbeat events from servers
    - Detects server failures (no heartbeat within timeout)
    - Notifies listeners of server status changes (up/down)
    - Provides health status queries
    """
    
    def __init__(self, heartbeat_interval_sec: float = 5.0, 
                 timeout_sec: float = 15.0):
        """
        Initialize heartbeat monitor.
        
        Args:
            heartbeat_interval_sec: Expected interval between heartbeats (for reference)
            timeout_sec: Time without heartbeat before marking server down
        """
        self.heartbeat_interval = heartbeat_interval_sec
        self.timeout = timedelta(seconds=timeout_sec)
        
        # Track last heartbeat from each server
        self._heartbeats: Dict[str, Heartbeat] = {}
        
        # Track server status (True = up, False = down)
        self._server_status: Dict[str, bool] = {}
        
        # Status change callbacks: (server_id, is_alive) -> None
        self._status_callbacks: List[Callable[[str, bool], None]] = []
        
        # Lock for thread-safe operations
        self._lock = threading.Lock()
    
    def record_heartbeat(self, server_id: str, timestamp: Optional[datetime] = None) -> None:
        """
        Record a heartbeat from a server.
        
        Updates server status and triggers callbacks if status changed.
        
        Args:
            server_id: Server identifier
            timestamp: When heartbeat was received (default: now)
        """
        with self._lock:
            # Create heartbeat event
            heartbeat = Heartbeat(server_id, timestamp)
            self._heartbeats[server_id] = heartbeat
            
            # Determine current status based on previous status
            was_down = self._server_status.get(server_id, False) is False
            
            # Mark as up
            self._server_status[server_id] = True
            
            # Notify if status changed (from down to up)
            if was_down:
                for callback in self._status_callbacks:
                    callback(server_id, True)
    
    def check_server_health(self) -> Dict[str, bool]:
        """
        Check health of all servers based on heartbeat timeouts.
        
        Updates server status and triggers callbacks for new failures.
        
        Returns:
            Dict mapping server_id -> is_alive (True/False)
        """
        with self._lock:
            current_time = datetime.now()
            updated_status = {}
            
            for server_id, heartbeat in self._heartbeats.items():
                # Check if heartbeat is too old
                time_since_heartbeat = current_time - heartbeat.timestamp
                is_alive = time_since_heartbeat <= self.timeout
                
                # Get previous status (default True if first check)
                was_alive = self._server_status.get(server_id, True)
                
                # Update status
                self._server_status[server_id] = is_alive
                updated_status[server_id] = is_alive
                
                # Notify if status changed (from up to down)
                if was_alive and not is_alive:
                    for callback in self._status_callbacks:
                        callback(server_id, False)
            
            return updated_status
    
    def is_server_alive(self, server_id: str) -> bool:
        """
        Check if a specific server is currently alive.
        
        Args:
            server_id: Server identifier
            
        Returns:
            True if server is healthy, False otherwise
        """
        self.check_server_health()
        with self._lock:
            return self._server_status.get(server_id, False)
    
    def get_server_status(self, server_id: str) -> Dict:
        """
        Get detailed health information for a server.
        
        Args:
            server_id: Server identifier
            
        Returns:
            Dict with keys: is_alive, last_heartbeat, time_since_heartbeat_ms
        """
        with self._lock:
            heartbeat = self._heartbeats.get(server_id)
            return self._status_from_heartbeat(heartbeat)
    
    def get_all_server_status(self) -> Dict[str, Dict]:
        """
        Get health information for all servers.
        
        Returns:
            Dict mapping server_id -> health_info
        """
        with self._lock:
            self._refresh_server_health_locked()
            return {
                server_id: self._status_from_heartbeat(heartbeat)
                for server_id, heartbeat in self._heartbeats.items()
            }

    def _refresh_server_health_locked(self) -> Dict[str, bool]:
        """Refresh status while the caller already holds ``_lock``."""
        current_time = datetime.now()
        updated_status = {}

        for server_id, heartbeat in self._heartbeats.items():
            time_since_heartbeat = current_time - heartbeat.timestamp
            is_alive = time_since_heartbeat <= self.timeout
            self._server_status[server_id] = is_alive
            updated_status[server_id] = is_alive

        return updated_status

    def _status_from_heartbeat(self, heartbeat: Optional[Heartbeat]) -> Dict:
        """Build a status dictionary without acquiring the monitor lock."""
        if not heartbeat:
            return {
                "is_alive": False,
                "last_heartbeat": None,
                "time_since_heartbeat_ms": None
            }

        current_time = datetime.now()
        time_since = (current_time - heartbeat.timestamp).total_seconds() * 1000
        is_alive = time_since <= self.timeout.total_seconds() * 1000

        return {
            "is_alive": is_alive,
            "last_heartbeat": heartbeat.timestamp.strftime("%H:%M:%S"),
            "time_since_heartbeat_ms": int(time_since)
        }
    
    def register_status_callback(self, callback: Callable[[str, bool], None]) -> None:
        """
        Register a callback for server status changes.
        
        Callback signature: callback(server_id: str, is_alive: bool) -> None
        - is_alive=True: Server came online
        - is_alive=False: Server went offline
        
        Args:
            callback: Callable invoked on status changes
        """
        with self._lock:
            self._status_callbacks.append(callback)
    
    def unregister_status_callback(self, callback: Callable[[str, bool], None]) -> None:
        """
        Unregister a status change callback.
        
        Args:
            callback: Callback to remove
        """
        with self._lock:
            if callback in self._status_callbacks:
                self._status_callbacks.remove(callback)
    
    def get_alive_servers(self) -> List[str]:
        """
        Get list of currently alive servers.
        
        Returns:
            List of server_ids that are healthy
        """
        self.check_server_health()
        with self._lock:
            return [sid for sid, alive in self._server_status.items() if alive]
    
    def get_down_servers(self) -> List[str]:
        """
        Get list of currently down servers.
        
        Returns:
            List of server_ids that are unhealthy
        """
        self.check_server_health()
        with self._lock:
            return [sid for sid, alive in self._server_status.items() if not alive]
    
    def reset(self) -> None:
        """Clear all heartbeat history and reset status."""
        with self._lock:
            self._heartbeats.clear()
            self._server_status.clear()
    
    def get_summary(self) -> Dict:
        """
        Get summary of cluster health.
        
        Returns:
            Dict with alive_count, down_count, total_count
        """
        self.check_server_health()
        with self._lock:
            alive = sum(1 for s in self._server_status.values() if s)
            down = sum(1 for s in self._server_status.values() if not s)
            return {
                "alive_count": alive,
                "down_count": down,
                "total_count": len(self._server_status)
            }
