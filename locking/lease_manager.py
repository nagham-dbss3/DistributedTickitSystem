"""TTL-based distributed lease manager for seat reservations.

The simulator runs in one process, but this manager models the coordination
point a distributed lock service would provide: one active lease per seat,
explicit holders, renewal, release, and automatic expiration.
"""
from __future__ import annotations

from dataclasses import dataclass
import threading
import time
import uuid
from typing import Callable, Dict, List, Optional


class LeaseConflictError(Exception):
    """Raised when a seat already has an active lease."""


class LeaseNotFoundError(Exception):
    """Raised when a lease cannot be found or is no longer active."""


@dataclass(frozen=True)
class Lease:
    lease_id: str
    seat_id: int
    holder: str
    owner: str
    expires_at: float

    def remaining_seconds(self, now: Optional[float] = None) -> int:
        now = time.time() if now is None else now
        return max(0, int(round(self.expires_at - now)))


class DistributedLeaseManager:
    """Thread-safe in-memory lease authority used by the simulator."""

    def __init__(
        self,
        default_ttl_seconds: float = 30.0,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self.default_ttl_seconds = float(default_ttl_seconds)
        self._time_fn = time_fn
        self._lock = threading.RLock()
        self._leases_by_seat: Dict[int, Lease] = {}
        self._leases_by_id: Dict[str, Lease] = {}

    def acquire(
        self,
        seat_id: int,
        holder: str,
        owner: str,
        ttl_seconds: Optional[float] = None,
    ) -> Lease:
        """Acquire a seat lease or raise if another active holder owns it."""
        with self._lock:
            self._expire_locked()
            existing = self._leases_by_seat.get(seat_id)
            if existing:
                raise LeaseConflictError(
                    f"Seat {seat_id} is leased by {existing.holder} until {existing.remaining_seconds(self._time_fn())}s"
                )

            ttl = self.default_ttl_seconds if ttl_seconds is None else float(ttl_seconds)
            lease = Lease(
                lease_id=str(uuid.uuid4()),
                seat_id=seat_id,
                holder=holder,
                owner=owner,
                expires_at=self._time_fn() + ttl,
            )
            self._leases_by_seat[seat_id] = lease
            self._leases_by_id[lease.lease_id] = lease
            return lease

    def renew(self, lease_id: str, ttl_seconds: Optional[float] = None) -> Lease:
        """Extend an active lease and return the renewed lease snapshot."""
        with self._lock:
            self._expire_locked()
            lease = self._leases_by_id.get(lease_id)
            if not lease:
                raise LeaseNotFoundError(f"Lease {lease_id} is not active")

            ttl = self.default_ttl_seconds if ttl_seconds is None else float(ttl_seconds)
            renewed = Lease(
                lease_id=lease.lease_id,
                seat_id=lease.seat_id,
                holder=lease.holder,
                owner=lease.owner,
                expires_at=self._time_fn() + ttl,
            )
            self._leases_by_seat[renewed.seat_id] = renewed
            self._leases_by_id[renewed.lease_id] = renewed
            return renewed

    def release(self, lease_id: str) -> None:
        """Release an active lease if it still exists."""
        with self._lock:
            lease = self._leases_by_id.pop(lease_id, None)
            if lease and self._leases_by_seat.get(lease.seat_id) == lease:
                self._leases_by_seat.pop(lease.seat_id, None)

    def validate(self, lease_id: str, seat_id: Optional[int] = None) -> Lease:
        """Return the active lease or raise if expired/mismatched."""
        with self._lock:
            self._expire_locked()
            lease = self._leases_by_id.get(lease_id)
            if not lease:
                raise LeaseNotFoundError(f"Lease {lease_id} is not active")
            if seat_id is not None and lease.seat_id != seat_id:
                raise LeaseNotFoundError(f"Lease {lease_id} does not belong to Seat {seat_id}")
            return lease

    def get_active_lease(self, seat_id: int) -> Optional[Lease]:
        with self._lock:
            self._expire_locked()
            return self._leases_by_seat.get(seat_id)

    def expire_leases(self) -> List[Lease]:
        """Expire stale leases and return the removed lease snapshots."""
        with self._lock:
            return self._expire_locked()

    def _expire_locked(self) -> List[Lease]:
        now = self._time_fn()
        expired = [lease for lease in self._leases_by_seat.values() if lease.expires_at <= now]
        for lease in expired:
            self._leases_by_seat.pop(lease.seat_id, None)
            self._leases_by_id.pop(lease.lease_id, None)
        return expired
