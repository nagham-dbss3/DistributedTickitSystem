"""Distributed seat state store.

Each booking server maintains a local replica of the seat state. This module
provides the storage layer that tracks seat states on a single server instance.
Synchronization logic will be added later.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from .seat_state import SeatState, SeatStateType


class StateStore:
    """Local seat state store for a single booking server.

    This store maintains the seat state replica for this server. It does not
    perform synchronization; that responsibility lies in a higher-level
    synchronization coordinator.
    """

    def __init__(self, server_id: str, logger: Optional[logging.Logger] = None) -> None:
        """Initialize the state store.

        Args:
            server_id: Identifier for this booking server.
            logger: Optional logger for recording operations.
        """
        self.server_id = server_id
        self._logger = logger or logging.getLogger(__name__)
        self._seats: Dict[int, SeatState] = {}
        self._version_clock = 0

    def initialize_seats(self, seat_count: int) -> None:
        """Initialize the store with all available seats.

        Args:
            seat_count: Number of seats to initialize.
        """
        for seat_id in range(1, seat_count + 1):
            self._seats[seat_id] = SeatState(
                seat_id=seat_id,
                state="available",
                version=self._next_version(),
            )
        self._logger.info(
            "StateStore initialized with %d seats on server %s", seat_count, self.server_id
        )

    def _next_version(self) -> int:
        """Increment and return the next version number."""
        self._version_clock += 1
        return self._version_clock

    def get_seat(self, seat_id: int) -> Optional[SeatState]:
        """Retrieve a seat's current state.

        Args:
            seat_id: The seat to retrieve.

        Returns:
            SeatState if found, None otherwise.
        """
        return self._seats.get(seat_id)

    def update_seat(
        self,
        seat_id: int,
        new_state: SeatStateType,
        owner: str = "",
    ) -> Optional[SeatState]:
        """Update a seat's state.

        Args:
            seat_id: The seat to update.
            new_state: New state value.
            owner: Optional owner identifier.

        Returns:
            Updated SeatState, or None if seat not found.
        """
        if seat_id not in self._seats:
            self._logger.warning("Attempt to update non-existent seat %d", seat_id)
            return None

        old_state = self._seats[seat_id]
        updated = SeatState(
            seat_id=seat_id,
            state=new_state,
            last_updated=datetime.utcnow().isoformat(),
            owner=owner,
            version=self._next_version(),
        )
        self._seats[seat_id] = updated

        self._logger.debug(
            "StateStore updated seat %d: %s -> %s (owner=%s, version=%d)",
            seat_id,
            old_state.state,
            new_state,
            owner,
            updated.version,
        )
        return updated

    def list_seats(self) -> List[SeatState]:
        """Return all seats in the store.

        Returns:
            List of all SeatState objects.
        """
        return list(self._seats.values())

    def list_by_state(self, state: SeatStateType) -> List[SeatState]:
        """Return all seats with a given state.

        Args:
            state: The state to filter by.

        Returns:
            List of SeatState objects matching the state.
        """
        return [s for s in self._seats.values() if s.state == state]

    def get_available_count(self) -> int:
        """Count of available seats."""
        return len(self.list_by_state("available"))

    def get_locked_count(self) -> int:
        """Count of locked seats."""
        return len(self.list_by_state("locked"))

    def get_reserved_count(self) -> int:
        """Count of reserved seats."""
        return len(self.list_by_state("reserved"))

    def to_dict(self) -> dict:
        """Export entire store as dictionary."""
        return {
            "server_id": self.server_id,
            "version_clock": self._version_clock,
            "seats": {
                str(seat_id): seat.to_dict() for seat_id, seat in self._seats.items()
            },
        }

    def from_dict(self, data: dict) -> None:
        """Load store from dictionary (used in replication)."""
        self.server_id = data.get("server_id", self.server_id)
        self._version_clock = data.get("version_clock", 0)
        self._seats = {}
        for seat_id_str, seat_data in data.get("seats", {}).items():
            seat_id = int(seat_id_str)
            self._seats[seat_id] = SeatState.from_dict(seat_data)
        self._logger.info(
            "StateStore loaded from replica with %d seats", len(self._seats)
        )
