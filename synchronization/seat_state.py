"""Seat state data model for distributed seat store.

Represents the state of a single seat as maintained by a booking server.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


SeatStateType = Literal["available", "locked", "reserved", "failed"]


@dataclass
class SeatState:
    """Represents the state of a single seat in a booking server's local store.

    Attributes:
        seat_id: Unique identifier for the seat.
        state: Current state of the seat (available, locked, reserved, failed).
        last_updated: ISO timestamp when this state was last changed.
        owner: Optional identifier of who locked/reserved the seat.
        version: Version number for conflict resolution (Lamport clock or similar).
    """

    seat_id: int
    state: SeatStateType
    last_updated: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    owner: str = ""
    version: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "seat_id": self.seat_id,
            "state": self.state,
            "last_updated": self.last_updated,
            "owner": self.owner,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SeatState:
        """Create from dictionary."""
        return cls(
            seat_id=data.get("seat_id", 0),
            state=data.get("state", "available"),
            last_updated=data.get("last_updated", datetime.utcnow().isoformat()),
            owner=data.get("owner", ""),
            version=data.get("version", 0),
        )
