"""Synchronization module for distributed seat state management.

This module provides the foundation for maintaining consistent seat state
across multiple booking servers. Includes local state storage, replication,
and eventual consistency simulation.
"""

from .eventual_consistency import EventualConsistencySimulator
from .replicator import StateReplicator
from .seat_state import SeatState, SeatStateType
from .state_store import StateStore

__all__ = [
    "SeatState",
    "SeatStateType",
    "StateStore",
    "StateReplicator",
    "EventualConsistencySimulator",
]
