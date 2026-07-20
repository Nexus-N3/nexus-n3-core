"""Legacy type definitions for sensor manager."""

from typing import NamedTuple
from enum import Enum

class ConnectionStatus(Enum):
    """Connection state for sensors."""
    CONNECTED= "Connected"
    DISCONNECTED = "Disconnected"

class DevicesValid(NamedTuple):
    """Discovery validation summary."""
    valid: bool
    missing: list[str]
    found: int
