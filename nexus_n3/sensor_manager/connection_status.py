"""Connection status definitions for managed sensor handles."""

from enum import Enum


class ConnectionStatus(Enum):
    """Connection status values for sensors."""

    CONNECTED = "Connected"
    DISCONNECTED = "Disconnected"
