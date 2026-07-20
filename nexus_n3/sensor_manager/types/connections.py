"""Connection status for sensor manager transports."""

from enum import Enum

class ConnectionStatus(Enum):
    """Connection state for sensors."""
    CONNECTED= "Connected"
    DISCONNECTED = "Disconnected"
