"""Private platform backends for the SensorManager Wi-Fi adapter."""

from .base import WifiBackend
from .fake import FakeWifiBackend

__all__ = ["FakeWifiBackend", "WifiBackend"]
