"""Platform-neutral support for the SensorManager Wi-Fi adapter."""

from .config import WifiRuntimeConfig
from .errors import WifiError
from .models import (
    IPv4Configuration,
    NexusWifiNetwork,
    WifiAccessPoint,
    WifiAdvertisement,
    WifiCapabilities,
    WifiCredentials,
    WifiDevice,
    WifiProvisioningCandidate,
    WifiTransportHandle,
)

__all__ = [
    "IPv4Configuration",
    "NexusWifiNetwork",
    "WifiAccessPoint",
    "WifiAdvertisement",
    "WifiCapabilities",
    "WifiCredentials",
    "WifiDevice",
    "WifiError",
    "WifiProvisioningCandidate",
    "WifiRuntimeConfig",
    "WifiTransportHandle",
]
