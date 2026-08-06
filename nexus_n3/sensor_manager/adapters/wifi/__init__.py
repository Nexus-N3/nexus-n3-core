"""Platform-neutral support for the SensorManager Wi-Fi adapter."""

from .config import ApAddressMode, WifiRuntimeConfig
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
    "ApAddressMode",
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
