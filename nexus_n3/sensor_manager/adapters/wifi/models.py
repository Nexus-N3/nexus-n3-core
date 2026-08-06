"""Platform-neutral values used by the SensorManager Wi-Fi adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class WifiCapabilities:
    """Operations supported by one platform backend and physical radio."""

    ap_hosting: bool
    scan_while_hosting: bool
    temporary_profiles: bool
    associated_client_reporting: bool
    backend_recovery: bool


@dataclass(frozen=True)
class WifiDevice:
    """A sensor identity discovered on the configured Nexus network."""

    address: str
    endpoint: Any = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WifiAdvertisement:
    """Minimal advertisement shape consumed by ``match_devices``."""

    local_name: str


@dataclass
class WifiTransportHandle:
    """Host-side handle joining a stable identity to its sensor driver."""

    address: str
    device: WifiDevice
    driver: Any
    disconnected_callback: Callable[[Any], Any] | None = None
    is_connected: bool = False
    connection: Any = None


@dataclass(frozen=True)
class WifiAccessPoint:
    """Platform-neutral result from an access-point scan."""

    id: str
    ssid: str
    bssid: str
    strength: int
    frequency_mhz: int
    secured: bool = False


@dataclass(frozen=True)
class IPv4Configuration:
    """Usable IPv4 configuration for an active Wi-Fi connection."""

    address: str
    prefix: int
    gateway: str = ""

    @property
    def cidr(self) -> str:
        return f"{self.address}/{self.prefix}"


@dataclass(frozen=True)
class WifiCredentials:
    """Credentials for a temporary Wi-Fi connection."""

    password: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class NexusWifiNetwork:
    """Network settings passed to a device-specific provisioner."""

    ssid: str
    credentials: WifiCredentials
    channel: int | None = None


@dataclass(frozen=True)
class WifiProvisioningCandidate:
    """A sensor driver claim on one scanned access point."""

    access_point: WifiAccessPoint
    confidence: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)
