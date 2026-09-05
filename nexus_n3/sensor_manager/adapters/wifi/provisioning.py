"""Sensor-specific hooks consumed by the generic Wi-Fi adapter."""

from __future__ import annotations

from typing import Any, Protocol, TYPE_CHECKING

from .models import (
    IPv4Configuration,
    NexusWifiNetwork,
    WifiAccessPoint,
    WifiDevice,
    WifiProvisioningCandidate,
)

if TYPE_CHECKING:
    from nexus_n3.sensor_manager.adapters.wifi_adapter import WifiAdapter
    from nexus_n3.sensor_manager.sensor_handle import SensorBase

    from .session import ProvisioningControls


class WifiSensorDriver(Protocol):
    """Device-specific Wi-Fi behavior supplied by a sensor implementation."""

    sensor_name: str

    async def discover_connected(
        self,
        network: IPv4Configuration,
    ) -> list[WifiDevice]:
        """Discover devices already connected to the Nexus sensor network."""
        ...

    def classify_access_points(
        self,
        access_points: list[WifiAccessPoint],
    ) -> list[WifiProvisioningCandidate]:
        """Claim provisioning APs recognized by this sensor driver."""
        ...

    async def identify_candidate(
        self,
        connection: IPv4Configuration,
    ) -> WifiDevice:
        """Read a stable identity before modifying the candidate sensor."""
        ...

    async def provision(
        self,
        connection: Any,
        target: NexusWifiNetwork,
        controls: ProvisioningControls,
    ) -> WifiDevice:
        """Configure the candidate to join the target Nexus network."""
        ...

    async def connect_sensor(
        self,
        sensor: SensorBase,
        device: WifiDevice,
        adapter: WifiAdapter,
    ) -> Any:
        """Open and verify the vendor-specific sensor connection."""
        ...

    async def disconnect_sensor(self, sensor: SensorBase) -> None:
        """Close the vendor connection without changing the host AP."""
        ...
