"""SensorManager-facing, platform-neutral Wi-Fi adapter."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from nexus_n3.sensor_manager.connection_status import ConnectionStatus

from .wifi.backends.base import WifiBackend
from .wifi.backends.fake import FakeWifiBackend
from .wifi.config import WifiRuntimeConfig
from .wifi.errors import (
    WifiBackendUnavailable,
    WifiConnectionFailed,
    WifiDeviceNotDiscovered,
    WifiDiscoveryResultInvalid,
    WifiNotInitialized,
    WifiSensorDriverUnavailable,
    WifiShuttingDown,
)
from .wifi.models import (
    IPv4Configuration,
    WifiAdvertisement,
    WifiCapabilities,
    WifiDevice,
    WifiTransportHandle,
)


@dataclass(frozen=True)
class _DiscoveredRecord:
    device: WifiDevice
    advertisement: WifiAdvertisement
    driver: Any


class WifiAdapter:
    """Shared Wi-Fi capability used by all SensorManager Wi-Fi sensors."""

    adapter_type = "WIFI"

    def __init__(
        self,
        config: WifiRuntimeConfig | None = None,
        backend: WifiBackend | None = None,
        drivers: Mapping[str, Any] | None = None,
    ) -> None:
        self.config = config or WifiRuntimeConfig.from_env()
        self.backend = backend or self._create_backend(self.config)
        self._drivers = dict(drivers or {})
        self._discovered: dict[str, _DiscoveredRecord] = {}
        self._connected_sensors: dict[str, Any] = {}
        self._network: IPv4Configuration | None = None
        self._initialized = False
        self._shutting_down = False
        self._operation_lock = asyncio.Lock()

    @staticmethod
    def _create_backend(config: WifiRuntimeConfig) -> WifiBackend:
        if config.backend == "fake":
            return FakeWifiBackend()
        raise WifiBackendUnavailable(
            f"Wi-Fi backend {config.backend!r} is not implemented"
        )

    @property
    def capabilities(self) -> WifiCapabilities:
        return self.backend.capabilities

    @property
    def network(self) -> IPv4Configuration | None:
        return self._network

    @property
    def operation_lock(self) -> asyncio.Lock:
        return self._operation_lock

    def register_driver(self, sensor_name: str, driver: Any) -> None:
        """Register a driver for compatible string-only discovery callers."""

        name = str(sensor_name).strip()
        if not name:
            raise ValueError("Wi-Fi sensor name must not be empty")
        self._drivers[name] = driver

    async def initialize(self) -> None:
        """Initialize the backend and ensure the configured Nexus AP is active."""

        if self._shutting_down:
            raise WifiShuttingDown("Wi-Fi adapter is shutting down")
        if self._initialized:
            return
        if not self.config.enabled:
            raise WifiBackendUnavailable("Wi-Fi sensor networking is disabled")
        await self.backend.initialize()
        self._network = await self.backend.ensure_ap_active()
        self._initialized = True

    async def discover_devices(
        self,
        requested: list[str] | list[Any],
        timeout: float | None = None,
    ) -> dict[str, tuple[WifiDevice, WifiAdvertisement]]:
        """Discover connected sensors through their device-specific drivers."""

        self._ensure_ready()
        timeout_s = timeout or self.config.discovery_timeout_s
        requests = self._resolve_requests(requested)
        discovered: dict[str, tuple[WifiDevice, WifiAdvertisement]] = {}
        records: dict[str, _DiscoveredRecord] = {}

        for sensor_name, driver in requests:
            devices = await asyncio.wait_for(
                driver.discover_connected(self._network),
                timeout=timeout_s,
            )
            if not isinstance(devices, Iterable):
                raise WifiDiscoveryResultInvalid(
                    f"Wi-Fi driver for {sensor_name!r} returned a non-iterable result"
                )
            for device in devices:
                if not isinstance(device, WifiDevice) or not device.address:
                    raise WifiDiscoveryResultInvalid(
                        f"Wi-Fi driver for {sensor_name!r} returned an invalid device"
                    )
                existing = records.get(device.address)
                if existing is not None and existing.driver is not driver:
                    raise WifiDiscoveryResultInvalid(
                        f"Wi-Fi identity {device.address!r} was claimed by multiple drivers"
                    )
                advertisement = WifiAdvertisement(local_name=sensor_name)
                records[device.address] = _DiscoveredRecord(
                    device=device,
                    advertisement=advertisement,
                    driver=driver,
                )
                discovered[device.address] = (device, advertisement)

        self._discovered = records
        return discovered

    def create_transport_client(
        self,
        address: str,
        loop=None,
        disconnected_callback=None,
    ) -> WifiTransportHandle:
        """Create a transport handle from the most recent discovery cache."""

        self._ensure_ready()
        record = self._discovered.get(address)
        if record is None:
            raise WifiDeviceNotDiscovered(
                f"Wi-Fi device {address!r} was not returned by discovery"
            )
        return WifiTransportHandle(
            address=address,
            device=record.device,
            driver=record.driver,
            disconnected_callback=disconnected_callback,
        )

    async def connect_to_device(
        self,
        sensor,
        adapter=None,
        timeout: float = 10,
    ) -> bool:
        """Delegate vendor connection establishment to the sensor driver."""

        self._ensure_ready()
        handle = getattr(sensor, "transport_client", None)
        if not isinstance(handle, WifiTransportHandle):
            raise WifiConnectionFailed(
                f"Sensor {getattr(sensor, 'name', '<unknown>')!r} "
                "does not have a Wi-Fi transport handle"
            )
        try:
            connection = await asyncio.wait_for(
                handle.driver.connect_sensor(
                    sensor,
                    handle.device,
                    adapter or self,
                ),
                timeout=timeout,
            )
        except Exception as exc:
            raise WifiConnectionFailed(
                f"Failed to connect Wi-Fi sensor {handle.address!r}"
            ) from exc
        if connection is None or connection is False:
            return False
        handle.connection = None if connection is True else connection
        handle.is_connected = True
        sensor.set_connection_status(ConnectionStatus.CONNECTED)
        self._connected_sensors[handle.address] = sensor
        return True

    async def connect_all(
        self,
        sensors,
        adapter=None,
        timeout: float = 10,
    ) -> bool:
        """Connect Wi-Fi sensors sequentially through their own drivers."""

        results = []
        for sensor in sensors:
            results.append(
                await self.connect_to_device(
                    sensor,
                    adapter or self,
                    timeout=timeout,
                )
            )
        return all(results)

    async def disconnect_sensor(self, sensor) -> bool:
        """Delegate vendor disconnection without closing the shared backend."""

        self._ensure_ready()
        return await self._disconnect_sensor(sensor)

    async def _disconnect_sensor(self, sensor) -> bool:
        handle = getattr(sensor, "transport_client", None)
        if not isinstance(handle, WifiTransportHandle):
            return False
        try:
            await handle.driver.disconnect_sensor(sensor)
        except Exception as exc:
            raise WifiConnectionFailed(
                f"Failed to disconnect Wi-Fi sensor {handle.address!r}"
            ) from exc
        handle.connection = None
        handle.is_connected = False
        sensor.set_connection_status(ConnectionStatus.DISCONNECTED)
        self._connected_sensors.pop(handle.address, None)
        if handle.disconnected_callback is not None:
            callback_result = handle.disconnected_callback(handle)
            if asyncio.iscoroutine(callback_result):
                await callback_result
        return True

    async def shutdown(self) -> None:
        """Disconnect known sensors and close the shared backend."""

        if self._shutting_down:
            return
        self._shutting_down = True
        try:
            for sensor in list(self._connected_sensors.values()):
                try:
                    await self._disconnect_sensor(sensor)
                except Exception:
                    pass
            await self.backend.shutdown()
        finally:
            self._connected_sensors.clear()
            self._discovered.clear()
            self._network = None
            self._initialized = False

    def _resolve_requests(
        self,
        requested: list[str] | list[Any],
    ) -> list[tuple[str, Any]]:
        resolved: list[tuple[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for item in requested:
            if isinstance(item, str):
                sensor_name = item.strip()
                driver = self._drivers.get(sensor_name)
            else:
                sensor_name = str(getattr(item, "name", "")).strip()
                driver = self._driver_for_sensor(item)
                if sensor_name and driver is not None:
                    self._drivers.setdefault(sensor_name, driver)
            if not sensor_name or driver is None:
                raise WifiSensorDriverUnavailable(
                    f"No Wi-Fi sensor driver is available for {sensor_name or item!r}"
                )
            key = (sensor_name, id(driver))
            if key not in seen:
                resolved.append((sensor_name, driver))
                seen.add(key)
        return resolved

    @staticmethod
    def _driver_for_sensor(sensor) -> Any:
        getter = getattr(sensor, "get_wifi_driver", None)
        if callable(getter):
            return getter()
        return getattr(sensor, "wifi_driver", None)

    def _ensure_ready(self) -> None:
        if self._shutting_down:
            raise WifiShuttingDown("Wi-Fi adapter is shutting down")
        if not self._initialized:
            raise WifiNotInitialized("Wi-Fi adapter is not initialized")


# Compatibility with the existing user-edited adapter registry import.
WiFiAdapter = WifiAdapter

__all__ = ["WiFiAdapter", "WifiAdapter"]
