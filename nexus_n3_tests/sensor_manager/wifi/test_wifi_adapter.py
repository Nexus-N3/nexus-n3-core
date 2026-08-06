from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nexus_n3.sensor_manager.adapters.wifi.backends.fake import FakeWifiBackend
from nexus_n3.sensor_manager.adapters.wifi.config import (
    ApAddressMode,
    WifiRuntimeConfig,
)
from nexus_n3.sensor_manager.adapters.wifi.errors import (
    WifiDeviceNotDiscovered,
    WifiDiscoveryResultInvalid,
    WifiNotInitialized,
    WifiSensorDriverUnavailable,
    WifiShuttingDown,
)
from nexus_n3.sensor_manager.adapters.wifi.models import (
    IPv4Configuration,
    WifiAdvertisement,
    WifiCredentials,
    WifiDevice,
    WifiTransportHandle,
)
from nexus_n3.sensor_manager.adapters.wifi_adapter import (
    WiFiAdapter,
    WifiAdapter,
)
from nexus_n3.sensor_manager.connection_status import ConnectionStatus
from nexus_n3.sensor_manager.utils.utils import (
    match_devices,
    validate_matched_devices,
)


class FakeWifiSensorDriver:
    def __init__(self, sensor_name: str, devices: list[WifiDevice]):
        self.sensor_name = sensor_name
        self.devices = list(devices)
        self.operations: list[object] = []

    async def discover_connected(self, network):
        self.operations.append(("discover_connected", network.cidr))
        return list(self.devices)

    async def connect_sensor(self, sensor, device, adapter):
        self.operations.append(("connect_sensor", device.address))
        return {"address": device.address}

    async def disconnect_sensor(self, sensor):
        self.operations.append(("disconnect_sensor", sensor.address))


class InvalidWifiSensorDriver(FakeWifiSensorDriver):
    async def discover_connected(self, network):
        return [object()]


@dataclass
class FakeSensor:
    name: str
    wifi_driver: object
    address: str | None = None
    transport_client: object | None = None
    connection_status: ConnectionStatus = ConnectionStatus.DISCONNECTED

    def get_wifi_driver(self):
        return self.wifi_driver

    def set_transport_client(self, client):
        self.transport_client = client

    def set_connection_status(self, status):
        self.connection_status = status


def _config(**overrides) -> WifiRuntimeConfig:
    values = {
        "enabled": True,
        "backend": "fake",
        "ap_address_mode": ApAddressMode.NETWORKMANAGER_SHARED,
        "expected_ap_cidr": "10.42.0.1/24",
    }
    values.update(overrides)
    return WifiRuntimeConfig(**values)


def test_compatibility_alias_uses_preferred_adapter_class():
    assert WiFiAdapter is WifiAdapter


def test_adapter_requires_initialization():
    async def scenario():
        adapter = WifiAdapter(config=_config(), backend=FakeWifiBackend())
        with pytest.raises(WifiNotInitialized):
            await adapter.discover_devices([])
        with pytest.raises(WifiNotInitialized):
            adapter.create_transport_client("sensor-001")

    asyncio.run(scenario())


def test_initialize_and_shutdown_order():
    async def scenario():
        backend = FakeWifiBackend()
        adapter = WifiAdapter(config=_config(), backend=backend)

        await adapter.initialize()
        await adapter.initialize()
        assert backend.operations == ["initialize", "ensure_ap_active"]
        assert adapter.network == IPv4Configuration("10.42.0.1", 24)

        await adapter.shutdown()
        await adapter.shutdown()
        assert backend.operations == [
            "initialize",
            "ensure_ap_active",
            "shutdown",
        ]

        with pytest.raises(WifiShuttingDown):
            await adapter.discover_devices([])

    asyncio.run(scenario())


def test_discovery_shape_passes_existing_matcher_and_validator():
    async def scenario():
        driver = FakeWifiSensorDriver(
            "Test WiFi Sensor",
            [
                WifiDevice(address="sensor-001", endpoint="192.168.60.10"),
                WifiDevice(address="sensor-002", endpoint="192.168.60.11"),
            ],
        )
        sensors = [
            FakeSensor("Test WiFi Sensor", driver),
            FakeSensor("Test WiFi Sensor", driver),
        ]
        adapter = WifiAdapter(config=_config(), backend=FakeWifiBackend())
        await adapter.initialize()

        devices = await adapter.discover_devices(sensors)

        assert set(devices) == {"sensor-001", "sensor-002"}
        assert all(
            isinstance(value, tuple) and len(value) == 2
            for value in devices.values()
        )
        assert all(
            isinstance(value[0], WifiDevice)
            and isinstance(value[1], WifiAdvertisement)
            for value in devices.values()
        )
        assert all(
            key == device.address
            for key, (device, _advertisement) in devices.items()
        )

        names = [sensor.name for sensor in sensors]
        matched = match_devices(names, devices)
        validation = validate_matched_devices(sensors, matched)

        assert len(matched) == 2
        assert validation.valid is True
        assert validation.missing == []
        assert validation.found == 2

    asyncio.run(scenario())


def test_string_discovery_uses_registered_driver():
    async def scenario():
        driver = FakeWifiSensorDriver(
            "Test WiFi Sensor",
            [WifiDevice(address="sensor-001")],
        )
        adapter = WifiAdapter(
            config=_config(),
            backend=FakeWifiBackend(),
            drivers={"Test WiFi Sensor": driver},
        )
        await adapter.initialize()

        devices = await adapter.discover_devices(["Test WiFi Sensor"])

        assert list(devices) == ["sensor-001"]
        assert devices["sensor-001"][1].local_name == "Test WiFi Sensor"

    asyncio.run(scenario())


def test_string_discovery_requires_registered_driver():
    async def scenario():
        adapter = WifiAdapter(config=_config(), backend=FakeWifiBackend())
        await adapter.initialize()

        with pytest.raises(WifiSensorDriverUnavailable):
            await adapter.discover_devices(["Unknown Sensor"])

    asyncio.run(scenario())


def test_invalid_driver_discovery_result_is_rejected():
    async def scenario():
        driver = InvalidWifiSensorDriver("Test WiFi Sensor", [])
        sensor = FakeSensor("Test WiFi Sensor", driver)
        adapter = WifiAdapter(config=_config(), backend=FakeWifiBackend())
        await adapter.initialize()

        with pytest.raises(WifiDiscoveryResultInvalid):
            await adapter.discover_devices([sensor])

    asyncio.run(scenario())


def test_transport_handle_connect_and_disconnect_use_cached_driver():
    async def scenario():
        driver = FakeWifiSensorDriver(
            "Test WiFi Sensor",
            [WifiDevice(address="sensor-001", endpoint="192.168.60.10")],
        )
        sensor = FakeSensor("Test WiFi Sensor", driver)
        disconnected = []
        adapter = WifiAdapter(config=_config(), backend=FakeWifiBackend())
        await adapter.initialize()
        await adapter.discover_devices([sensor])

        handle = adapter.create_transport_client(
            "sensor-001",
            disconnected_callback=disconnected.append,
        )
        sensor.address = "sensor-001"
        sensor.set_transport_client(handle)

        assert isinstance(handle, WifiTransportHandle)
        assert handle.device.endpoint == "192.168.60.10"
        assert await adapter.connect_to_device(sensor, adapter)
        assert sensor.connection_status is ConnectionStatus.CONNECTED
        assert handle.is_connected is True
        assert handle.connection == {"address": "sensor-001"}

        assert await adapter.disconnect_sensor(sensor)
        assert sensor.connection_status is ConnectionStatus.DISCONNECTED
        assert handle.is_connected is False
        assert disconnected == [handle]
        assert driver.operations[-2:] == [
            ("connect_sensor", "sensor-001"),
            ("disconnect_sensor", "sensor-001"),
        ]

    asyncio.run(scenario())


def test_unknown_transport_address_is_rejected():
    async def scenario():
        adapter = WifiAdapter(config=_config(), backend=FakeWifiBackend())
        await adapter.initialize()

        with pytest.raises(WifiDeviceNotDiscovered):
            adapter.create_transport_client("missing")

    asyncio.run(scenario())


def test_shutdown_disconnects_connected_sensor_before_backend():
    async def scenario():
        driver = FakeWifiSensorDriver(
            "Test WiFi Sensor",
            [WifiDevice(address="sensor-001")],
        )
        sensor = FakeSensor("Test WiFi Sensor", driver)
        backend = FakeWifiBackend()
        adapter = WifiAdapter(config=_config(), backend=backend)
        await adapter.initialize()
        await adapter.discover_devices([sensor])
        sensor.address = "sensor-001"
        sensor.set_transport_client(adapter.create_transport_client(sensor.address))
        await adapter.connect_to_device(sensor, adapter)

        await adapter.shutdown()

        assert ("disconnect_sensor", "sensor-001") in driver.operations
        assert backend.operations[-1] == "shutdown"
        assert sensor.connection_status is ConnectionStatus.DISCONNECTED

    asyncio.run(scenario())


def test_config_validation_and_secret_redaction():
    with pytest.raises(ValueError, match="Unsupported Wi-Fi backend"):
        WifiRuntimeConfig(backend="unknown")
    with pytest.raises(ValueError, match="interface name"):
        WifiRuntimeConfig(enabled=True, backend="linux-networkmanager")
    with pytest.raises(ValueError, match="Invalid expected Wi-Fi AP CIDR"):
        WifiRuntimeConfig(expected_ap_cidr="not-a-cidr")
    with pytest.raises(ValueError, match="address mode"):
        WifiRuntimeConfig(ap_address_mode="unknown")
    with pytest.raises(ValueError, match="must be IPv4"):
        WifiRuntimeConfig(expected_ap_cidr="fd00::1/64")

    config = _config(ap_password="top-secret")
    credentials = WifiCredentials(password="top-secret")

    assert "top-secret" not in repr(config)
    assert "top-secret" not in repr(credentials)
