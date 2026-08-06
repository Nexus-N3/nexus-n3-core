from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
import sys
from unittest.mock import AsyncMock, Mock

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nexus_n3.sensor_manager.adapter_pool import AdapterPool
from nexus_n3.sensor_manager.adapters.ble_adapter import BLEAdapter
from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.adapters.usb_camera_adapter import USBCameraAdapter
from nexus_n3.sensor_manager.adapters.wifi.config import (
    ApAddressMode,
    WifiRuntimeConfig,
)
from nexus_n3.sensor_manager.adapters.wifi.models import WifiDevice
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.connection_service import ConnectionService
from nexus_n3.sensor_manager.discovery_service import DiscoveryService
from nexus_n3.sensor_manager.sensor_controller import SensorController
from nexus_n3.sensor_manager.types.connections import ConnectionStatus


def _wifi_config() -> WifiRuntimeConfig:
    return WifiRuntimeConfig(
        enabled=True,
        backend="fake",
        ap_address_mode=ApAddressMode.NETWORKMANAGER_SHARED,
        expected_ap_cidr="10.42.0.1/24",
    )


class FakeWifiDriver:
    def __init__(self, devices):
        self.devices = list(devices)
        self.operations = []

    async def discover_connected(self, network):
        self.operations.append(("discover", network.cidr))
        return list(self.devices)

    async def connect_sensor(self, sensor, device, adapter):
        self.operations.append(("connect", device.address))
        return {"address": device.address}

    async def disconnect_sensor(self, sensor):
        self.operations.append(("disconnect", sensor.address))


@dataclass(eq=False)
class FakeSensor:
    name: str
    adapter: str
    wifi_driver: object | None = None
    address: str | None = None
    transport_client: object | None = None
    connection_status: ConnectionStatus = ConnectionStatus.DISCONNECTED

    def get_wifi_driver(self):
        return self.wifi_driver

    def set_transport_client(self, client):
        self.transport_client = client

    def set_connection_status(self, status):
        self.connection_status = status

    def _emit(self, event_name, payload):
        return None


def test_wifi_uses_normal_discover_connect_setup_and_disconnect_services(monkeypatch):
    async def scenario():
        driver = FakeWifiDriver(
            [
                WifiDevice(address="wifi-001", endpoint="192.168.60.10"),
                WifiDevice(address="wifi-002", endpoint="192.168.60.11"),
            ]
        )
        sensors = [
            FakeSensor("Test WiFi Sensor", "WIFI", driver),
            FakeSensor("Test WiFi Sensor", "WIFI", driver),
        ]
        pool = AdapterPool(
            ble_runtime_config=BLERuntimeConfig(backend="bleak"),
            wifi_runtime_config=_wifi_config(),
        )
        pool.group_sensors(sensors)
        await pool.initialize_all()

        events = []
        discovery = DiscoveryService(pool, Mock())
        discovered = await discovery.discover_all(
            sensors=sensors,
            loop=asyncio.get_running_loop(),
            register_listeners_with_sensor=lambda sensor: None,
            emit_to_client=lambda event, payload: events.append((event, payload)),
        )

        assert discovered == sensors
        assert {sensor.address for sensor in sensors} == {"wifi-001", "wifi-002"}
        assert all(sensor.transport_client is not None for sensor in sensors)
        assert events[-1] == ("on_discover", sensors)

        setup = []
        connection = ConnectionService(pool, Mock())
        connected = await connection.connect_all(
            sensors=sensors,
            set_up_sensor=lambda sensor: _record_async(setup, sensor),
            emit_to_client=lambda event, payload: events.append((event, payload)),
        )

        assert connected == sensors
        assert setup == sensors
        assert all(
            sensor.connection_status.name == ConnectionStatus.CONNECTED.name
            for sensor in sensors
        )

        disconnected = await connection.disconnect(
            sensors_to_disconnect=sensors,
            emit_to_client=lambda event, payload: events.append((event, payload)),
            disconnected_status=ConnectionStatus.DISCONNECTED,
        )

        assert set(disconnected) == {"wifi-001", "wifi-002"}
        assert all(
            sensor.connection_status.name == ConnectionStatus.DISCONNECTED.name
            for sensor in sensors
        )
        assert [operation[0] for operation in driver.operations].count("connect") == 2
        assert [operation[0] for operation in driver.operations].count("disconnect") == 2
        await pool.shutdown_all()

    monkeypatch.setattr(
        "nexus_n3.sensor_manager.connection_service.platform.system",
        lambda: "Linux",
    )
    asyncio.run(scenario())


async def _record_async(records, value):
    records.append(value)


class FakeBleAdapter:
    adapter_type = "BLE"

    def __init__(self):
        self.requested = None

    async def discover_devices(self, requested):
        self.requested = list(requested)
        return {
            "AA:BB:CC:DD:EE:01": (
                SimpleNamespace(address="AA:BB:CC:DD:EE:01"),
                SimpleNamespace(local_name="Test BLE Sensor"),
            )
        }

    @staticmethod
    def create_transport_client(address, loop=None, disconnected_callback=None):
        return SimpleNamespace(address=address)


def test_mixed_ble_and_wifi_discovery_preserves_adapter_groups():
    async def scenario():
        wifi_driver = FakeWifiDriver([WifiDevice(address="wifi-001")])
        ble_sensor = FakeSensor("Test BLE Sensor", "BLE")
        wifi_sensor = FakeSensor("Test WiFi Sensor", "WIFI", wifi_driver)
        pool = AdapterPool(
            ble_runtime_config=BLERuntimeConfig(backend="bleak"),
            wifi_runtime_config=_wifi_config(),
        )
        ble_adapter = FakeBleAdapter()
        pool.adapters["BLE"] = ble_adapter
        pool.get_or_create("WIFI")
        await pool.initialize_all()

        service = DiscoveryService(pool, Mock())
        discovered = await service.discover_all(
            sensors=[ble_sensor, wifi_sensor],
            loop=asyncio.get_running_loop(),
            register_listeners_with_sensor=lambda sensor: None,
            emit_to_client=lambda event, payload: None,
        )

        assert discovered == [ble_sensor, wifi_sensor]
        assert ble_adapter.requested == [ble_sensor]
        assert ble_sensor.address == "AA:BB:CC:DD:EE:01"
        assert wifi_sensor.address == "wifi-001"
        await pool.shutdown_all()

    asyncio.run(scenario())


def test_incomplete_discovery_does_not_start_connection():
    async def scenario():
        discovery = SimpleNamespace(discover_all=AsyncMock(return_value=[]))
        connection = SimpleNamespace(connect_all=AsyncMock())
        controller = SensorController(
            sensors_ref=lambda: [object()],
            get_connected_sensors=lambda: [],
            get_connected_sensor_by_address=lambda address: [],
            set_up_sensor=AsyncMock(),
            register_listeners_with_sensor=Mock(),
            emit_to_client=Mock(),
            loop=asyncio.get_running_loop(),
            adapter_pool=Mock(),
            discovery_service=discovery,
            connection_service=connection,
            streaming_service=Mock(),
        )

        assert await controller.handle_discover_and_connect() == []
        connection.connect_all.assert_not_awaited()

    asyncio.run(scenario())


def test_ble_disconnect_keeps_transport_client_fallback():
    async def scenario():
        transport_client = SimpleNamespace(address="AA:BB:CC:DD:EE:01")
        sensor = FakeSensor(
            "Test BLE Sensor",
            "BLE",
            address="AA:BB:CC:DD:EE:01",
            transport_client=transport_client,
            connection_status=ConnectionStatus.CONNECTED,
        )
        adapter = Mock(spec=["disconnect"])
        adapter.adapter_type = "BLE"
        adapter.disconnect = AsyncMock(return_value=True)
        pool = AdapterPool(
            ble_runtime_config=BLERuntimeConfig(backend="bleak"),
            wifi_runtime_config=_wifi_config(),
        )
        pool.adapters["BLE"] = adapter
        service = ConnectionService(pool, Mock())

        disconnected = await service.disconnect(
            sensors_to_disconnect=[sensor],
            emit_to_client=lambda event, payload: None,
            disconnected_status=ConnectionStatus.DISCONNECTED,
        )

        assert disconnected == ["AA:BB:CC:DD:EE:01"]
        adapter.disconnect.assert_awaited_once_with(transport_client)
        assert sensor.connection_status is ConnectionStatus.DISCONNECTED

    asyncio.run(scenario())


def test_builtin_ble_adapters_accept_sensor_instances_and_strings(monkeypatch):
    async def scenario():
        bleak_discover = AsyncMock(return_value={})
        monkeypatch.setattr(
            "nexus_n3.sensor_manager.adapters.ble_adapter.BleakScanner.discover",
            bleak_discover,
        )
        await BLEAdapter.discover_devices([SimpleNamespace(name="Movesense")])
        await BLEAdapter.discover_devices(["Movesense"])
        assert bleak_discover.await_count == 2

        gateway = object.__new__(GatewayBLEAdapter)
        gateway.gateway_client = SimpleNamespace(scan=Mock())
        gateway.execute = AsyncMock(return_value=[])
        await gateway.discover_devices([SimpleNamespace(name="Movella DOT")])
        assert gateway.execute.await_args.kwargs["name_prefix_filter"] == "Movella DOT"

        gateway.execute.reset_mock()
        await gateway.discover_devices(["Movella DOT"])
        assert gateway.execute.await_args.kwargs["name_prefix_filter"] == "Movella DOT"

    asyncio.run(scenario())


def test_usb_camera_discovery_accepts_sensor_instances_and_strings(monkeypatch):
    monkeypatch.setattr(Path, "glob", lambda self, pattern: [Path("/dev/video0")])
    monkeypatch.setattr(
        USBCameraAdapter,
        "_sysfs_name",
        staticmethod(lambda node: "USB Camera"),
    )
    monkeypatch.setattr(USBCameraAdapter, "_by_id_map", staticmethod(dict))
    monkeypatch.setattr(USBCameraAdapter, "_by_path_map", staticmethod(dict))

    from_sensor = asyncio.run(
        USBCameraAdapter.discover_devices([SimpleNamespace(name="Configured Camera")])
    )
    from_string = asyncio.run(
        USBCameraAdapter.discover_devices(["Configured Camera"])
    )

    assert from_sensor["/dev/video0"][1].local_name == "Configured Camera"
    assert from_string["/dev/video0"][1].local_name == "Configured Camera"
