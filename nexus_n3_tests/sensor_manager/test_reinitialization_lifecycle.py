from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace

from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.sensor_handle import SensorBase
from nexus_n3.sensor_manager.types.connections import ConnectionStatus


class _GatewayState:
    def __init__(self):
        self.connected: set[str] = set()
        self.subscriptions: set[str] = set()
        self.closed_adapters = 0


class _TransportClient:
    def __init__(self, address: str):
        self.address = address
        self.is_connected = False


class _LifecycleAdapter:
    adapter_type = "BLE"
    state: _GatewayState

    def __init__(self):
        self.transport_clients: dict[str, _TransportClient] = {}

    async def discover_devices(self, requested):
        return {
            sensor.expected_address: (
                SimpleNamespace(
                    address=sensor.expected_address,
                    name=sensor.name,
                ),
                SimpleNamespace(local_name=sensor.name, service_uuids=()),
            )
            for sensor in requested
        }

    def create_transport_client(self, address, loop=None, disconnected_callback=None):
        client = _TransportClient(address)
        self.transport_clients[address] = client
        return client

    async def connect_all(self, sensors, _adapter):
        for sensor in sensors:
            self.state.connected.add(sensor.address)
            sensor.transport_client.is_connected = True
            sensor.set_connection_status(ConnectionStatus.CONNECTED)
        return True

    async def disconnect(self, transport_client):
        address = transport_client.address
        self.state.connected.discard(address)
        self.state.subscriptions.discard(address)
        transport_client.is_connected = False
        self.transport_clients.pop(address, None)
        return True

    def close(self):
        # Closing the host-side serial/client object must not be mistaken for
        # disconnecting the remote sensors or clearing gateway subscriptions.
        self.transport_clients.clear()
        self.state.closed_adapters += 1


class _LifecycleSensor(SensorBase):
    def __init__(self, address: str):
        super().__init__(
            SimpleNamespace(local_name="Lifecycle Sensor"),
            {
                "sensor": {"adapter": "BLE"},
                "events": [
                    "on_connected",
                    "on_disconnected",
                    "on_stream_started",
                    "on_stream_stopped",
                ],
                "locations": {"supported": ["CHEST"]},
            },
        )
        self.expected_address = address
        self.host_closed = False
        self.streaming = False
        self._manager_loop = None
        self._runtime_adapter = None
        self._adapter_callbacks = {}

    def bind_manager_runtime(self, *, loop):
        self._manager_loop = loop

    async def setup(self, adapter, **_kwargs):
        if self.address in adapter.state.subscriptions:
            raise RuntimeError(f"stale subscription for {self.address}")
        adapter.state.subscriptions.add(self.address)
        self._adapter_callbacks["measurement"] = "measurement"

    async def start_stream(self, _adapter):
        self.streaming = True

    async def stop_stream(self, _adapter):
        self.streaming = False

    def close_host(self):
        self.host_closed = True


def _submit(manager: SensorManager, message: str, timeout: float = 3.0):
    completion = Future()
    manager.loop.call_soon_threadsafe(
        manager.queue.put_nowait,
        {"message": message, "_completion": completion},
    )
    return completion.result(timeout=timeout)


def test_changed_sensor_set_reinitialization_removes_previous_runtime_state(monkeypatch):
    """A completed session can be replaced without restarting Core or the gateway."""

    gateway_state = _GatewayState()
    _LifecycleAdapter.state = gateway_state
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.platform.system",
        lambda: "Darwin",
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.connection_service.platform.system",
        lambda: "Darwin",
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class",
        lambda _adapter_type, _ble_runtime_config=None: _LifecycleAdapter,
    )

    manager = SensorManager(
        ble_runtime_config=BLERuntimeConfig(backend="bleak"),
    )

    try:
        previous = _LifecycleSensor("AA:00:00:00:00:01")
        manager.init_sensor_manager([previous])
        _submit(manager, "discover_and_connect")
        _submit(manager, "start_all")
        manager.stop_all().result(timeout=3.0)

        assert not previous.streaming
        assert gateway_state.connected == {previous.address}
        assert gateway_state.subscriptions == {previous.address}

        replacement = _LifecycleSensor("AA:00:00:00:00:01")
        added = _LifecycleSensor("AA:00:00:00:00:02")
        manager.init_sensor_manager([replacement, added])

        assert previous.host_closed
        assert previous.connection_status is ConnectionStatus.DISCONNECTED
        assert previous.transport_client is None
        assert previous._runtime_adapter is None
        assert previous._manager_loop is None
        assert previous._adapter_callbacks == {}
        assert gateway_state.connected == set()
        assert gateway_state.subscriptions == set()

        _submit(manager, "discover_and_connect")
        _submit(manager, "start_all")

        assert manager.sensors == [replacement, added]
        assert all(
            sensor.connection_status is ConnectionStatus.CONNECTED
            for sensor in manager.sensors
        )
        assert all(sensor.streaming for sensor in manager.sensors)
        assert gateway_state.connected == {
            replacement.address,
            added.address,
        }
        assert gateway_state.subscriptions == gateway_state.connected
    finally:
        manager.stop_manager()
