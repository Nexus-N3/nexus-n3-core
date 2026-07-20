from __future__ import annotations

import base64
import asyncio
import hashlib
import json
import os
import sys
import textwrap
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

OS_ROOT = Path(__file__).resolve().parents[2]
if str(OS_ROOT) not in sys.path:
    sys.path.insert(0, str(OS_ROOT))

from nexus_n3.core.orchestrators.subject_graph import SubjectGraph
from nexus_n3.plugins.install.installer import PluginInstaller
from nexus_n3.plugins.runtime.sensor_runtime import resolve_installed_sensor_class
from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.connection_status import ConnectionStatus


def test_phase4_subject_graph_prefers_installed_sensor_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    plugin_root = tmp_path / "plugins"
    bundle_path = _build_sensor_bundle(tmp_path)
    PluginInstaller(plugin_root).install_bundle(bundle_path)
    monkeypatch.setenv("NEXUS_N3_PLUGIN_ROOT", str(plugin_root))

    graph = SubjectGraph()
    assert graph.init_subjects(
        [
            {
                "subject_id": "subject-1",
                "sensors": [
                    {
                        "local_name": "Movella DOT",
                        "number_of": 1,
                        "locations": ["CHEST"],
                    }
                ],
            }
        ]
    )

    sensor = graph.get_subjects()[0].sensors[0]["sensor"]
    assert sensor.__class__.__module__ == "nexus_n3.plugins.runtime.sensor_runtime"
    assert sensor.name == "Movella DOT"
    assert sensor.adapter == "BLE"


def test_phase4_sensor_manager_connects_installed_sensor_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    plugin_root = tmp_path / "plugins"
    bundle_path = _build_sensor_bundle(tmp_path)
    PluginInstaller(plugin_root).install_bundle(bundle_path)
    monkeypatch.setenv("NEXUS_N3_PLUGIN_ROOT", str(plugin_root))
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class",
        lambda adapter_type, ble_runtime_config=None: FakeBLEAdapter,
    )

    graph = SubjectGraph()
    assert graph.init_subjects(
        [
            {
                "subject_id": "subject-1",
                "sensors": [
                    {
                        "local_name": "Movella DOT",
                        "number_of": 1,
                        "locations": ["CHEST"],
                    }
                ],
            }
        ]
    )
    sensors = graph.get_subjects()[0].sensors

    manager = SensorManager()
    events: dict[str, list] = {
        "discover": [],
        "connected": [],
        "battery": [],
        "button": [],
        "data": [],
        "identify": [],
        "stream_started": [],
        "stream_stopped": [],
        "errors": [],
    }
    manager.register_listener("on_discover", events["discover"].append)
    manager.register_listener("on_connected", events["connected"].append)
    manager.register_listener("on_battery", events["battery"].append)
    manager.register_listener("on_button", events["button"].append)
    manager.register_listener("on_data", events["data"].append)
    manager.register_listener("on_identify", events["identify"].append)
    manager.register_listener("on_stream_started", events["stream_started"].append)
    manager.register_listener("on_stream_stopped", events["stream_stopped"].append)
    manager.register_listener("on_error", events["errors"].append)

    manager.init_sensor_manager(sensors)
    asyncio.run_coroutine_threadsafe(
        manager.controller.handle_discover_and_connect(),
        manager.loop,
    ).result(timeout=5.0)
    _wait_for(lambda: bool(events["connected"]))

    connected_sensor = events["connected"][0][0]
    assert connected_sensor.__class__.__module__ == "nexus_n3.plugins.runtime.sensor_runtime"
    assert connected_sensor.connection_status == ConnectionStatus.CONNECTED
    assert events["discover"][0][0].address == "AA:BB:CC:DD:EE:01"

    _wait_for(lambda: bool(events["battery"]))
    battery_payload = events["battery"][0]
    assert battery_payload["address"] == "AA:BB:CC:DD:EE:01"
    assert battery_payload["battery"].battery_level == 91
    assert battery_payload["battery"].is_charging is True
    assert not events["errors"]

    manager.stop_manager()


def test_phase4_sensor_host_runtime_preserves_callback_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    plugin_root = tmp_path / "plugins"
    bundle_path = _build_sensor_bundle(tmp_path)
    PluginInstaller(plugin_root).install_bundle(bundle_path)
    monkeypatch.setenv("NEXUS_N3_PLUGIN_ROOT", str(plugin_root))

    sensor_cls = resolve_installed_sensor_class("Movella DOT")
    assert sensor_cls is not None
    sensor = sensor_cls(None)
    sensor.address = "AA:BB:CC:DD:EE:01"
    sensor.set_location("CHEST")
    sensor.set_transport_client(FakeTransportClient(sensor.address))
    sensor.set_connection_status(ConnectionStatus.CONNECTED)

    loop = asyncio.new_event_loop()
    sensor.bind_manager_runtime(loop=loop)
    adapter = FakeBLEAdapter()
    sensor._runtime_adapter = adapter
    loop_thread = threading.Thread(target=_run_loop, args=(loop,), daemon=True)
    loop_thread.start()

    events: dict[str, list] = {
        "battery": [],
        "button": [],
        "data": [],
        "errors": [],
    }
    sensor.register_listener("on_battery", events["battery"].append)
    sensor.register_listener("on_button", events["button"].append)
    sensor.register_listener("on_data", events["data"].append)
    sensor.register_listener("on_error", events["errors"].append)

    try:
        asyncio.run(sensor.setup(adapter, enable_battery=True, enable_button=True))
        _wait_for(lambda: bool(events["battery"]))

        battery_payload = events["battery"][0]
        assert battery_payload["address"] == "AA:BB:CC:DD:EE:01"
        assert battery_payload["battery"].battery_level == 91
        assert battery_payload["battery"].is_charging is True

        asyncio.run(sensor.identify(adapter))
        assert any(uuid == "control-uuid" for uuid, _payload in adapter.write_calls)

        asyncio.run(sensor.start_stream(adapter))
        _wait_for(lambda: bool(events["data"]) and bool(events["button"]))

        data_payload = events["data"][0]
        assert data_payload.address == "AA:BB:CC:DD:EE:01"
        assert data_payload.location == "CHEST"
        assert data_payload.sample_type == "imu"
        assert tuple(data_payload.quat) == (1.0, 0.0, 0.0, 0.0)

        button_payload = events["button"][0]
        assert button_payload["address"] == "AA:BB:CC:DD:EE:01"
        assert button_payload["location"] == "CHEST"
        assert button_payload["button_press"] == 5

        asyncio.run(sensor.stop_stream(adapter))
        assert ("measurement-uuid", b"stop") in adapter.write_calls
        assert not events["errors"]
    finally:
        sensor.close_host()
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2.0)
        loop.close()


class FakeTransportClient:
    def __init__(self, address: str, disconnected_callback=None):
        self.address = address
        self.is_connected = False
        self.disconnected_callback = disconnected_callback
        self.notify_callbacks: dict[str, object] = {}


@dataclass
class FakeDevice:
    address: str
    name: str


@dataclass
class FakeAdvertisementData:
    local_name: str


class FakeBLEAdapter:
    adapter_type = "BLE"

    def __init__(self):
        self.write_calls: list[tuple[str, bytes]] = []

    async def discover_devices(self, names: list[str], timeout: float = 5.0):
        return {
            "AA:BB:CC:DD:EE:01": (
                FakeDevice(address="AA:BB:CC:DD:EE:01", name="Movella DOT"),
                FakeAdvertisementData(local_name="Movella DOT"),
            )
        }

    def create_transport_client(self, address: str, loop=None, disconnected_callback=None):
        return FakeTransportClient(address, disconnected_callback=disconnected_callback)

    async def connect_to_device(self, sensor, adapter, timeout: float = 10):
        sensor.transport_client.is_connected = True
        sensor.set_connection_status(ConnectionStatus.CONNECTED)
        return True

    async def connect_all(self, devices, adapter, timeout: float = 10):
        for sensor in devices:
            await self.connect_to_device(sensor, adapter, timeout=timeout)
        return True

    async def disconnect(self, transport_client):
        transport_client.is_connected = False
        return True

    async def set_notify_callback(self, transport_client, uuid, callback_func):
        transport_client.notify_callbacks[str(uuid)] = callback_func
        return True

    async def read(self, transport_client, uuid):
        if str(uuid) == "battery-uuid":
            return bytes([91, 1])
        if str(uuid) == "control-uuid":
            return b"\x00\x00\x00IDENTIFY"
        return b""

    async def write(self, transport_client, uuid, char):
        payload = bytes(char)
        self.write_calls.append((str(uuid), payload))
        if str(uuid) == "measurement-uuid" and payload == b"start":
            data_callback = transport_client.notify_callbacks["data-uuid"]
            button_callback = transport_client.notify_callbacks["button-uuid"]
            data_callback(
                "data-uuid",
                b"eyJ0aW1lc3RhbXAiOjEsInF1YXQiOlsxLjAsMC4wLDAuMCwwLjBdLCJhY2NlbCI6WzAuMSwwLjIsMC4zXSwiZ3lybyI6WzEuMSwxLjIsMS4zXX0=",
            )
            button_callback("button-uuid", bytes([5]))
        return True


def _wait_for(predicate, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for condition")


def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _build_sensor_bundle(tmp_path: Path) -> Path:
    wheel_path = _build_sensor_wheel(tmp_path / "sensor-wheel")
    manifest = {
        "schema_version": 1,
        "plugin_id": "movella-dot-runtime",
        "plugin_type": "sensor",
        "display_name": "Movella DOT",
        "version": "1.0.0",
        "sdk_version": "0.1.0",
        "min_nexus_n3_core_version": "0.0.0",
        "runtime_protocol": {"name": "nexusn3-local-jsonrpc", "version": 1},
        "entrypoint": {
            "module": "external_runtime_sensor.sensor",
            "callable": "ExternalRuntimeSensor",
        },
        "artifacts": [
            {
                "type": "wheel",
                "path": f"artifacts/{wheel_path.name}",
                "sha256": _sha256_file(wheel_path),
            }
        ],
        "spec": {"type": "sensor_yaml", "path": "metadata/sensor_spec.yaml"},
        "capabilities": {
            "capabilities": ["identify", "button", "notify_battery"],
            "events": ["on_data", "on_button", "on_battery", "on_error"],
            "data_streams": ["imu"],
            "adapter": "BLE",
            "attributes": ["SAMPLING_RATE"],
        },
        "inputs": [],
        "outputs": [],
        "adapter_requirements": {"family": "BLE", "supported_backends": []},
        "permissions": {},
        "healthcheck": {
            "command": "callable",
            "module": "external_runtime_sensor.sensor",
            "callable": "healthcheck",
            "timeout_seconds": 10,
        },
    }
    spec = textwrap.dedent(
        """
        sensor:
          name: "Movella DOT"
          type: "movelladot"
          adapter: "BLE"

        capabilities:
          - identify
          - button
          - notify_battery

        events:
          - on_data
          - on_button
          - on_battery
          - on_disconnected
          - on_identify
          - on_error

        attributes:
          SAMPLING_RATE:
            default: 60
            supported: [60]
            unit: "Hz"

        locations:
          supported:
            - CHEST

        computations: []

        data_streams:
          imu:
            sample_type: IMUSample
        """
    ).strip() + "\n"
    payloads = {
        "manifest.json": json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
        f"artifacts/{wheel_path.name}": wheel_path.read_bytes(),
        "metadata/sensor_spec.yaml": spec.encode("utf-8"),
    }
    checksums = {name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()}
    payloads["checksums.json"] = json.dumps(checksums, indent=2, sort_keys=True).encode("utf-8")
    bundle_path = tmp_path / "movella-dot-runtime-1.0.0.rsnxplugin"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in payloads.items():
            archive.writestr(name, data)
    return bundle_path


def _build_sensor_wheel(build_dir: Path) -> Path:
    build_dir.mkdir(parents=True, exist_ok=True)
    package_dir = build_dir / "external_runtime_sensor"
    dist_info = build_dir / "external_runtime_sensor-1.0.0.dist-info"
    package_dir.mkdir(parents=True, exist_ok=True)
    dist_info.mkdir(parents=True, exist_ok=True)

    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "sensor.py").write_text(
        textwrap.dedent(
            """
            import base64
            import json
            from dataclasses import dataclass
            from types import SimpleNamespace


            class BatteryStatus:
                def __init__(self, battery_level, is_charging):
                    self.battery_level = int(battery_level)
                    self.is_charging = bool(is_charging)


            @dataclass(frozen=True)
            class Sample:
                timestamp: int
                sensor_type: str
                address: str
                location: str | None
                sampling_rate: int | None
                quat: tuple[float, float, float, float] | None
                accel: tuple[float, float, float] | None
                gyro: tuple[float, float, float] | None
                sample_type = "imu"


            class ExternalRuntimeSensor:
                sensor_type = SimpleNamespace(local_name="Movella DOT")
                SAMPLE_CLASS = Sample

                def __init__(self, _sensor):
                    self.address = None
                    self.name = "Movella DOT"
                    self.location = None
                    self.adapter = "BLE"
                    self.connection_status = None
                    self.transport_client = None
                    self.capabilities = {"identify", "button", "notify_battery"}
                    self.attributes = {"SAMPLING_RATE": 60}
                    self.listeners = {
                        "on_data": None,
                        "on_button": None,
                        "on_battery": None,
                        "on_disconnected": None,
                        "on_identify": None,
                        "on_error": None,
                    }

                @classmethod
                def load_raw_spec(cls):
                    return {
                        "sensor": {"name": "Movella DOT", "adapter": "BLE"},
                        "locations": {"supported": ["CHEST"]},
                        "computations": [],
                    }

                def register_listener(self, event, callback):
                    self.listeners[event] = callback

                def set_connection_status(self, status):
                    self.connection_status = status

                def _emit(self, event, payload):
                    callback = self.listeners.get(event)
                    if callback:
                        callback(payload)

                async def setup(self, adapter, enable_battery=False, enable_button=False):
                    await adapter.set_notify_callback(self.transport_client, "data-uuid", self.on_data_packet)
                    if enable_battery:
                        await adapter.set_notify_callback(self.transport_client, "battery-uuid", self.on_battery)
                        self.on_battery("battery-uuid", await adapter.read(self.transport_client, "battery-uuid"))
                    if enable_button:
                        await adapter.set_notify_callback(self.transport_client, "button-uuid", self.on_button)

                async def start_stream(self, adapter):
                    await adapter.write(self.transport_client, "measurement-uuid", b"start")

                async def stop_stream(self, adapter):
                    await adapter.write(self.transport_client, "measurement-uuid", b"stop")

                async def identify(self, adapter):
                    payload = await adapter.read(self.transport_client, "control-uuid")
                    await adapter.write(self.transport_client, "control-uuid", b"identify" + payload[3:])

                def on_battery(self, sender, batt_bytes):
                    self._emit(
                        "on_battery",
                        {
                            "address": self.address,
                            "battery": BatteryStatus(batt_bytes[0], bool(batt_bytes[1])),
                        },
                    )

                def on_button(self, sender, event):
                    self._emit(
                        "on_button",
                        {
                            "address": self.address,
                            "location": self.location,
                            "button_press": int(event[0]),
                        },
                    )

                def on_data_packet(self, sender, packet):
                    payload = json.loads(base64.b64decode(packet).decode("utf-8"))
                    sample = Sample(
                        timestamp=int(payload["timestamp"]),
                        sensor_type=self.name,
                        address=self.address,
                        location=self.location,
                        sampling_rate=int(self.attributes.get("SAMPLING_RATE", 60)),
                        quat=tuple(payload["quat"]),
                        accel=tuple(payload["accel"]),
                        gyro=tuple(payload["gyro"]),
                    )
                    self._emit("on_data", sample)


            def healthcheck():
                return True
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    files = {
        "external_runtime_sensor/__init__.py": (package_dir / "__init__.py").read_bytes(),
        "external_runtime_sensor/sensor.py": (package_dir / "sensor.py").read_bytes(),
        "external_runtime_sensor-1.0.0.dist-info/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: nexus-n3-tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        "external_runtime_sensor-1.0.0.dist-info/METADATA": (
            b"Metadata-Version: 2.1\nName: external-runtime-sensor\nVersion: 1.0.0\nSummary: Test wheel\n"
        ),
    }
    records = []
    for name, data in list(files.items()):
        digest = hashlib.sha256(data).digest()
        b64 = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        records.append(f"{name},sha256={b64},{len(data)}")
    records.append("external_runtime_sensor-1.0.0.dist-info/RECORD,,")
    files["external_runtime_sensor-1.0.0.dist-info/RECORD"] = ("\n".join(records) + "\n").encode("utf-8")

    wheel_path = build_dir / "external_runtime_sensor-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return wheel_path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        digest.update(handle.read())
    return digest.hexdigest()
