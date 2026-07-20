from __future__ import annotations

import hashlib
import json
import os
import sys
import textwrap
import zipfile
from pathlib import Path

import pytest

OS_ROOT = Path(__file__).resolve().parents[2]
if str(OS_ROOT) not in sys.path:
    sys.path.insert(0, str(OS_ROOT))

from nexus_n3.plugins.install.installer import PluginInstaller
from nexus_n3.plugins.runtime.sensor_runtime import resolve_installed_sensor_class
from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.connection_status import ConnectionStatus
from nexus_n3.sensor_manager.sensor_handle import SensorBase


class SourceSensor(SensorBase):
    sensor_type = type("SensorType", (), {"local_name": "Source Sensor"})()

    def __init__(self):
        super().__init__(
            self.sensor_type,
            {
                "sensor": {"name": "Source Sensor", "adapter": "BLE"},
                "events": ["on_data", "on_error"],
                "data_streams": {"imu": {"sample_type": "IMUSample"}},
                "locations": {"supported": ["CHEST"]},
            },
        )


class BuiltinConsumerSensor(SensorBase):
    sensor_type = type("SensorType", (), {"local_name": "Builtin Consumer"})()

    def __init__(self):
        super().__init__(
            self.sensor_type,
            {
                "sensor": {"name": "Builtin Consumer", "adapter": "BLE"},
                "events": ["on_error"],
                "inputs": [{"name": "imu", "schema": "imu"}],
                "locations": {"supported": ["CHEST"]},
            },
        )
        self.consumed = []

    def consume_input(self, source_plugin_id: str, payload) -> bool:
        self.consumed.append((source_plugin_id, payload))
        return True


class NoopAdapter:
    adapter_type = "BLE"

    def close(self):
        return None


def test_phase5_routes_builtin_sensor_output_to_builtin_consumer(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("nexus_n3.sensor_manager.utils.utils.DBUS_AVAILABLE", False, raising=False)
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class",
        lambda adapter_type, ble_runtime_config=None: NoopAdapter,
    )

    source = SourceSensor()
    source.address = "source-1"
    source.set_location("CHEST")
    source.set_connection_status(ConnectionStatus.CONNECTED)

    consumer = BuiltinConsumerSensor()
    consumer.address = "consumer-1"
    consumer.set_location("CHEST")
    consumer.set_connection_status(ConnectionStatus.CONNECTED)

    manager = SensorManager()
    manager.init_sensor_manager([source, consumer])

    sample = {
        "timestamp": 1,
        "sensor_type": "Source Sensor",
        "address": "source-1",
        "location": "CHEST",
        "sampling_rate": 60,
        "quat": (1.0, 0.0, 0.0, 0.0),
        "accel": (0.1, 0.2, 0.3),
        "gyro": (1.1, 1.2, 1.3),
        "sample_type": "imu",
    }
    manager._emit_to_client("on_data", sample)

    assert len(consumer.consumed) == 1
    source_plugin_id, envelope = consumer.consumed[0]
    assert source_plugin_id == f"{SourceSensor.__module__}:{SourceSensor.__name__}"
    assert envelope.output_name == "imu"
    assert envelope.schema == "imu"
    assert envelope.payload.address == "source-1"
    assert envelope.payload.sample_type == "imu"

    manager.stop_manager()


def test_phase5_routes_builtin_sensor_output_to_host_backed_consumer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("nexus_n3.sensor_manager.utils.utils.DBUS_AVAILABLE", False, raising=False)
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class",
        lambda adapter_type, ble_runtime_config=None: NoopAdapter,
    )

    plugin_root = tmp_path / "plugins"
    bundle_path = _build_consumer_bundle(tmp_path)
    PluginInstaller(plugin_root).install_bundle(bundle_path)
    monkeypatch.setenv("NEXUS_N3_PLUGIN_ROOT", str(plugin_root))

    source = SourceSensor()
    source.address = "source-1"
    source.set_location("CHEST")
    source.set_connection_status(ConnectionStatus.CONNECTED)

    consumer_cls = resolve_installed_sensor_class("Host Consumer Sensor")
    assert consumer_cls is not None
    consumer = consumer_cls(None)
    consumer.address = "consumer-1"
    consumer.set_location("CHEST")
    consumer.set_connection_status(ConnectionStatus.CONNECTED)

    manager = SensorManager()
    routed_errors = []
    manager.register_listener("on_error", routed_errors.append)
    manager.init_sensor_manager([source, consumer])

    sample = {
        "timestamp": 1,
        "sensor_type": "Source Sensor",
        "address": "source-1",
        "location": "CHEST",
        "sampling_rate": 60,
        "quat": (1.0, 0.0, 0.0, 0.0),
        "accel": (0.1, 0.2, 0.3),
        "gyro": (1.1, 1.2, 1.3),
        "sample_type": "imu",
    }
    manager._emit_to_client("on_data", sample)

    assert routed_errors == [
        f"consume_input called from {SourceSensor.__module__}:{SourceSensor.__name__} for imu"
    ]

    manager.stop_manager()


def _build_consumer_bundle(tmp_path: Path) -> Path:
    wheel_path = _build_consumer_wheel(tmp_path / "consumer-wheel")
    manifest = {
        "schema_version": 1,
        "plugin_id": "host-consumer-sensor",
        "plugin_type": "sensor",
        "display_name": "Host Consumer Sensor",
        "version": "1.0.0",
        "sdk_version": "0.1.0",
        "min_nexus_n3_core_version": "0.0.0",
        "runtime_protocol": {"name": "nexusn3-local-jsonrpc", "version": 1},
        "entrypoint": {
            "module": "host_consumer_sensor.sensor",
            "callable": "HostConsumerSensor",
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
            "events": ["on_error"],
            "adapter": "BLE",
        },
        "inputs": [{"name": "imu", "schema": "imu"}],
        "outputs": [],
        "adapter_requirements": {"family": "BLE", "supported_backends": []},
        "permissions": {},
        "healthcheck": {
            "command": "callable",
            "module": "host_consumer_sensor.sensor",
            "callable": "healthcheck",
            "timeout_seconds": 10,
        },
    }
    spec = textwrap.dedent(
        """
        sensor:
          name: "Host Consumer Sensor"
          type: "host-consumer"
          adapter: "BLE"

        events:
          - on_error

        locations:
          supported:
            - CHEST
        """
    ).strip() + "\n"
    payloads = {
        "manifest.json": json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
        f"artifacts/{wheel_path.name}": wheel_path.read_bytes(),
        "metadata/sensor_spec.yaml": spec.encode("utf-8"),
    }
    checksums = {name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()}
    payloads["checksums.json"] = json.dumps(checksums, indent=2, sort_keys=True).encode("utf-8")
    bundle_path = tmp_path / "host-consumer-sensor-1.0.0.rsnxplugin"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in payloads.items():
            archive.writestr(name, data)
    return bundle_path


def _build_consumer_wheel(build_dir: Path) -> Path:
    build_dir.mkdir(parents=True, exist_ok=True)
    package_dir = build_dir / "host_consumer_sensor"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "sensor.py").write_text(
        textwrap.dedent(
            """
            from types import SimpleNamespace


            class HostConsumerSensor:
                sensor_type = SimpleNamespace(local_name="Host Consumer Sensor")

                def __init__(self, _sensor):
                    self.address = None
                    self.name = "Host Consumer Sensor"
                    self.location = None
                    self.adapter = "BLE"
                    self.connection_status = None
                    self.transport_client = None
                    self.attributes = {}
                    self.listeners = {"on_error": None}

                def register_listener(self, event, callback):
                    self.listeners[event] = callback

                def set_connection_status(self, status):
                    self.connection_status = status

                def _emit(self, event, payload):
                    callback = self.listeners.get(event)
                    if callback:
                        callback(payload)

                def consume_input(self, source_plugin_id, payload):
                    self._emit("on_error", f"consume_input called from {source_plugin_id} for {payload.output_name}")
                    return True


            def healthcheck():
                return True
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    files = {
        "host_consumer_sensor/__init__.py": (package_dir / "__init__.py").read_bytes(),
        "host_consumer_sensor/sensor.py": (package_dir / "sensor.py").read_bytes(),
        "host_consumer_sensor-1.0.0.dist-info/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: nexus-n3-tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        "host_consumer_sensor-1.0.0.dist-info/METADATA": (
            b"Metadata-Version: 2.1\nName: host-consumer-sensor\nVersion: 1.0.0\nSummary: Test wheel\n"
        ),
    }
    records = []
    for name, data in list(files.items()):
        digest = hashlib.sha256(data).digest()
        b64 = __import__("base64").urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        records.append(f"{name},sha256={b64},{len(data)}")
    records.append("host_consumer_sensor-1.0.0.dist-info/RECORD,,")
    files["host_consumer_sensor-1.0.0.dist-info/RECORD"] = ("\n".join(records) + "\n").encode("utf-8")

    wheel_path = build_dir / "host_consumer_sensor-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return wheel_path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        digest.update(handle.read())
    return digest.hexdigest()
