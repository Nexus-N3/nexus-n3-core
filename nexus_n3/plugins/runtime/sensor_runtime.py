"""Runtime support for host-backed sensor plugins."""

from __future__ import annotations

import asyncio
import base64
import copy
import os
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from nexus_n3.sensor_manager.connection_status import ConnectionStatus
from nexus_n3.sensor_manager.sensor_handle import SensorBase

from ..common.jsonio import read_json
from ..install.config import resolve_plugin_root
from ..install.layout import PluginLayout
from .serde import deep_namespace, to_jsonable
from .transport import StdioJsonRpcTransport, PluginTransportError
from .environment import prepend_pythonpath, resolve_runtime_python

@dataclass(frozen=True)
class InstalledSensorPlugin:
    plugin_id: str
    sensor_name: str
    install_path: Path
    runtime_path: Path
    manifest: dict[str, Any]
    metadata: dict[str, Any]


class SensorHostClient:
    """Client wrapper for one sensor plugin host process."""

    def __init__(self, plugin: InstalledSensorPlugin, proxy: "InstalledSensorProxy"):
        self.plugin = plugin
        self.proxy = proxy

        runtime_python = resolve_runtime_python(plugin.runtime_path)
        core_import_root = Path(__file__).resolve().parents[3]
        env = os.environ.copy()
        prepend_pythonpath(env, core_import_root)
        
        self.transport = StdioJsonRpcTransport(
            [
                str(runtime_python),
                "-m",
                "nexus_n3.plugins.runtime.sensor_host",
                "--install-path",
                str(plugin.install_path),
            ],
            env=env,
            cwd=core_import_root,
        )
        self.transport.register_handler("adapter.read", self._handle_adapter_read)
        self.transport.register_handler("adapter.write", self._handle_adapter_write)
        self.transport.register_handler("adapter.subscribe", self._handle_adapter_subscribe)
        self.transport.register_handler("sensor.emit_event", self._handle_sensor_event)
        description = self.transport.request("describe", {})
        health = self.transport.request("healthcheck", {})

        if description.get("plugin_id") != plugin.plugin_id:
            raise PluginTransportError(
                "Plugin host loaded an unexpected plugin: "
                f"expected={plugin.plugin_id!r}, "
                f"received={description.get('plugin_id')!r}"
            )

        if not health.get("ok"):
            raise PluginTransportError(
                f"Plugin host health check failed: {health}"
            )

    def close(self) -> None:
        self.transport.close()

    def bind_sensor(self) -> None:
        self.transport.request(
            "bind_sensor",
            {
                "address": self.proxy.address,
                "location": self.proxy.location,
                "attributes": dict(self.proxy.attributes),
                "connection_status": getattr(self.proxy.connection_status, "name", None),
            },
        )

    def setup(self, *, enable_battery: bool, enable_button: bool) -> Any:
        self.bind_sensor()
        return self.transport.request(
            "setup",
            {
                "enable_battery": enable_battery,
                "enable_button": enable_button,
            },
        )

    def start_stream(self) -> Any:
        self.bind_sensor()
        return self.transport.request("start_stream", {})

    def stop_stream(self) -> Any:
        self.bind_sensor()
        return self.transport.request("stop_stream", {})

    def identify(self) -> Any:
        self.bind_sensor()
        return self.transport.request("identify", {})

    def consume_input(self, *, source_plugin_id: str, payload: Any) -> bool:
        self.bind_sensor()
        result = self.transport.request(
            "consume_input",
            {
                "source_plugin_id": source_plugin_id,
                "payload": to_jsonable(payload),
            },
        )
        return bool((result or {}).get("ok"))

    def _handle_adapter_read(self, params: dict[str, Any]) -> dict[str, Any]:
        data = self.proxy._adapter_request("read", str(params["uuid"]))
        return {"data_b64": base64.b64encode(bytes(data)).decode("ascii")}

    def _handle_adapter_write(self, params: dict[str, Any]) -> dict[str, Any]:
        payload = base64.b64decode(params["data_b64"].encode("ascii"))
        self.proxy._adapter_request("write", str(params["uuid"]), payload)
        return {"ok": True}

    def _handle_adapter_subscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        self.proxy._register_host_callback(
            callback_id=str(params["callback_id"]),
            notify_uuid=str(params["uuid"]),
            transport=self.transport,
        )
        return {"ok": True}

    def _handle_sensor_event(self, params: dict[str, Any]) -> dict[str, Any]:
        self.proxy._handle_host_event(
            str(params["event"]),
            params.get("payload"),
        )
        return {"ok": True}


class InstalledSensorProxy(SensorBase):
    """SensorBase-compatible proxy that forwards lifecycle to a sensor host."""

    _plugin: InstalledSensorPlugin | None = None
    sensor_type = SimpleNamespace(local_name="unknown")

    def __init__(self, sensor):
        if self._plugin is None:
            raise RuntimeError("installed sensor proxy missing plugin descriptor")
        super().__init__(self.sensor_type, copy.deepcopy(self._plugin.metadata))
        self.plugin_id = self._plugin.plugin_id
        self.routing_inputs = list(_routing_entries(self._plugin.manifest.get("inputs")))
        self.routing_outputs = list(_routing_entries(self._plugin.manifest.get("outputs")))
        self._plugin_client: SensorHostClient | None = None
        self._manager_loop = None
        self._adapter_callbacks: dict[str, str] = {}

    @classmethod
    def load_raw_spec(cls) -> dict:
        if cls._plugin is None:
            return {}
        return copy.deepcopy(cls._plugin.metadata)

    def bind_manager_runtime(self, *, loop) -> None:
        self._manager_loop = loop

    def set_connection_status(self, status: ConnectionStatus):
        super().set_connection_status(status)
        if self._plugin_client is not None:
            try:
                self._plugin_client.bind_sensor()
            except Exception:
                pass

    async def setup(self, adapter, enable_battery: bool = False, enable_button: bool = False):
        client = self._ensure_client()
        await asyncio.to_thread(
            client.setup,
            enable_battery=enable_battery,
            enable_button=enable_button,
        )

    async def start_stream(self, adapter):
        client = self._ensure_client()
        await asyncio.to_thread(client.start_stream)

    async def stop_stream(self, adapter):
        client = self._ensure_client()
        await asyncio.to_thread(client.stop_stream)

    async def identify(self, adapter):
        client = self._ensure_client()
        await asyncio.to_thread(client.identify)

    def consume_input(self, source_plugin_id: str, payload) -> bool:
        client = self._ensure_client()
        return client.consume_input(source_plugin_id=source_plugin_id, payload=payload)

    def close_host(self) -> None:
        if self._plugin_client is None:
            return
        self._plugin_client.close()
        self._plugin_client = None

    def _ensure_client(self) -> SensorHostClient:
        if self._plugin_client is None:
            self._plugin_client = SensorHostClient(self._plugin, self)
        return self._plugin_client

    def _adapter_request(self, method: str, uuid: str, payload: bytes | None = None) -> Any:
        adapter = getattr(self, "_runtime_adapter", None)
        if adapter is None:
            raise RuntimeError("sensor proxy adapter not bound")
        if self.transport_client is None:
            raise RuntimeError("sensor proxy transport client not bound")
        if self._manager_loop is None:
            raise RuntimeError("sensor proxy manager loop not bound")
        if method == "read":
            coro = adapter.read(self.transport_client, uuid)
        elif method == "write":
            coro = adapter.write(self.transport_client, uuid, payload)
        else:
            raise RuntimeError(f"unsupported adapter request: {method}")
        return asyncio.run_coroutine_threadsafe(coro, self._manager_loop).result(timeout=30.0)

    def _register_host_callback(self, *, callback_id: str, notify_uuid: str, transport: StdioJsonRpcTransport) -> None:
        adapter = getattr(self, "_runtime_adapter", None)
        if adapter is None:
            raise RuntimeError("sensor proxy adapter not bound")
        if self.transport_client is None:
            raise RuntimeError("sensor proxy transport client not bound")
        if self._manager_loop is None:
            raise RuntimeError("sensor proxy manager loop not bound")
        if callback_id in self._adapter_callbacks:
            return

        def _notify(sender, data):
            transport.notify(
                "adapter.notification",
                {
                    "callback_id": callback_id,
                    "sender": str(sender),
                    "data_b64": base64.b64encode(bytes(data)).decode("ascii"),
                },
            )

        self._adapter_callbacks[callback_id] = notify_uuid
        coro = adapter.set_notify_callback(self.transport_client, notify_uuid, _notify)
        asyncio.run_coroutine_threadsafe(coro, self._manager_loop).result(timeout=30.0)

    def _handle_host_event(self, event_name: str, payload: Any) -> None:
        if event_name == "on_data":
            self._emit(event_name, deep_namespace(payload) if isinstance(payload, dict) else payload)
            return
        self._emit(event_name, payload if not isinstance(payload, dict) else deep_namespace_dict(payload))


def resolve_installed_sensor_class(
    local_name: str,
    plugin_root: str | Path | None = None,
) -> type[InstalledSensorProxy] | None:
    plugin = find_installed_sensor_plugin(local_name, plugin_root=plugin_root)
    if plugin is None:
        return None
    class_name = f"{plugin.plugin_id.replace('-', '_').title().replace('_', '')}Proxy"
    attrs = {
        "_plugin": plugin,
        "sensor_type": SimpleNamespace(local_name=plugin.sensor_name),
    }
    return type(class_name, (InstalledSensorProxy,), attrs)


def find_installed_sensor_plugin(
    local_name: str,
    plugin_root: str | Path | None = None,
) -> InstalledSensorPlugin | None:
    normalized = _normalize_name(local_name)
    layout = PluginLayout(resolve_plugin_root(plugin_root))
    for catalog_path in sorted(layout.catalog_dir.glob("*.json")):
        if catalog_path.name in {"plugins.json", "install_failures.json"}:
            continue
        catalog = read_json(catalog_path, default={}) or {}
        if catalog.get("plugin_type") != "sensor" or not catalog.get("enabled", True):
            continue
        active_version = catalog.get("active_version")
        if not active_version:
            continue
        version_payload = (catalog.get("versions") or {}).get(active_version) or {}
        install_path_raw = version_payload.get("install_path")
        runtime_path_raw = version_payload.get("runtime_path")
        if not install_path_raw or not runtime_path_raw:
            continue
        install_path = Path(install_path_raw)
        manifest = read_json(install_path / "manifest.json", default={}) or {}
        metadata = _load_sensor_metadata(install_path, manifest)
        sensor_name = ((metadata.get("sensor") or {}).get("name") or manifest.get("display_name") or "").strip()
        if _normalize_name(sensor_name) != normalized:
            continue
        return InstalledSensorPlugin(
            plugin_id=str(catalog.get("plugin_id") or manifest.get("plugin_id")),
            sensor_name=sensor_name,
            install_path=install_path,
            runtime_path=Path(runtime_path_raw),
            manifest=manifest,
            metadata=metadata,
        )
    return None


def _load_sensor_metadata(install_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    bundle_dir = install_path / "bundle"
    metadata_candidate = bundle_dir / "metadata" / "sensor_spec.yaml"
    if metadata_candidate.exists():
        return yaml.safe_load(metadata_candidate.read_text(encoding="utf-8")) or {}
    spec_path = ((manifest.get("spec") or {}).get("path") or "").strip()
    if spec_path:
        for candidate in (bundle_dir / spec_path, bundle_dir / Path(spec_path).name):
            if candidate.exists():
                return yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
    return {}


#def _runtime_python(runtime_path: Path) -> Path:
#    return runtime_path / "bin" / "python"


def _normalize_name(name: str) -> str:
    return str(name).strip().lower()


def _routing_entries(values: Any) -> list[dict[str, str | None]]:
    entries: list[dict[str, str | None]] = []
    for value in values or []:
        if isinstance(value, str):
            text = value.strip()
            if text:
                entries.append({"name": text, "schema": None})
            continue
        if not isinstance(value, dict):
            continue
        name = str(value.get("name") or "").strip()
        schema = str(value.get("schema") or "").strip() or None
        if name or schema:
            entries.append({"name": name or None, "schema": schema})
    return entries


def deep_namespace_dict(payload: dict[str, Any]) -> dict[str, Any]:
    result = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            result[key] = deep_namespace(value)
        elif isinstance(value, list):
            result[key] = [deep_namespace(item) if isinstance(item, dict) else item for item in value]
        else:
            result[key] = value
    return result
