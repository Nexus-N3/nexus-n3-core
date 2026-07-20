"""Host process entry point for isolated sensor plugins."""

from __future__ import annotations

import argparse
import asyncio
import base64
import importlib
import sys
from pathlib import Path
from typing import Any
import threading

from ..common.jsonio import read_json
from ..common.jsonrpc import JsonRpcConnection
from .serde import deep_namespace, to_jsonable


def _load_symbol(raw: str):
    module_name, _, attr_name = raw.partition(":")
    if not module_name or not attr_name:
        raise ValueError(f"invalid entry point: {raw}")
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def _run_maybe_async(value):
    if asyncio.iscoroutine(value):
        return asyncio.run(value)
    return value


class HostAdapterProxy:
    """Adapter proxy exposed to the plugin inside the isolated host."""

    def __init__(self, connection: JsonRpcConnection):
        self._connection = connection
        self._callbacks: dict[str, Any] = {}
        self._callbacks_by_uuid: dict[str, str] = {}
        self._connection.register_handler("adapter.notification", self._handle_notification)
        self._notification_lock = threading.Lock()

    async def read(self, transport_client, uuid):
        result = self._connection.request("adapter.read", {"uuid": str(uuid)})
        return base64.b64decode(result["data_b64"].encode("ascii"))

    async def write(self, transport_client, uuid, char):
        self._connection.request(
            "adapter.write",
            {
                "uuid": str(uuid),
                "data_b64": base64.b64encode(bytes(char)).decode("ascii"),
            },
        )

    async def set_notify_callback(self, transport_client, uuid, callback_func):
        notify_uuid = str(uuid)
        callback_id = self._callbacks_by_uuid.get(notify_uuid)
        if callback_id is None:
            callback_id = f"cb-{len(self._callbacks) + 1}"
            self._callbacks_by_uuid[notify_uuid] = callback_id
        self._callbacks[callback_id] = callback_func
        self._connection.request(
            "adapter.subscribe",
            {
                "callback_id": callback_id,
                "uuid": notify_uuid,
            },
        )

    def _handle_notification(self, params: dict[str, Any]) -> dict[str, Any]:
        with self._notification_lock:
            callback = self._callbacks[str(params["callback_id"])]
            sender = params.get("sender")
            data = base64.b64decode(params["data_b64"].encode("ascii"))
            result = callback(sender, data)
            return {"awaited": bool(_run_maybe_async(result) is not None)}


class SensorHost:
    """Owns one plugin process runtime for one sensor plugin version."""

    def __init__(self, install_path: Path, connection: JsonRpcConnection):
        self.install_path = install_path
        self.manifest = read_json(install_path / "manifest.json", default={}) or {}
        self.capabilities = (self.manifest.get("capabilities") or {}).copy()
        self._sensor_cls = _load_symbol(
            "{module}:{callable}".format(**self.manifest["entrypoint"])
        )
        self._sensor = self._sensor_cls(None)
        self._adapter = HostAdapterProxy(connection)
        self._register_listeners()

    def describe(self) -> dict[str, Any]:
        return {
            "plugin_id": self.manifest.get("plugin_id"),
            "plugin_type": self.manifest.get("plugin_type"),
            "sensor_name": getattr(getattr(self._sensor, "sensor_type", None), "local_name", None),
        }

    def healthcheck(self) -> dict[str, Any]:
        return {"ok": True}

    def bind_sensor(self, params: dict[str, Any]) -> dict[str, Any]:
        self._sensor.address = params.get("address")
        self._sensor.location = params.get("location")
        self._sensor.transport_client = params.get("address")
        for key, value in (params.get("attributes") or {}).items():
            self._sensor.attributes[key] = value
        status_name = params.get("connection_status")
        if status_name:
            try:
                from nexus_n3_plugin_sdk.types.connections import ConnectionStatus

                self._sensor.set_connection_status(ConnectionStatus[status_name])
            except Exception:
                pass
        return {"bound": True}

    def setup(self, params: dict[str, Any]) -> dict[str, Any]:
        result = _run_maybe_async(
            self._sensor.setup(
                self._adapter,
                enable_battery=bool(params.get("enable_battery")),
                enable_button=bool(params.get("enable_button")),
            )
        )
        return {"ok": True, "result": to_jsonable(result)}

    def start_stream(self, params: dict[str, Any]) -> dict[str, Any]:
        result = _run_maybe_async(self._sensor.start_stream(self._adapter))
        return {"ok": True, "result": to_jsonable(result)}

    def stop_stream(self, params: dict[str, Any]) -> dict[str, Any]:
        result = _run_maybe_async(self._sensor.stop_stream(self._adapter))
        return {"ok": True, "result": to_jsonable(result)}

    def identify(self, params: dict[str, Any]) -> dict[str, Any]:
        result = _run_maybe_async(self._sensor.identify(self._adapter))
        return {"ok": True, "result": to_jsonable(result)}

    def consume_input(self, params: dict[str, Any]) -> dict[str, Any]:
        result = _run_maybe_async(
            self._sensor.consume_input(
                str(params["source_plugin_id"]),
                deep_namespace(params.get("payload")),
            )
        )
        return {"ok": bool(result)}

    def shutdown(self) -> dict[str, Any]:
        return {"ok": True}

    def _register_listeners(self) -> None:
        listeners = getattr(self._sensor, "listeners", {}) or {}
        for event_name in listeners.keys():
            self._sensor.register_listener(
                event_name,
                lambda payload, en=event_name: self._forward_event(en, payload),
            )

    def _forward_event(self, event_name: str, payload: Any) -> None:
        serialized = to_jsonable(payload)
        if isinstance(serialized, dict) and "sample_type" not in serialized:
            sample_type = getattr(payload, "sample_type", None)
            if sample_type:
                serialized["sample_type"] = sample_type
        self._adapter._connection.request(
            "sensor.emit_event",
            {
                "event": event_name,
                "payload": serialized,
            },
        )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--install-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    rpc_stdout = sys.stdout
    sys.stdout = sys.stderr
    connection = JsonRpcConnection(sys.stdin, rpc_stdout, name="sensor-host", autostart=False)
    host = SensorHost(Path(args.install_path).resolve(), connection)
    methods = {
        "describe": lambda _params: host.describe(),
        "healthcheck": lambda _params: host.healthcheck(),
        "bind_sensor": host.bind_sensor,
        "setup": host.setup,
        "start_stream": host.start_stream,
        "stop_stream": host.stop_stream,
        "identify": host.identify,
        "consume_input": host.consume_input,
        "shutdown": lambda _params: host.shutdown(),
    }
    for method_name, handler in methods.items():
        connection.register_handler(method_name, handler)
    connection.start()
    try:
        connection._reader_thread.join()
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
