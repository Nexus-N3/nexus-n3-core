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
        self._connection.register_handler(
            "adapter.notification",
            self._handle_notification,
            ordered=True,
        )
        #self._notification_lock = threading.Lock()
        self._notification_context = threading.local()

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

    async def set_notify_callback(
        self,
        transport_client,
        uuid,
        callback_func,
        *,
        indicate: bool = False,
    ):
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
                "indicate": indicate,
            },
        )

    async def unset_notify_callback(self, transport_client, uuid):
        notify_uuid = str(uuid)
        self._connection.request("adapter.unsubscribe", {"uuid": notify_uuid})
        callback_id = self._callbacks_by_uuid.pop(notify_uuid, None)
        if callback_id is not None:
            self._callbacks.pop(callback_id, None)

    def _handle_notification(self, params: dict[str, Any]) -> dict[str, Any]:
        callback = self._callbacks[str(params["callback_id"])]
        sender = params.get("sender")
        data = base64.b64decode(params["data_b64"].encode("ascii"))
        self._notification_context.timing = dict(params.get("timing") or {})

        try:
            result = callback(sender, data)
            return {"awaited": bool(_run_maybe_async(result) is not None)}
        finally:
            self._notification_context.timing = None

    def current_notification_timing(self) -> dict[str, Any]:
        return dict(getattr(self._notification_context, "timing", None) or {})


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

    def wifi_discover_connected(self, params: dict[str, Any]) -> dict[str, Any]:
        """Run optional plugin discovery and serialize returned devices."""
        discover = getattr(self._sensor, "discover_connected", None)
        if not callable(discover):
            raise RuntimeError("sensor plugin does not implement Wi-Fi discovery")
        devices = _run_maybe_async(discover(deep_namespace(params["network"])))
        return {"devices": to_jsonable(devices or [])}

    def wifi_connect_sensor(self, params: dict[str, Any]) -> dict[str, Any]:
        """Run the plugin's vendor-specific Wi-Fi connection method."""
        connect = getattr(self._sensor, "connect_sensor", None)
        if not callable(connect):
            raise RuntimeError("sensor plugin does not implement Wi-Fi connect")
        result = _run_maybe_async(connect(deep_namespace(params["device"])))
        return {"ok": bool(result)}

    def wifi_disconnect_sensor(self, params: dict[str, Any]) -> dict[str, Any]:
        """Run the plugin's vendor-specific Wi-Fi disconnect method."""
        _ = params
        disconnect = getattr(self._sensor, "disconnect_sensor", None)
        if not callable(disconnect):
            raise RuntimeError("sensor plugin does not implement Wi-Fi disconnect")
        result = _run_maybe_async(disconnect())
        return {"ok": bool(result)}

    def wifi_classify_access_points(self, params: dict[str, Any]) -> dict[str, Any]:
        """Let the plugin claim compatible provisioning access points."""
        classify = getattr(self._sensor, "classify_access_points", None)
        if not callable(classify):
            return {"candidates": []}
        access_points = [
            deep_namespace(item)
            for item in (params.get("access_points") or [])
        ]
        result = _run_maybe_async(classify(access_points))
        return {"candidates": to_jsonable(result or [])}

    def wifi_provision(self, params: dict[str, Any]) -> dict[str, Any]:
        """Provision a sensor and return cleanup hints to Core."""
        provision = getattr(self._sensor, "provision", None)
        if not callable(provision):
            raise RuntimeError("sensor plugin does not implement Wi-Fi provisioning")
        controls = _HostProvisioningControls()
        result = _run_maybe_async(
            provision(
                deep_namespace(params["network"]),
                deep_namespace(params["target"]),
                controls,
            )
        )
        return {
            "device": to_jsonable(result),
            "remote_access_point_disappeared": controls.disappeared,
        }

    def wifi_identify_candidate(self, params: dict[str, Any]) -> dict[str, Any]:
        """Identify a provisioning candidate before it is modified."""
        identify = getattr(self._sensor, "identify_candidate", None)
        if not callable(identify):
            raise RuntimeError(
                "sensor plugin does not implement Wi-Fi candidate identification"
            )
        result = _run_maybe_async(
            identify(deep_namespace(params["network"]))
        )
        return {"device": to_jsonable(result)}

    def get_diagnostics_snapshot(self, params: dict[str, Any]) -> dict[str, Any]:
        """Return an optional JSON-safe diagnostics snapshot from the plugin."""
        _ = params
        getter = getattr(self._sensor, "get_diagnostics_snapshot", None)
        if not callable(getter):
            return {"supported": False, "snapshot": {}}
        result = _run_maybe_async(getter())
        return {"supported": True, "snapshot": to_jsonable(result or {})}

    def reset_session_diagnostics(self, params: dict[str, Any]) -> dict[str, Any]:
        """Reset optional plugin counters at a recording-session boundary."""
        _ = params
        reset = getattr(self._sensor, "reset_session_diagnostics", None)
        if not callable(reset):
            return {"supported": False}
        _run_maybe_async(reset())
        return {"supported": True}

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
        timing = None
        if event_name == "on_data":
            timing = {}
            payload_timing = getattr(payload, "_nexus_timing", None)
            if payload_timing:
                timing.update(dict(payload_timing))
            # Adapter-owned transports such as BLE remain authoritative when
            # both the transport callback and payload provide the same field.
            timing.update(self._adapter.current_notification_timing())
        self._adapter._connection.request(
            "sensor.emit_event",
            {
                "event": event_name,
                "payload": serialized,
                "timing": timing or None,
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
        "wifi.discover_connected": host.wifi_discover_connected,
        "wifi.connect_sensor": host.wifi_connect_sensor,
        "wifi.disconnect_sensor": host.wifi_disconnect_sensor,
        "wifi.classify_access_points": host.wifi_classify_access_points,
        "wifi.identify_candidate": host.wifi_identify_candidate,
        "wifi.provision": host.wifi_provision,
        "get_diagnostics_snapshot": host.get_diagnostics_snapshot,
        "reset_session_diagnostics": host.reset_session_diagnostics,
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


class _HostProvisioningControls:
    def __init__(self) -> None:
        self.disappeared = False

    def remote_access_point_disappeared(self) -> None:
        self.disappeared = True


if __name__ == "__main__":
    raise SystemExit(main())
