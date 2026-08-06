"""Gateway-backed BLE adapter.

This adapter preserves the public surface expected by existing BLE sensor
plugins while moving the transport execution to the Nexus BLE gateway over a
shared USB serial connection.

The important boundary is unchanged:

- plugins still see a `BLE` adapter contract
- plugins still receive raw notification payload bytes
- packet parsing remains in the plugin layer

The adapter owns the shared gateway client session plus a set of lightweight
per-sensor proxy transport clients keyed by BLE address.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

from nexus_n3.core.pipeline_diagnostics import pipeline_diagnostics
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.sensor_manager.adapters.gateway_ble_client import (
    GatewaySerialClient,
    SensorConnection,
    StreamFrame,
    discovered_devices_to_discovery_map,
)
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.types.connections import ConnectionStatus

logger = get_module_logger("Gateway BLE Adapter")


@dataclass
class GatewayBLETransportClient:
    """Proxy client representing a remote BLE sensor managed by the gateway."""

    address: str
    loop: Any = None
    disconnected_callback: Callable[[Any], None] | None = None
    is_connected: bool = False
    sensor_id: int | None = None
    binary_notify_uuid: str | None = None
    suppress_disconnect_event: bool = False
    notify_callbacks: dict[str, Callable[[Any, bytes], Any]] = field(default_factory=dict)


class GatewayBLEAdapter:
    """BLE adapter that delegates BLE execution to the gateway serial client.

    A single adapter instance owns one shared `GatewaySerialClient` and routes
    discovery, connect, subscribe, GATT I/O, disconnect, and diagnostics events
    back into the plugin-compatible callback model used by `SensorManager`.
    """

    adapter_type = "BLE"

    def __init__(self):
        self.ble_runtime_config = BLERuntimeConfig.from_env()
        self.gateway_client = GatewaySerialClient(
            self.ble_runtime_config,
            client_name="nexus_n3_gateway",
            verbose=False,
        )
        self.diagnostics_callback: Callable[[dict[str, Any]], None] | None = None
        self.transport_clients: dict[str, GatewayBLETransportClient] = {}
        self.gateway_client.register_event_handler("sensor_disconnected", self._handle_sensor_disconnected)
        self.gateway_client.register_event_handler("notification", self._handle_notification)
        self.gateway_client.register_event_handler("stream_frame", self._handle_stream_frame)
        self.gateway_client.register_event_handler("notification_drops", self._handle_notification_drops)
        self.gateway_client.register_event_handler("ble_notification_rx_stats_complete", self._handle_stats_complete)

    def close(self):
        """Close the shared gateway session and drop cached transport clients."""
        self.transport_clients.clear()
        self.gateway_client.close()

    def set_diagnostics_callback(self, callback: Callable[[dict[str, Any]], None] | None):
        """Register a callback for structured transport and gateway diagnostics."""
        self.diagnostics_callback = callback

    async def connect(self, ble_device: GatewayBLETransportClient):
        """Connect a BLE device through the gateway and return True/False."""
        return await self.execute(self._connect_sync, ble_device)

    async def disconnect(self, ble_device: GatewayBLETransportClient):
        """Disconnect a BLE device through the gateway and return True/False."""
        result = await self.execute(self._disconnect_sync, ble_device)
        if result is None:
            return not bool(getattr(ble_device, "is_connected", False))
        return result

    def create_transport_client(self, address: str, loop=None, disconnected_callback=None):
        """Create a per-sensor proxy transport client for a gateway-managed device."""
        client = GatewayBLETransportClient(
            address=address,
            loop=loop,
            disconnected_callback=disconnected_callback,
        )
        self.transport_clients[address.strip().upper()] = client
        return client

    @staticmethod
    async def connect_to_device(device, adapter, timeout: float = 10):
        """Connect a single device using the gateway-backed BLE adapter."""
        connect_logger = get_module_logger("Sensor Connect")
        msg = f"Connecting to device {device.name} (addr={getattr(device, 'address', None)})"
        print(msg)
        connect_logger.info(msg)
        try:
            connected = await asyncio.wait_for(
                adapter.connect(device.transport_client),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            timeout_msg = (
                f"Connect timeout for {device.name} (addr={getattr(device, 'address', None)})"
            )
            print(timeout_msg)
            connect_logger.warning(timeout_msg)
            return False

        if connected is None:
            connected = bool(getattr(device.transport_client, "is_connected", False))

        if connected:
            device.set_connection_status(ConnectionStatus.CONNECTED)
            ok_msg = f"Connected to {device.name} (addr={getattr(device, 'address', None)})"
            print(ok_msg)
            connect_logger.info(ok_msg)

        return connected

    async def connect_all(self, devices, adapter, timeout: float = 10):
        """Connect all devices sequentially using the gateway-backed adapter."""
        connect_logger = get_module_logger("Sensor Connect")
        msg = f"Connecting to {len(devices)} device(s)"
        print(msg)
        connect_logger.info(msg)
        result = []
        for device in devices:
            result.append(
                await GatewayBLEAdapter.connect_to_device(device, adapter, timeout=timeout)
            )
        return all(result)

    async def test_discover_devices(self, timeout: float = 5.0):
        """Test discovery through the gateway."""
        return await self.discover_devices([], timeout=timeout)

    async def discover_devices(self, requested, timeout: float = 5.0):
        """Discover devices through the gateway.

        This must eventually return enough metadata for host-side discovery
        matching in `DiscoveryService`.
        """
        requested_names = []
        for item in requested:
            name = item if isinstance(item, str) else getattr(item, "name", "")
            normalized = str(name).strip()
            if normalized:
                requested_names.append(normalized)
        unique_names = sorted(set(requested_names))
        multi_family_scan = len(unique_names) > 1
        effective_timeout_s = timeout
        if multi_family_scan:
            # Mixed-sensor discovery is executed as sequential per-family prefix
            # scans. Give each family enough time to be seen.
            effective_timeout_s = max(timeout, 10.0)
        timeout_ms = max(int(effective_timeout_s * 1000.0), 1000)

        if len(unique_names) == 1:
            devices = await self.execute(
                self.gateway_client.scan,
                timeout_ms,
                name_prefix_filter=unique_names[0],
            )
        else:
            merged_devices: dict[str, Any] = {}
            for name in unique_names:
                family_devices = await self.execute(
                    self.gateway_client.scan,
                    timeout_ms,
                    name_prefix_filter=name,
                )
                logger.info(
                    "Gateway discovery family scan requested_name=%s timeout_s=%.1f found=%s",
                    name,
                    effective_timeout_s,
                    [(device.address, device.name) for device in family_devices],
                )
                for device in family_devices:
                    merged_devices[device.address] = device
            devices = list(merged_devices.values())

        logger.info(
            "Gateway discovery complete requested_names=%s timeout_s=%.1f found=%s",
            unique_names,
            effective_timeout_s,
            [(device.address, device.name) for device in devices],
        )
        return discovered_devices_to_discovery_map(devices)

    @staticmethod
    async def execute(function, *args, **kwargs):
        """Execute gateway work on the event loop or a worker thread as needed."""
        try:
            if asyncio.iscoroutinefunction(function):
                result = await function(*args, **kwargs)
            else:
                result = await asyncio.to_thread(function, *args, **kwargs)
            return result
        except Exception as exc:
            logger.error("Error during gateway BLE operation: %s", exc)
            raise

    async def set_notify_callback(self, ble_device, uuid, callback_func):
        """Register a notification callback for a remote BLE characteristic.

        The gateway implementation must later subscribe remotely and route raw
        notifications back into this stored callback using the same callback
        shape expected by existing sensor plugins.
        """

        def wrapped_callback(sender, data):
            address = getattr(ble_device, "address", None)
            pipeline_diagnostics.mark_first_ble_notify(address)
            pipeline_diagnostics.increment(address, "ble_notify_count", 1)
            try:
                result = callback_func(sender, data)

                if asyncio.iscoroutine(result):
                    print("[BLE_CALLBACK_ASYNC]", address, uuid, flush=True)
                    asyncio.create_task(result)

                return result
            except Exception as exc:
                pipeline_diagnostics.increment(address, "ble_notify_callback_error_count", 1)
                pipeline_diagnostics.record_event(
                    "ble_notify_callback_error",
                    address=address,
                    uuid=str(uuid),
                    error=f"{type(exc).__name__}: {exc}",
                )
                raise

        subscribe_as_binary = len(ble_device.notify_callbacks) == 0
        ble_device.notify_callbacks[str(uuid)] = wrapped_callback
        if subscribe_as_binary:
            ble_device.binary_notify_uuid = str(uuid)
        await self.execute(
            self.gateway_client.subscribe_with_retry,
            ble_device.address,
            str(uuid),
            self.ble_runtime_config.gateway_subscribe_timeout_s,
            binary_notifications=subscribe_as_binary,
        )

    async def write(self, ble_device, uuid, char):
        """Write a GATT characteristic through the gateway."""
        return await self.execute(
            self.gateway_client.write_gatt,
            ble_device.address,
            str(uuid),
            bytes(char).hex(),
            self.ble_runtime_config.gateway_write_timeout_s,
            without_response=False,
        )

    async def read(self, ble_device, uuid):
        """Read a GATT characteristic through the gateway."""
        return await self.execute(
            self.gateway_client.read_gatt,
            ble_device.address,
            str(uuid),
            self.ble_runtime_config.gateway_read_timeout_s,
        )

    async def get_diagnostics_snapshot(self):
        """Return a structured snapshot of gateway transport diagnostics.

        The gateway is asked for a fresh status snapshot first, but a refresh
        failure does not prevent the adapter from returning the latest locally
        cached diagnostics payload.
        """
        try:
            await self.execute(
                self.gateway_client.get_status_snapshot,
                self.ble_runtime_config.gateway_read_timeout_s,
            )
        except Exception as exc:
            logger.warning("Gateway diagnostics snapshot refresh failed: %s", exc)
        return self._build_diagnostics_payload(event="gateway_status_snapshot")

    def _connect_sync(self, ble_device: GatewayBLETransportClient) -> bool:
        connections = self.gateway_client.connect(
            [ble_device.address],
            timeout_s=self.ble_runtime_config.gateway_connect_timeout_s,
        )
        if not connections:
            return False
        connection: SensorConnection = connections[0]
        ble_device.sensor_id = connection.sensor_id
        ble_device.is_connected = True
        self.transport_clients[ble_device.address.strip().upper()] = ble_device
        return True

    def _disconnect_sync(self, ble_device: GatewayBLETransportClient) -> bool:
        ble_device.suppress_disconnect_event = True
        try:
            disconnected = self.gateway_client.disconnect(
                [ble_device.address],
                timeout_s=self.ble_runtime_config.gateway_connect_timeout_s,
                allow_timeout=False,
            )
            ok = ble_device.address.strip().upper() in disconnected
            ble_device.is_connected = False
            ble_device.sensor_id = None
            ble_device.binary_notify_uuid = None
            ble_device.notify_callbacks.clear()
            self.transport_clients.pop(ble_device.address.strip().upper(), None)
            return ok
        finally:
            ble_device.suppress_disconnect_event = False

    def _handle_sensor_disconnected(self, msg: dict[str, Any]) -> None:
        address = str(msg.get("address", "")).strip().upper()
        if not address:
            return
        transport_client = self.transport_clients.get(address)
        if not transport_client:
            return
        if transport_client.suppress_disconnect_event:
            transport_client.is_connected = False
            return
        transport_client.is_connected = False
        if transport_client.disconnected_callback:
            transport_client.disconnected_callback(transport_client)

    def _handle_notification(self, msg: dict[str, Any]) -> None:
        address = str(msg.get("address", "")).strip().upper()
        uuid = str(msg.get("characteristic_uuid", ""))
        payload_hex = str(msg.get("payload_hex", ""))
        if not address or not uuid:
            return
        transport_client = self.transport_clients.get(address)
        if not transport_client:
            return
        callback = transport_client.notify_callbacks.get(uuid)
        if not callback:
            return
        callback(uuid, bytes.fromhex(payload_hex))

    def _handle_stream_frame(self, frame: StreamFrame) -> None:
        transport_client = None
        for candidate in self.transport_clients.values():
            if candidate.sensor_id == frame.sensor_id:
                transport_client = candidate
                break
        if not transport_client or not transport_client.binary_notify_uuid:
            return
        callback = transport_client.notify_callbacks.get(transport_client.binary_notify_uuid)
        if not callback:
            return
        callback(transport_client.binary_notify_uuid, frame.payload)

    def _handle_notification_drops(self, msg: dict[str, Any]) -> None:
        self._emit_diagnostics(
            self._build_diagnostics_payload(
                event="notification_drops",
                extra={"drop_count": msg.get("drop_count")},
            )
        )

    def _handle_stats_complete(self, _msg: dict[str, Any]) -> None:
        self._emit_diagnostics(self._build_diagnostics_payload(event="gateway_stats_update"))

    def _emit_diagnostics(self, payload: dict[str, Any]) -> None:
        if self.diagnostics_callback:
            self.diagnostics_callback(payload)

    def _build_diagnostics_payload(
        self,
        *,
        event: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sensors = []
        for client in self.transport_clients.values():
            sensors.append(
                {
                    "address": client.address,
                    "sensor_id": client.sensor_id,
                    "is_connected": bool(client.is_connected),
                }
            )
        payload = {
            "backend": self.ble_runtime_config.backend_label,
            "event": event,
            "notification_drop_count": self.gateway_client.notification_drop_count,
            "parser": {
                "stream_checksum_failures": self.gateway_client.stream_checksum_failures,
                "stream_resync_drop_bytes": self.gateway_client.stream_resync_drop_bytes,
                "stream_resync_events": self.gateway_client.stream_resync_events,
                "stream_partial_json_waits": self.gateway_client.stream_partial_json_waits,
                "stream_partial_frame_waits": self.gateway_client.stream_partial_frame_waits,
            },
            "transport": dict(self.gateway_client.gateway_transport_stats),
            "ble_rx": dict(self.gateway_client.gateway_ble_rx_stats),
            "sensors": sensors,
        }
        if extra:
            payload.update(extra)
        return payload
