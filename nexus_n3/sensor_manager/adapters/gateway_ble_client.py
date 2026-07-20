"""Shared serial transport client for the Nexus BLE gateway.

The gateway exposes BLE operations over a USB serial protocol. This client is
the low-level transport layer used by `GatewayBLEAdapter` and is responsible
for:

- serial port lifecycle
- request/response correlation by `request_id`
- async JSON event dispatch
- binary stream-frame parsing
- transport diagnostics and parser counters
- transport recovery when the gateway is powered on after server start or is
  power-cycled during runtime
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import json
import queue
import threading
import time
from typing import Any, Callable

import serial

from nexus_n3.logger.logger import get_module_logger
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig

logger = get_module_logger("Gateway BLE Client")

STREAM_FRAME_MAGIC = b"\xA5\x5A"


@dataclass(frozen=True)
class StreamFrame:
    sensor_id: int
    gateway_timestamp_us: int
    payload: bytes


@dataclass(frozen=True)
class DiscoveredDevice:
    address: str
    name: str = ""
    rssi: int | None = None
    service_uuids: tuple[str, ...] = ()
    raw: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SensorConnection:
    address: str
    sensor_id: int | None = None


@dataclass(frozen=True)
class GatewayAdvertisementData:
    local_name: str = ""
    service_uuids: tuple[str, ...] = ()
    rssi: int | None = None


@dataclass(frozen=True)
class GatewayBLEDevice:
    address: str
    name: str = ""
    path: str | None = None


def json_objects_from_line(line: str):
    decoder = json.JSONDecoder()
    for index, character in enumerate(line):
        if character != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(line[index:])
            yield obj
        except json.JSONDecodeError:
            continue


class GatewaySerialClient:
    """Threaded request/response and event client for the BLE gateway.

    One instance represents one host-side gateway session shared across all BLE
    sensors using the gateway backend.
    """

    def __init__(
        self,
        config: BLERuntimeConfig,
        *,
        client_name: str = "nexus_n3_gateway",
        verbose: bool = False,
    ):
        self.config = config
        self.client_name = client_name
        self.verbose = verbose
        self.ser: serial.Serial | None = None
        self.buf = bytearray()
        self.running = False
        self.started = False
        self.read_thread: threading.Thread | None = None
        self.write_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.pending_requests: dict[str, queue.Queue] = {}
        self.event_handlers: dict[str, list[Callable[[Any], None]]] = defaultdict(list)
        self.disconnected_addresses: set[str] = set()
        self.notification_drop_count: int = 0
        self.gateway_transport_stats: dict[str, Any] = {}
        self.gateway_ble_rx_stats: dict[str, dict[str, Any]] = {}
        self.stream_checksum_failures: int = 0
        self.stream_resync_drop_bytes: int = 0
        self.stream_resync_events: int = 0
        self.stream_partial_json_waits: int = 0
        self.stream_partial_frame_waits: int = 0
        self._partial_block_kind: str | None = None
        self._partial_block_len: int = -1
        self.phase = "idle"
        self._transport_reset_lock = threading.Lock()

    def start(self) -> None:
        """Open the serial port, start the reader, and complete the gateway handshake."""
        if self.started:
            return
        port = self.config.gateway_serial_port
        if not port:
            raise ValueError("GATEWAY_SERIAL_PORT is required for BLE gateway backend")

        try:
            self.ser = serial.Serial(
                port=port,
                baudrate=self.config.gateway_baudrate,
                timeout=0.1,
                write_timeout=1.0,
                dsrdtr=False,
                rtscts=False,
            )
            self.ser.setDTR(True)
            self.ser.setRTS(True)
            time.sleep(0.5)
            self.ser.reset_input_buffer()

            self.running = True
            self.read_thread = threading.Thread(
                target=self._read_loop,
                daemon=True,
                name="nexus-ble-gateway-read",
            )
            self.read_thread.start()
            self.started = True
            self.phase = "startup"
            self.reset_session(timeout_s=5.0)
            self.hello(protocol_version=self.config.gateway_protocol_version)
        except Exception:
            self._close_transport()
            raise

    def close(self) -> None:
        """Stop the reader and fully tear down the gateway transport session."""
        self._close_transport()
        self.pending_requests.clear()
        self.buf.clear()
        self.disconnected_addresses.clear()
        self.started = False
        self.phase = "idle"

    def _close_transport(self) -> None:
        """Close serial transport resources without clearing all cached state."""
        self.running = False
        if self.read_thread and self.read_thread.is_alive():
            self.read_thread.join(timeout=1.0)
        self.read_thread = None
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
        self.started = False
        self.phase = "idle"

    def request_id(self, prefix: str) -> str:
        return f"{prefix}_{int(time.time() * 1000)}"

    def register_event_handler(self, event_type: str, callback: Callable[[Any], None]) -> None:
        """Register a callback for a gateway JSON event type or parsed stream frame."""
        self.event_handlers[event_type].append(callback)

    def send(self, obj: dict[str, Any]) -> None:
        """Send a single JSON command to the gateway, starting the transport if needed."""
        self.start()
        assert self.ser is not None
        line = json.dumps(obj, separators=(",", ":")) + "\n"
        with self.write_lock:
            try:
                self.ser.write(line.encode("utf-8"))
                self.ser.flush()
            except (serial.SerialException, OSError) as exc:
                self._handle_transport_failure(
                    f"serial write failed: {type(exc).__name__}: {exc}",
                    recover=False,
                )
                raise

    def hello(self, protocol_version: int = 1) -> None:
        request_id = "hello_host_tool"
        request_queue = self._register_request(request_id)
        try:
            self.send(
                {
                    "type": "hello",
                    "request_id": request_id,
                    "protocol_version": protocol_version,
                    "client": self.client_name,
                }
            )
            self._wait_for_success(request_id, request_queue, "hello_ack", timeout_s=5.0)
        finally:
            self._unregister_request(request_id)

    def reset_session(self, timeout_s: float = 5.0) -> None:
        request_id = self.request_id("reset")
        request_queue = self._register_request(request_id)
        try:
            self.send({"type": "reset_session", "request_id": request_id})
            self._wait_for_success(request_id, request_queue, "reset_session_complete", timeout_s)
        finally:
            self._unregister_request(request_id)

    def scan(
        self,
        timeout_ms: int,
        *,
        name_filter: str | None = None,
        name_prefix_filter: str | None = None,
    ) -> list[DiscoveredDevice]:
        """Scan through the gateway and return normalized discovered devices."""
        def _scan_once() -> list[DiscoveredDevice]:
            request_id = self.request_id("scan")
            request_queue = self._register_request(request_id)
            matches: dict[str, DiscoveredDevice] = {}
            try:
                self.send({"type": "scan_start", "request_id": request_id, "timeout_ms": timeout_ms})
                deadline = time.time() + max(10.0, timeout_ms / 1000.0 + 5.0)
                while time.time() < deadline:
                    msg = self._wait_for_message(request_queue, deadline)
                    msg_type = msg.get("type")
                    if msg_type == "scan_result" and msg.get("request_id") == request_id:
                        name = str(msg.get("name", ""))
                        if name_filter is not None and name != name_filter:
                            continue
                        if name_prefix_filter is not None and not name.startswith(name_prefix_filter):
                            continue
                        address = self._normalize_address(msg.get("address"))
                        if not address or address in matches:
                            continue
                        service_uuids = tuple(
                            str(value).lower()
                            for value in msg.get("service_uuids", [])
                            if isinstance(value, str)
                        )
                        matches[address] = DiscoveredDevice(
                            address=address,
                            name=name,
                            rssi=msg.get("rssi"),
                            service_uuids=service_uuids,
                            raw=dict(msg),
                        )
                        continue
                    if msg_type == "scan_complete" and msg.get("request_id") == request_id:
                        return list(matches.values())
                raise TimeoutError("Timed out waiting for scan_complete")
            finally:
                self._unregister_request(request_id)

        return self._execute_with_transport_retry("scan", _scan_once)

    def connect(self, addresses: list[str], timeout_s: float) -> list[SensorConnection]:
        """Connect one or more BLE addresses through the gateway."""
        def _connect_once() -> list[SensorConnection]:
            request_id = self.request_id("connect")
            request_queue = self._register_request(request_id)
            pending = [self._normalize_address(address) for address in addresses]
            connected: list[SensorConnection] = []
            try:
                self.send({"type": "connect_addresses", "request_id": request_id, "addresses": addresses})
                deadline = time.time() + timeout_s
                while time.time() < deadline and pending:
                    msg = self._wait_for_message(request_queue, deadline)
                    msg_type = msg.get("type")
                    if msg_type == "sensor_connected":
                        address = self._normalize_address(msg.get("address"))
                        if address in pending:
                            pending.remove(address)
                            self.disconnected_addresses.discard(address)
                            connected.append(
                                SensorConnection(
                                    address=address,
                                    sensor_id=msg.get("sensor_id") if isinstance(msg.get("sensor_id"), int) else None,
                                )
                            )
                        continue
                    if msg_type == "sensor_disconnected":
                        address = self._normalize_address(msg.get("address"))
                        if address in pending:
                            pending.remove(address)
                        continue
                    self._raise_if_error(msg, request_id, "Gateway connect failed")
                if pending:
                    raise TimeoutError("Failed to connect: " + ", ".join(pending))
                return connected
            finally:
                self._unregister_request(request_id)

        return self._execute_with_transport_retry("connect", _connect_once)

    def subscribe(
        self,
        address: str,
        characteristic_uuid: str,
        timeout_s: float,
        *,
        binary_notifications: bool = False,
    ) -> None:
        """Subscribe to notifications for a characteristic through the gateway."""
        def _subscribe_once() -> None:
            request_id = self.request_id("subscribe")
            request_queue = self._register_request(request_id)
            try:
                self.send(
                    {
                        "type": "subscribe",
                        "request_id": request_id,
                        "address": address,
                        "characteristic_uuid": characteristic_uuid,
                        "binary_notifications": binary_notifications,
                    }
                )
                self._wait_for_success(request_id, request_queue, "subscribe_complete", timeout_s)
            finally:
                self._unregister_request(request_id)

        self._execute_with_transport_retry("subscribe", _subscribe_once)

    def subscribe_with_retry(
        self,
        address: str,
        characteristic_uuid: str,
        timeout_s: float,
        *,
        binary_notifications: bool = False,
        attempts: int = 2,
        retry_delay_s: float = 0.3,
    ) -> None:
        """Subscribe with a small retry budget to match the tooling reference behavior."""
        last_exc: Exception | None = None
        normalized_address = self._normalize_address(address)
        for attempt in range(1, max(attempts, 1) + 1):
            self.assert_connected(normalized_address, action="subscribe")
            try:
                self.subscribe(
                    normalized_address,
                    characteristic_uuid,
                    timeout_s,
                    binary_notifications=binary_notifications,
                )
                return
            except Exception as exc:
                last_exc = exc
                if self.is_disconnected(normalized_address):
                    raise RuntimeError(
                        f"sensor disconnected during subscribe address={normalized_address}: {exc}"
                    ) from exc
                if attempt >= max(attempts, 1):
                    break
                time.sleep(retry_delay_s)
        if last_exc is not None:
            raise last_exc

    def write_gatt(
        self,
        address: str,
        characteristic_uuid: str,
        payload_hex: str,
        timeout_s: float,
        *,
        without_response: bool = False,
        allow_timeout: bool = False,
    ) -> float | None:
        """Perform a GATT write through the gateway."""
        def _write_once() -> float | None:
            request_id = self.request_id("write")
            request_queue = self._register_request(request_id)
            try:
                self.send(
                    {
                        "type": "gatt_write",
                        "request_id": request_id,
                        "address": address,
                        "characteristic_uuid": characteristic_uuid,
                        "payload_hex": payload_hex,
                        "without_response": without_response,
                    }
                )
                try:
                    self._wait_for_success(request_id, request_queue, "write_complete", timeout_s)
                    return time.monotonic()
                except TimeoutError:
                    if allow_timeout:
                        return None
                    raise
            finally:
                self._unregister_request(request_id)

        return self._execute_with_transport_retry("write_gatt", _write_once)

    def read_gatt(self, address: str, characteristic_uuid: str, timeout_s: float) -> bytes:
        """Perform a GATT read through the gateway."""
        def _read_once() -> bytes:
            request_id = self.request_id("read")
            request_queue = self._register_request(request_id)
            try:
                self.send(
                    {
                        "type": "gatt_read",
                        "request_id": request_id,
                        "address": address,
                        "characteristic_uuid": characteristic_uuid,
                    }
                )
                deadline = time.time() + timeout_s
                while time.time() < deadline:
                    msg = self._wait_for_message(request_queue, deadline)
                    if msg.get("type") == "read_result" and msg.get("request_id") == request_id:
                        return bytes.fromhex(str(msg.get("payload_hex", "")))
                    self._raise_if_error(msg, request_id, "Gateway gatt_read failed")
                raise TimeoutError(f"Timed out waiting for gatt_read on {address}")
            finally:
                self._unregister_request(request_id)

        return self._execute_with_transport_retry("read_gatt", _read_once)

    def disconnect(
        self,
        addresses: list[str],
        timeout_s: float,
        *,
        allow_timeout: bool = False,
    ) -> list[str]:
        """Disconnect one or more addresses through the gateway."""
        def _disconnect_once() -> list[str]:
            request_id = self.request_id("disconnect")
            request_queue = self._register_request(request_id)
            pending = [
                self._normalize_address(address)
                for address in addresses
                if self._normalize_address(address) not in self.disconnected_addresses
            ]
            disconnected = [
                self._normalize_address(address)
                for address in addresses
                if self._normalize_address(address) in self.disconnected_addresses
            ]
            try:
                if not pending:
                    return disconnected
                self.send({"type": "disconnect_addresses", "request_id": request_id, "addresses": addresses})
                deadline = time.time() + timeout_s
                while time.time() < deadline and pending:
                    try:
                        msg = self._wait_for_message(request_queue, deadline)
                    except TimeoutError:
                        if allow_timeout:
                            return disconnected
                        raise
                    if msg.get("type") == "sensor_disconnected":
                        address = self._normalize_address(msg.get("address"))
                        if address in pending:
                            pending.remove(address)
                            disconnected.append(address)
                        continue
                    if msg.get("type") == "error" and msg.get("request_id") == request_id:
                        if msg.get("error_code") == -3:
                            break
                        self._raise_if_error(msg, request_id, "Gateway disconnect failed")
                if pending and not allow_timeout:
                    raise TimeoutError("Failed to disconnect: " + ", ".join(pending))
                return disconnected
            finally:
                self._unregister_request(request_id)

        return self._execute_with_transport_retry("disconnect", _disconnect_once)

    def is_disconnected(self, address: str) -> bool:
        return self._normalize_address(address) in self.disconnected_addresses

    def assert_connected(self, address: str, *, action: str) -> None:
        if self.is_disconnected(address):
            raise RuntimeError(f"sensor already disconnected before {action} address={address}")

    def get_status_snapshot(self, timeout_s: float = 10.0) -> dict[str, Any]:
        """Request and return a complete gateway diagnostics snapshot."""
        def _status_once() -> dict[str, Any]:
            request_id = self.request_id("status")
            request_queue = self._register_request(request_id)
            saw_status = False
            saw_transport_stats = False
            saw_ble_stats_complete = False
            self.gateway_transport_stats = {}
            self.gateway_ble_rx_stats = {}
            try:
                self.send({"type": "get_status", "request_id": request_id})
                deadline = time.time() + timeout_s
                while time.time() < deadline:
                    msg = self._wait_for_message(request_queue, deadline)
                    msg_type = msg.get("type")
                    if msg_type == "status":
                        saw_status = True
                    elif msg_type == "gateway_transport_stats":
                        saw_transport_stats = True
                    elif msg_type == "ble_notification_rx_stats_complete":
                        saw_ble_stats_complete = True
                    if saw_status and saw_transport_stats and saw_ble_stats_complete:
                        return {
                            "transport": dict(self.gateway_transport_stats),
                            "ble_rx": dict(self.gateway_ble_rx_stats),
                        }
                raise TimeoutError(
                    "Timed out waiting for complete status snapshot: "
                    f"saw_status={saw_status} saw_transport_stats={saw_transport_stats} "
                    f"saw_ble_stats_complete={saw_ble_stats_complete}"
                )
            finally:
                self._unregister_request(request_id)

        return self._execute_with_transport_retry("get_status", _status_once)

    def _read_loop(self) -> None:
        """Read serial bytes continuously and route parsed JSON or stream frames."""
        assert self.ser is not None
        while self.running:
            try:
                chunk = self.ser.read(256)
                if chunk:
                    self.buf.extend(chunk)
                while True:
                    item = self._extract_item()
                    if item is None:
                        break
                    item_type, payload = item
                    if item_type == "json":
                        self._observe_json(payload)
                        self._route_json(payload)
                    else:
                        self._dispatch_event("stream_frame", payload)
            except Exception as exc:
                if self.running:
                    self._handle_transport_failure(
                        f"serial read loop failed: {type(exc).__name__}: {exc}",
                        recover=False,
                    )
                    logger.exception("Gateway serial read loop failed: %s", exc)
                time.sleep(0.1)

    def _route_json(self, msg: dict[str, Any]) -> None:
        request_id = msg.get("request_id")
        if request_id and request_id in self.pending_requests:
            self.pending_requests[request_id].put(msg)
        elif msg.get("type") in {
            "gateway_transport_stats",
            "ble_notification_rx_stats",
            "ble_notification_rx_stats_complete",
        }:
            for pending_queue in self.pending_requests.values():
                pending_queue.put(msg)
        self._dispatch_event(str(msg.get("type", "")), msg)

    def _dispatch_event(self, event_type: str, payload: Any) -> None:
        for callback in list(self.event_handlers.get(event_type, [])):
            try:
                callback(payload)
            except Exception as exc:
                logger.exception("Gateway event handler failed for %s: %s", event_type, exc)

    def _extract_item(self):
        while self.buf:
            if self.buf[0] == ord("{"):
                newline_index = self.buf.find(b"\n")
                if newline_index < 0:
                    self._record_partial_block("json")
                    return None
                line = self.buf[:newline_index].decode("utf-8", errors="replace").strip()
                del self.buf[: newline_index + 1]
                self._clear_partial_block()
                if not line:
                    continue
                for msg in json_objects_from_line(line):
                    return ("json", msg)
                continue

            if len(self.buf) >= 2 and self.buf[:2] == STREAM_FRAME_MAGIC:
                if len(self.buf) < 14:
                    self._record_partial_block("frame")
                    return None
                version = self.buf[2]
                if version != 0x01:
                    self._drop_and_resync(1)
                    continue
                sensor_id = self.buf[3]
                gateway_timestamp_us = int.from_bytes(self.buf[4:12], "little")
                payload_len = self.buf[12]
                total_len = 13 + payload_len + 1
                if len(self.buf) < total_len:
                    self._record_partial_block("frame")
                    return None
                payload = bytes(self.buf[13 : 13 + payload_len])
                checksum = self.buf[13 + payload_len]
                computed = sum(self.buf[2 : 13 + payload_len]) & 0xFF
                if checksum != computed:
                    self.stream_checksum_failures += 1
                    self._drop_and_resync(1)
                    continue
                del self.buf[:total_len]
                self._clear_partial_block()
                return ("stream_frame", StreamFrame(sensor_id, gateway_timestamp_us, payload))

            next_json = self.buf.find(b"{")
            next_frame = self.buf.find(STREAM_FRAME_MAGIC)
            candidates = [index for index in (next_json, next_frame) if index >= 0]
            if not candidates:
                keep_len = 1 if self.buf[-1:] == STREAM_FRAME_MAGIC[:1] else 0
                self._drop_and_resync(len(self.buf) - keep_len)
                return None
            drop_len = min(candidates)
            if drop_len > 0:
                self._drop_and_resync(drop_len)
            else:
                self._clear_partial_block()
        return None

    def _observe_json(self, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type")
        if msg_type == "sensor_disconnected":
            address = self._normalize_address(msg.get("address"))
            if address:
                self.disconnected_addresses.add(address)
            return
        if msg_type == "notification_drops":
            value = msg.get("drop_count")
            if isinstance(value, int):
                self.notification_drop_count = value
            return
        if msg_type == "gateway_transport_stats":
            self.gateway_transport_stats = dict(msg)
            return
        if msg_type == "ble_notification_rx_stats":
            address = self._normalize_address(str(msg.get("address", "")))
            if address:
                normalized = dict(msg)
                normalized["address"] = address
                self.gateway_ble_rx_stats[address] = normalized

    def _register_request(self, request_id: str) -> queue.Queue:
        request_queue: queue.Queue = queue.Queue()
        self.pending_requests[request_id] = request_queue
        return request_queue

    def _unregister_request(self, request_id: str) -> None:
        self.pending_requests.pop(request_id, None)

    def _wait_for_success(
        self,
        request_id: str,
        request_queue: queue.Queue,
        success_type: str,
        timeout_s: float,
    ) -> dict[str, Any]:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            msg = self._wait_for_message(request_queue, deadline)
            if msg.get("type") == success_type and msg.get("request_id") == request_id:
                return msg
            self._raise_if_error(msg, request_id, "Gateway command failed")
        raise TimeoutError(f"Timed out waiting for {success_type} request_id={request_id}")

    def _wait_for_message(self, request_queue: queue.Queue, deadline: float) -> dict[str, Any]:
        timeout = max(0.0, deadline - time.time())
        if timeout <= 0:
            raise TimeoutError("Timed out waiting for gateway response")
        try:
            return request_queue.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError("Timed out waiting for gateway response") from exc

    def _handle_transport_failure(self, reason: str, *, recover: bool) -> None:
        """Tear down a broken transport session and optionally trigger recovery."""
        logger.warning("Gateway transport failure: %s", reason)
        self._close_transport()
        self.pending_requests.clear()
        self.buf.clear()
        if recover:
            self._recover_transport(reason)

    def _recover_transport(self, reason: str) -> None:
        """Reopen and re-handshake the gateway transport after a failure."""
        with self._transport_reset_lock:
            if self.started:
                return
            logger.info("Resetting gateway transport after failure: %s", reason)
            self.start()

    @staticmethod
    def _is_transport_retryable(exc: Exception) -> bool:
        if isinstance(exc, (serial.SerialException, OSError)):
            return True
        if not isinstance(exc, TimeoutError):
            return False
        text = str(exc)
        return (
            "gateway response" in text
            or "scan_complete" in text
            or "status snapshot" in text
            or "gatt_read" in text
        )

    def _execute_with_transport_retry(self, operation: str, func: Callable[[], Any]) -> Any:
        """Retry one operation after resetting the transport for retryable failures."""
        try:
            return func()
        except (TimeoutError, serial.SerialException, OSError) as exc:
            if not self._is_transport_retryable(exc):
                raise
            self._handle_transport_failure(
                f"{operation} failed: {type(exc).__name__}: {exc}",
                recover=False,
            )
            logger.info("Retrying gateway operation after transport reset: %s", operation)
            self._recover_transport(f"{operation} retry")
            return func()

    def _raise_if_error(self, msg: dict[str, Any], request_id: str, prefix: str) -> None:
        if msg.get("type") == "error" and msg.get("request_id") == request_id:
            raise RuntimeError(f"{prefix}: {msg.get('message')} ({msg.get('error_code')})")

    def _drop_and_resync(self, drop_len: int) -> None:
        if drop_len <= 0:
            self._clear_partial_block()
            return
        self.stream_resync_drop_bytes += drop_len
        self.stream_resync_events += 1
        del self.buf[:drop_len]
        self._clear_partial_block()

    def _record_partial_block(self, kind: str) -> None:
        current_len = len(self.buf)
        if self._partial_block_kind == kind and self._partial_block_len == current_len:
            return
        self._partial_block_kind = kind
        self._partial_block_len = current_len
        if kind == "json":
            self.stream_partial_json_waits += 1
        elif kind == "frame":
            self.stream_partial_frame_waits += 1

    def _clear_partial_block(self) -> None:
        self._partial_block_kind = None
        self._partial_block_len = -1

    @staticmethod
    def _normalize_address(address: str | None) -> str:
        return "" if not address else address.strip().upper()


def discovered_devices_to_discovery_map(
    devices: list[DiscoveredDevice],
) -> dict[str, tuple[GatewayBLEDevice, GatewayAdvertisementData]]:
    """Normalize gateway scan results to the discovery shape used by SensorManager."""
    normalized: dict[str, tuple[GatewayBLEDevice, GatewayAdvertisementData]] = {}
    for entry in devices:
        device = GatewayBLEDevice(address=entry.address, name=entry.name, path=entry.address)
        adv = GatewayAdvertisementData(
            local_name=entry.name,
            service_uuids=entry.service_uuids,
            rssi=entry.rssi,
        )
        normalized[entry.address] = (device, adv)
    return normalized
