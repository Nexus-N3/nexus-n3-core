"""Azure bridge service implementation."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
from queue import Empty, Queue
import signal
import threading
import time

from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.logger.logger import get_module_logger

from .azure_device_client import AzureDeviceClientAdapter
from .config import AzureBridgeConfig
from .file_upload import IoTHubFileUploader, build_session_blob_name
from .local_gateway_client import LocalGatewayClient
from .state_store import BridgeStateStore
from .telemetry_mapper import build_method_response_payload, map_event_for_cloud, new_correlation_id

logger = get_module_logger("Azure Bridge")

SAFE_READ_METHODS = {
    mt.CMD_IS_SERVER_READY,
    mt.CMD_GET_DEVICE_INFO,
    mt.CMD_CHECK_BATTERY,
    mt.CMD_GET_USB_STATUS,
}

METHOD_NAME_TO_COMMAND = {
    mt.CMD_IS_SERVER_READY: mt.CMD_IS_SERVER_READY,
    mt.CMD_GET_DEVICE_INFO: mt.CMD_GET_DEVICE_INFO,
    mt.EVT_CONTROL_CENTER_MESSAGE: mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE,
    mt.CMD_INIT_SYSTEM: mt.CMD_INIT_SYSTEM,
    mt.CMD_DISCOVER_SENSORS: mt.CMD_DISCOVER_SENSORS,
    mt.CMD_DISCOVER_SENSORS_FOR_SUBJECTS: mt.CMD_DISCOVER_SENSORS_FOR_SUBJECTS,
    mt.CMD_CONNECT_TO_ALL: mt.CMD_CONNECT_TO_ALL,
    mt.CMD_CONNECT_SUBJECTS: mt.CMD_CONNECT_SUBJECTS,
    mt.CMD_IDENTIFY_SENSOR: mt.CMD_IDENTIFY_SENSOR,
    mt.CMD_START_STREAM_FOR_ALL: mt.CMD_START_STREAM_FOR_ALL,
    mt.CMD_START_STREAM_FOR_SUBJECTS: mt.CMD_START_STREAM_FOR_SUBJECTS,
    mt.CMD_STOP_STREAM_FOR_ALL: mt.CMD_STOP_STREAM_FOR_ALL,
    mt.CMD_STOP_STREAM_FOR_SUBJECTS: mt.CMD_STOP_STREAM_FOR_SUBJECTS,
    mt.CMD_DISCONNECT_ALL: mt.CMD_DISCONNECT_ALL,
    mt.CMD_DISCONNECT_SUBJECTS: mt.CMD_DISCONNECT_SUBJECTS,
    mt.CMD_CHECK_BATTERY: mt.CMD_CHECK_BATTERY,
    mt.CMD_USB_MOUNT: mt.CMD_USB_MOUNT,
    mt.CMD_USB_SAFE_UNMOUNT: mt.CMD_USB_SAFE_UNMOUNT,
    mt.CMD_GET_USB_STATUS: mt.CMD_GET_USB_STATUS,
    mt.CMD_ROBOT_MOTION: mt.CMD_ROBOT_MOTION,
    mt.CMD_ROBOT_STOP: mt.CMD_ROBOT_STOP,
}

METHOD_REPLY_EVENTS = {
    mt.CMD_IS_SERVER_READY: {mt.EVT_SERVER_READY},
    mt.CMD_GET_DEVICE_INFO: {mt.EVT_DEVICE_INFO},
    mt.CMD_GET_USB_STATUS: {mt.EVT_USB_STATUS},
    # Session control feature mappings
    mt.CMD_INIT_SYSTEM: {mt.EVT_SYSTEM_INITIALIZED},
    mt.CMD_DISCOVER_SENSORS: {mt.EVT_SENSORS_DISCOVERED},
    mt.CMD_DISCOVER_SENSORS_FOR_SUBJECTS: {mt.EVT_SENSORS_DISCOVERED_FOR_SUBJECT},
    mt.CMD_CONNECT_SUBJECTS: {mt.EVT_SENSOR_CONNECTED},
    mt.CMD_IDENTIFY_SENSOR: {mt.EVT_SENSOR_IDENTIFIED},
    mt.CMD_START_STREAM_FOR_SUBJECTS: {mt.EVT_STREAM_STARTED},
    mt.CMD_STOP_STREAM_FOR_SUBJECTS: {mt.EVT_STREAM_STOPPED},
    mt.CMD_DISCONNECT_SUBJECTS: {mt.EVT_SENSOR_DISCONNECTED},
    mt.CMD_ROBOT_MOTION: {mt.EVT_ROBOT_STATUS},
    mt.CMD_ROBOT_STOP: {mt.EVT_ROBOT_STATUS},
}

METHOD_REPLY_TIMEOUT_SECONDS = 5.0
CONNECT_RETRY_POLL_SECONDS = 1.0
NEIA_TARGET_ALIASES = {
    "control_center",
    "neia",
    "neia_api",
    "neia.control_center",
    "neia-api.control_center",
}
LOCAL_ONLY_EVENT_TYPES = {
    mt.EVT_CONTROL_CENTER_MESSAGE,
}


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


@dataclass(slots=True)
class _CloudMethod:
    name: str
    payload: dict


@dataclass(slots=True)
class _PendingUpload:
    archive_path: str
    archive_name: str
    blob_name: str
    payload: dict
    attempts: int = 0
    next_attempt_at: float = 0.0


class AzureBridgeService:
    """Forward local events to Azure and gate remote commands."""

    def __init__(self, config: AzureBridgeConfig):
        self.config = config
        self.state_store = BridgeStateStore(config.state_file)
        self.local_client = LocalGatewayClient(
            cmd_pub_addr=config.local_cmd_pub_addr,
            evt_sub_addr=config.local_evt_sub_addr,
        )
        self.azure_client = AzureDeviceClientAdapter(
            config.connection_string,
            websockets=config.websockets,
            keep_alive=config.keep_alive,
            connection_retry_interval=config.connection_retry_interval,
        )
        self._running = False
        self._stop_event = threading.Event()
        self._capabilities = {
            "supported_sensors": [],
            "supported_algorithms": [],
            "supported_gateways": [],
            "supported_bridges": [],
        }
        self._last_upload = None
        self._pending_method_replies: dict[str, Queue] = {}
        self._pending_lock = threading.Lock()
        self._pending_uploads: dict[str, _PendingUpload] = {}
        self._pending_upload_lock = threading.Lock()
        self._local_client_started = False
        self._reconnect_requested = False
        self._last_connect_attempt_at = 0.0
        self.state_store.update(
            control_mode="remote_control_enabled" if config.remote_control_enabled else "local_primary"
        )

    def start(self, *, install_signal_handlers: bool = True) -> None:
        """Start bridge I/O loops and block until stopped."""
        self._stop_event.clear()
        while not self._stop_event.is_set():
            if self._ensure_cloud_connection(force=True):
                break
            time.sleep(CONNECT_RETRY_POLL_SECONDS)
        if self._stop_event.is_set():
            return
        self.local_client.start(self._handle_local_event)
        self._local_client_started = True
        self._publish_reported_properties()
        self._running = True
        logger.info("Azure bridge started", extra={"console": True})
        if install_signal_handlers:
            self._install_signal_handlers()
        try:
            while not self._stop_event.is_set():
                if self._reconnect_requested or not self.azure_client.connected:
                    self._ensure_cloud_connection()
                self._process_pending_uploads()
                time.sleep(CONNECT_RETRY_POLL_SECONDS)
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop bridge resources."""
        self._stop_event.set()
        if not self._running:
            return
        self._running = False
        if self._local_client_started:
            self.local_client.close()
            self._local_client_started = False
        self.azure_client.shutdown()
        logger.info("Azure bridge stopped", extra={"console": True})

    def status(self) -> dict:
        """Return a compact status snapshot for admin surfaces."""
        return {
            "enabled": True,
            "running": self._running,
            "connected": self.azure_client.connected,
            "device_id": self.config.device_id,
            "site": self.config.site,
            "remote_control_enabled": self.config.remote_control_enabled,
            "control_mode": self.state_store.state.control_mode,
            "last_upload": self._last_upload,
            "pending_uploads": len(self._pending_uploads),
        }

    def set_remote_control_enabled(self, enabled: bool) -> None:
        """Update remote-control policy and publish the new bridge state."""
        self.config.remote_control_enabled = enabled
        self.state_store.update(
            control_mode="remote_control_enabled" if enabled else "local_primary"
        )
        if self._running:
            self._publish_reported_properties()
        logger.info(
            f"remote control {'enabled' if enabled else 'disabled'}",
            extra={"console": True},
        )

    def _install_signal_handlers(self) -> None:
        def _stop_handler(_signum, _frame):
            self._stop_event.set()

        signal.signal(signal.SIGINT, _stop_handler)
        signal.signal(signal.SIGTERM, _stop_handler)

    def _handle_local_event(self, event: dict) -> None:
        event_type = event.get("type")
        if not event_type:
            return

        self._capture_method_reply(event)

        if event_type in LOCAL_ONLY_EVENT_TYPES:
            logger.info(
                f"local-only event received: type={event_type}",
                extra={"console": True},
            )
            return

        telemetry = map_event_for_cloud(
            event,
            device_id=self.config.device_id,
            customer_id=self.config.customer_id,
            site_id=self.config.site_id,
            site=self.config.site,
        )
        try:
            self.azure_client.send_telemetry(telemetry)
        except Exception as exc:
            self._request_reconnect(f"telemetry send failed: {type(exc).__name__}: {exc}")
            return
        logger.info(
            f"telemetry sent: type={event_type} device_id={self.config.device_id}",
            extra={"console": True},
        )

        if event_type == mt.EVT_SERVER_READY:
            payload = event.get("payload", {}) or {}
            self._capabilities["supported_sensors"] = payload.get("supported_sensors", [])
            self._capabilities["supported_algorithms"] = payload.get("supported_algorithms", [])
            self._capabilities["supported_gateways"] = payload.get("supported_gateways", [])
            self._capabilities["supported_bridges"] = payload.get("supported_bridges", [])
            self._publish_reported_properties()

        if event_type == mt.EVT_STREAM_DRAINED:
            self._handle_stream_drained(event.get("payload", {}) or {})

        if event_type == mt.EVT_ERROR:
            self.state_store.update(last_error=str(event.get("payload")))
            self._publish_reported_properties()

    def _handle_twin_patch(self, patch: dict) -> None:
        logger.info(f"Received desired property patch: {patch}")

        if "remote_control_enabled" in patch:
            self.set_remote_control_enabled(bool(patch["remote_control_enabled"]))

        if "device_lock_state" in patch:
            self.state_store.update(device_lock_state=str(patch["device_lock_state"]))
            self._publish_reported_properties()

    def _handle_method_request(self, request) -> None:
        method = _CloudMethod(
            name=str(getattr(request, "name", "")),
            payload=getattr(request, "payload", {}) or {},
        )
        correlation_id = new_correlation_id()

        if method.name not in METHOD_NAME_TO_COMMAND:
            self.azure_client.send_method_response(
                request,
                404,
                build_method_response_payload(
                    status=404,
                    message=f"Unknown method '{method.name}'",
                    correlation_id=correlation_id,
                ),
            )
            logger.info(
                f"method response sent: name={method.name} status=404 correlation_id={correlation_id}",
                extra={"console": True},
            )
            return

        if not self._method_allowed(method):
            self.azure_client.send_method_response(
                request,
                403,
                build_method_response_payload(
                    status=403,
                    message="Method blocked by bridge control policy",
                    correlation_id=correlation_id,
                    extra={
                        "control_mode": self.state_store.state.control_mode,
                        "device_lock_state": self.state_store.state.device_lock_state,
                    },
                ),
            )
            logger.info(
                f"method response sent: name={method.name} status=403 correlation_id={correlation_id}",
                extra={"console": True},
            )
            return

        if method.name == mt.EVT_CONTROL_CENTER_MESSAGE:
            self._handle_control_center_forward(request, method, correlation_id)
            return

        command = {
            "type": METHOD_NAME_TO_COMMAND[method.name],
            "payload": dict(method.payload),
        }
        command["payload"].setdefault("correlation_id", correlation_id)
        waiter = self._register_method_reply_waiter(command["type"], correlation_id)
        self.local_client.send_command(command)
        logger.info(
            f"local command published: type={command['type']} correlation_id={correlation_id}",
            extra={"console": True},
        )

        reply = self._wait_for_method_reply(command["type"], correlation_id, waiter)
        if reply is not None:
            if reply.get("type") == mt.EVT_ERROR:
                self.azure_client.send_method_response(
                    request,
                    500,
                    build_method_response_payload(
                        status=500,
                        message="Command failed",
                        correlation_id=correlation_id,
                        extra={
                            "command_type": command["type"],
                            "event_type": reply.get("type"),
                            "payload": reply.get("payload"),
                        },
                    ),
                )
                logger.info(
                    f"method response sent: name={method.name} status=500 correlation_id={correlation_id}",
                    extra={"console": True},
                )
                return
            self.azure_client.send_method_response(
                request,
                200,
                build_method_response_payload(
                    status=200,
                    message="Command completed",
                    correlation_id=correlation_id,
                    extra={
                        "command_type": command["type"],
                        "event_type": reply.get("type"),
                        "payload": reply.get("payload"),
                    },
                ),
            )
            logger.info(
                f"method response sent: name={method.name} status=200 correlation_id={correlation_id}",
                extra={"console": True},
            )
            return

        self.azure_client.send_method_response(
            request,
            202,
            build_method_response_payload(
                status=202,
                message="Command accepted by bridge",
                correlation_id=correlation_id,
                extra={"command_type": command["type"]},
            ),
        )
        logger.info(
            f"method response sent: name={method.name} status=202 correlation_id={correlation_id}",
            extra={"console": True},
        )

    def _handle_control_center_forward(self, request, method: _CloudMethod, correlation_id: str) -> None:
        if not self._is_neia_targeted_message(method.payload):
            self.azure_client.send_method_response(
                request,
                400,
                build_method_response_payload(
                    status=400,
                    message="Control Center message is not targeted for NEIA",
                    correlation_id=correlation_id,
                ),
            )
            logger.info(
                f"method response sent: name={method.name} status=400 correlation_id={correlation_id}",
                extra={"console": True},
            )
            return

        command = {
            "type": mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE,
            "payload": {
                "message": dict(method.payload),
                "correlation_id": correlation_id,
            },
        }
        self.local_client.send_command(command)
        logger.info(
            f"control center message forwarded locally: correlation_id={correlation_id}",
            extra={"console": True},
        )
        self.azure_client.send_method_response(
            request,
            202,
            build_method_response_payload(
                status=202,
                message="Control Center message accepted by bridge",
                correlation_id=correlation_id,
                extra={"command_type": command['type']},
            ),
        )
        logger.info(
            f"method response sent: name={method.name} status=202 correlation_id={correlation_id}",
            extra={"console": True},
        )

    def _method_allowed(self, method: _CloudMethod) -> bool:
        if self.config.remote_control_enabled:
            return True
        return method.name in SAFE_READ_METHODS

    @staticmethod
    def _is_neia_targeted_message(message: dict) -> bool:
        targets: set[str] = set()
        for key in ("target", "consumer", "bridge"):
            value = message.get(key)
            if isinstance(value, str):
                normalized = value.strip().lower()
                if normalized:
                    targets.add(normalized)
        for key in ("targets", "consumers"):
            value = message.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        normalized = item.strip().lower()
                        if normalized:
                            targets.add(normalized)
        return bool(targets & NEIA_TARGET_ALIASES)

    def _capture_method_reply(self, event: dict) -> None:
        payload = event.get("payload", {}) or {}
        if not isinstance(payload, dict):
            return
        correlation_id = payload.get("correlation_id")
        if not correlation_id:
            return
        with self._pending_lock:
            waiter = self._pending_method_replies.get(correlation_id)
        if waiter is None:
            return
        waiter.put(event)

    def _register_method_reply_waiter(self, command_type: str, correlation_id: str) -> Queue | None:
        expected_events = METHOD_REPLY_EVENTS.get(command_type)
        if not expected_events:
            return None

        waiter: Queue = Queue(maxsize=1)
        with self._pending_lock:
            self._pending_method_replies[correlation_id] = waiter
        return waiter

    def _wait_for_method_reply(self, command_type: str, correlation_id: str, waiter: Queue | None) -> dict | None:
        expected_events = METHOD_REPLY_EVENTS.get(command_type)
        if not expected_events or waiter is None:
            return None
        try:
            deadline = time.monotonic() + METHOD_REPLY_TIMEOUT_SECONDS
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                try:
                    event = waiter.get(timeout=remaining)
                except Empty:
                    return None
                if event.get("type") == mt.EVT_ERROR:
                    return event
                if event.get("type") in expected_events:
                    return event
        finally:
            with self._pending_lock:
                self._pending_method_replies.pop(correlation_id, None)

    def _handle_connection_state_change(self, *args, **kwargs) -> None:
        if not self.azure_client.connected:
            self._request_reconnect("azure client reported disconnected")
        logger.info(
            f"connection state changed: connected={self.azure_client.connected} args={args} kwargs={kwargs}",
            extra={"console": True},
        )

    def _handle_background_exception(self, exception: Exception) -> None:
        self._request_reconnect(f"background exception: {type(exception).__name__}: {exception}")
        logger.error(
            f"background exception: {type(exception).__name__}: {exception}",
            extra={"console": True},
        )

    def _handle_stream_drained(self, payload: dict) -> None:
        logger.info(
            f"stream drained received: payload={payload}",
            extra={"console": True},
        )
        if payload.get("status") != "ok":
            logger.info(
                f"upload skipped: stream_drained status={payload.get('status')}",
                extra={"console": True},
            )
            return
        if not payload.get("all_local_streams_stopped"):
            logger.info(
                "upload skipped: not all local streams are stopped",
                extra={"console": True},
            )
            return
        archive_path = payload.get("session_archive_path")
        archive_name = payload.get("session_archive_name")
        if not archive_path or not archive_name:
            logger.info(
                f"upload skipped: session archive metadata missing ({archive_path})",
                extra={"console": True},
            )
            return

        blob_name = build_session_blob_name(
            archive_name,
            customer_id=self.config.customer_id,
            site_id=self.config.site_id or self.config.site,
            device_id=self.config.device_id,
        )
        self._queue_pending_upload(
            payload=payload,
            archive_path=str(archive_path),
            archive_name=str(archive_name),
            blob_name=blob_name,
        )

    def _queue_pending_upload(self, *, payload: dict, archive_path: str, archive_name: str, blob_name: str) -> None:
        pending = _PendingUpload(
            archive_path=archive_path,
            archive_name=archive_name,
            blob_name=blob_name,
            payload=dict(payload),
            next_attempt_at=0.0,
        )
        with self._pending_upload_lock:
            existing = self._pending_uploads.get(archive_path)
            if existing is not None:
                existing.payload = dict(payload)
                existing.archive_name = archive_name
                existing.blob_name = blob_name
                existing.next_attempt_at = min(existing.next_attempt_at, pending.next_attempt_at)
            else:
                self._pending_uploads[archive_path] = pending
        logger.info(
            f"session archive queued for upload: archive={archive_path} blob_name={blob_name}",
            extra={"console": True},
        )

    def _process_pending_uploads(self, *, now: float | None = None) -> None:
        if not self.azure_client.connected:
            return
        current_time = time.monotonic() if now is None else now
        with self._pending_upload_lock:
            pending_uploads = [
                pending
                for pending in self._pending_uploads.values()
                if pending.next_attempt_at <= current_time
            ]
        for pending in pending_uploads:
            self._attempt_pending_upload(pending, current_time=current_time)

    def _attempt_pending_upload(self, pending: _PendingUpload, *, current_time: float) -> None:
        archive = Path(pending.archive_path)
        if not archive.is_file():
            self._schedule_upload_retry(
                pending,
                current_time=current_time,
                reason=f"session archive missing or not found ({pending.archive_path})",
            )
            return
        try:
            uploader = IoTHubFileUploader(self.azure_client)
            result = uploader.upload_file(pending.archive_path, blob_name=pending.blob_name)
            self._last_upload = {
                "success": result.success,
                "status_code": result.status_code,
                "status_description": result.status_description,
                "blob_name": result.blob_name,
                "container_name": result.container_name,
                "session_archive_path": pending.archive_path,
                "session_timestamp": pending.payload.get("session_timestamp"),
            }
            if not result.success:
                self._schedule_upload_retry(
                    pending,
                    current_time=current_time,
                    reason=(
                        f"session archive upload not successful: archive={pending.archive_path} "
                        f"status={result.status_code} reason={result.status_description}"
                    ),
                )
                if result.status_code >= 500:
                    self._request_reconnect(
                        f"upload failed with status {result.status_code}: {result.status_description}"
                    )
                return
            logger.info(
                f"session archive uploaded: archive={pending.archive_path} blob={result.blob_name}",
                extra={"console": True},
            )
            with self._pending_upload_lock:
                self._pending_uploads.pop(pending.archive_path, None)
            self._publish_reported_properties()
            self._request_usb_safe_unmount(pending)
        except Exception as exc:
            self._request_reconnect(f"session archive upload failed: {type(exc).__name__}: {exc}")
            self._schedule_upload_retry(
                pending,
                current_time=current_time,
                reason=(
                    f"session archive upload failed: archive={pending.archive_path} "
                    f"error={type(exc).__name__}: {exc}"
                ),
            )

    def _schedule_upload_retry(self, pending: _PendingUpload, *, current_time: float, reason: str) -> None:
        attempts = None
        with self._pending_upload_lock:
            tracked = self._pending_uploads.get(pending.archive_path)
            if tracked is None:
                return
            tracked.attempts += 1
            tracked.next_attempt_at = current_time + self.config.upload_retry_interval
            attempts = tracked.attempts
        logger.info(
            f"upload retry scheduled: archive={pending.archive_path} attempts={attempts} reason={reason}",
            extra={"console": True},
        )

    def _should_request_usb_safe_unmount(self, pending: _PendingUpload) -> bool:
        base_root = str((pending.payload or {}).get("base_root") or "").strip()
        if base_root.startswith("/exports/nexus_n3_data/nexus_n3_outputs"):
            return True
        archive_path = str(pending.archive_path or "").strip()
        return archive_path.startswith("/exports/nexus_n3_data/nexus_n3_outputs/")

    def _request_usb_safe_unmount(self, pending: _PendingUpload) -> None:
        if not self._local_client_started:
            return
        if not self._should_request_usb_safe_unmount(pending):
            logger.info(
                f"usb safe unmount skipped: archive uses local fallback storage ({pending.archive_path})",
                extra={"console": True},
            )
            return
        self.local_client.send_command({"type": mt.CMD_USB_SAFE_UNMOUNT, "payload": {}})
        logger.info(
            "usb safe unmount requested after successful upload",
            extra={"console": True},
        )

    def _publish_reported_properties(self) -> None:
        payload = {
            "bridge": {
                "version": _package_version("nexus-n3-core"),
                "device_id": self.config.device_id,
                "site": self.config.site,
                "gateway_mode": "local_zeromq_bridge",
                "remote_control_enabled": self.config.remote_control_enabled,
            },
            "control": self.state_store.snapshot(),
            "capabilities": self._capabilities,
        }
        if self._last_upload is not None:
            payload["last_upload"] = self._last_upload
        try:
            self.azure_client.patch_reported_properties(payload)
        except Exception as exc:
            self._request_reconnect(f"reported properties update failed: {type(exc).__name__}: {exc}")
            return
        logger.info(
            f"reported properties updated: device_id={self.config.device_id} control_mode={self.state_store.state.control_mode}",
            extra={"console": True},
        )

    def _configure_cloud_handlers(self) -> None:
        self.azure_client.set_method_handler(self._handle_method_request)
        self.azure_client.set_twin_patch_handler(self._handle_twin_patch)
        self.azure_client.set_connection_state_handler(self._handle_connection_state_change)
        self.azure_client.set_background_exception_handler(self._handle_background_exception)

    def _request_reconnect(self, reason: str) -> None:
        self._reconnect_requested = True
        logger.info(f"azure reconnect requested: {reason}", extra={"console": True})

    def _ensure_cloud_connection(self, *, force: bool = False) -> bool:
        if self.azure_client.connected and not self._reconnect_requested and not force:
            return True

        now = time.monotonic()
        if not force and (now - self._last_connect_attempt_at) < self.config.connection_retry_interval:
            return False
        self._last_connect_attempt_at = now

        try:
            self.azure_client.shutdown()
            self.azure_client.connect()
            self._configure_cloud_handlers()
            self._reconnect_requested = False
            logger.info(
                f"azure cloud connection established: device_id={self.config.device_id}",
                extra={"console": True},
            )
            if self._local_client_started:
                self._publish_reported_properties()
            return True
        except Exception as exc:
            logger.error(
                f"azure cloud connection failed: device_id={self.config.device_id} error={type(exc).__name__}: {exc}",
                extra={"console": True},
            )
            return False
