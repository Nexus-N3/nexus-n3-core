"""Gateway message handler for commands and core dispatch."""

from nexus_n3.core.core import Core
from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.plugins.runtime.discovery import get_installed_plugin_inventory
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig

from datetime import datetime, timezone
from importlib import metadata

logger = get_module_logger("Message Handler")

class MessageHandler:
    def __init__(self, site, _system_event_bus, ble_runtime_config: BLERuntimeConfig | None = None):
        """
        Args:
            site: Site name for this deployment.
            _system_event_bus: Event bus for system events.
        """
        self.site = site
        self._system_event_bus = _system_event_bus
        self.ble_runtime_config = ble_runtime_config or BLERuntimeConfig.from_env()
        self.si = None
        self._dispatcher = None
        self._before_stream_start = None
        self._after_stream_stop = None
        self._usb_mount_handler = None
        self._usb_unmount_handler = None
        self._usb_status_provider = None
        self._device_info_provider = None
        self._robot_service = None

        self.is_ready = False
        self.registry = None

    def set_dispatcher(self, dispatcher):
        """
        Set a dispatcher to route messages in master mode.

        Args:
            dispatcher: Callable that accepts a message dict.
        """
        print("setting dispatcher")
        self._dispatcher = dispatcher

    def set_stream_lifecycle_hooks(self, before_stream_start=None, after_stream_stop=None):
        """
        Register optional local hooks around stream lifecycle operations.

        Args:
            before_stream_start: Callable invoked before a local start command.
            after_stream_stop: Callable invoked after a local full-stop command.
        """
        self._before_stream_start = before_stream_start
        self._after_stream_stop = after_stream_stop

    def set_usb_handlers(self, mount_handler=None, unmount_handler=None, status_provider=None):
        """Register optional USB control handlers for standalone/master nodes."""
        self._usb_mount_handler = mount_handler
        self._usb_unmount_handler = unmount_handler
        self._usb_status_provider = status_provider

    def set_device_info_provider(self, provider=None):
        """Register an optional runtime status provider for device info snapshots."""
        self._device_info_provider = provider

    def set_robot_service(self, robot_service=None):
        """Register an optional robot service for handling robot commands."""
        self._robot_service = robot_service

    def _release_version(self) -> str:
        """Return the installed nexus-n3-core version or 'unknown'."""
        for name in ("nexus-n3-core", "nexus_n3_core"):
            try:
                return metadata.version(name)
            except metadata.PackageNotFoundError:
                continue
        return "unknown"

    def _capabilities_payload(self) -> dict:
        """Return currently supported edge capabilities."""
        robot_status = self._robot_service.status() if self._robot_service else {
            "supported": False,
            "running": False,
            "robot_id": None,
        }
        if self.si:
            return {
                "supported_sensors": self.si.get_supported_sensors(),
                "supported_algorithms": self.si.get_supported_algorithms(),
                "supported_gateways": self.si.get_supported_gateways(),
                "supported_bridges": self.si.get_supported_bridges(),
                "ble_runtime": self.si.get_ble_runtime_config(),
                "robot": robot_status,
            }
        return {
            "supported_sensors": [],
            "supported_algorithms": [],
            "supported_gateways": [],
            "supported_bridges": [],
            "ble_runtime": self.ble_runtime_config.as_public_dict(),
            "robot": robot_status,
        }

    def _plugin_inventory_payload(self, capabilities: dict, snapshot_at: str) -> dict:
        """Build a first-pass plugin inventory from discovered sensor/algorithm support."""
        sensors = [
            {
                "name": sensor.get("name"),
                "locations": list(sensor.get("locations", []) or []),
                "computations": list(sensor.get("computations", []) or []),
            }
            for sensor in capabilities.get("supported_sensors", [])
        ]
        algorithms = [
            {"name": name}
            for name in capabilities.get("supported_algorithms", [])
        ]
        runtime = self._device_info_provider() if self._device_info_provider else {}
        runtime = runtime or {}
        apps = list(runtime.get("neia_apps", []) or [])
        workflows = list(runtime.get("neia_workflows", []) or [])
        return {
            "snapshot_id": f"{self.site}-{snapshot_at}",
            "synced_at": snapshot_at,
            "sensors": sensors,
            "algorithms": algorithms,
            "apps": apps,
            "workflows": workflows,
            "summary": {
                "sensors": len(sensors),
                "algorithms": len(algorithms),
                "apps": len(apps),
                "workflows": len(workflows),
            },
        }

    def _log_plugin_startup_summary(self) -> None:
        """Print/log the installed plugin inventory visible to this runtime."""
        inventory = get_installed_plugin_inventory()
        sensor_plugins = inventory["sensor_plugins"]
        algorithm_plugins = inventory["algorithm_plugins"]
        sensor_summary = ", ".join(
            f"{item['sensor_name']} ({item['version']})"
            for item in sensor_plugins
        ) or "none"
        algorithm_summary = ", ".join(
            f"{item['algorithm_name']} ({item['version']})"
            for item in algorithm_plugins
        ) or "none"
        summary = (
            f"[PLUGINS] root={inventory['plugin_root']} "
            f"sensors={inventory['sensor_count']} [{sensor_summary}] "
            f"algorithms={inventory['algorithm_count']} [{algorithm_summary}]"
        )
        print(summary)
        logger.info(summary)

    def _device_info_payload(self, *, correlation_id: str | None = None) -> dict:
        """Build a device snapshot aligned to the control-center device model."""
        runtime = self._device_info_provider() if self._device_info_provider else {}
        runtime = runtime or {}
        snapshot_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        capabilities = self._capabilities_payload()
        plugin_inventory = self._plugin_inventory_payload(capabilities, snapshot_at)
        active_bridge = runtime.get("active_bridge") or "none"
        status = runtime.get("status") or ("online" if self.is_ready else "offline")
        usb_disk = runtime.get("usb_disk") or {"present": False, "path": None}
        software_version = runtime.get("software_version") or f"nexus-n3-core {self._release_version()}"

        payload = {
            "site": self.site,
            "snapshot_at": snapshot_at,
            "device": {
                "display_name": runtime.get("display_name") or self.site,
                "site": self.site,
                "role": runtime.get("role"),
                "status": status,
                "gateway_name": runtime.get("gateway_name"),
                "active_bridge": active_bridge,
                "iot_hub_device_id": runtime.get("iot_hub_device_id"),
                "serial_number": runtime.get("serial_number"),
                "device_type": runtime.get("device_type"),
                "software_version": software_version,
            },
            "admin_summary": {
                "server_status": runtime.get("server_status", "unknown"),
                "uptime_seconds": runtime.get("uptime_seconds"),
                "uptime": runtime.get("uptime", "unknown"),
                "remote_bridge": active_bridge,
                "remote_control_enabled": bool(runtime.get("remote_control_enabled", False)),
                "usb_storage_mounted": bool(usb_disk.get("present")),
                "usb_disk": usb_disk,
                "last_heartbeat_at": runtime.get("last_heartbeat_at"),
            },
            "capabilities": capabilities,
            "plugin_inventory": plugin_inventory,
            "robot": capabilities["robot"],
        }
        if correlation_id:
            payload["correlation_id"] = correlation_id
        return payload

    def handle(self, msg: dict):
        """
        Handle an inbound message from the gateway.

        Args:
            msg: Message dictionary with type and optional payload.
        """
        print("message recieved in handler:", msg)
        msg_type = msg.get("type")
        payload = msg.get("payload", {})
        logger.info(f"message recv {msg_type}, {payload}")

        # If in master mode with dispatcher, pass command to dispatcher
        if self._dispatcher:
            self._dispatcher(msg)
            return

        # Otherwise execute locally (standalone mode and worker mode)
        self._handle_local(msg_type, payload)

    def _handle_local(self, msg_type, payload):
        """
        Execute commands locally for standalone/worker modes.

        Args:
            msg_type: Command type constant.
            payload: Command payload.
        """
        correlation_id = payload.get("correlation_id")

        def emit_error(message: str):
            error_payload = {"message": message}
            if correlation_id:
                error_payload["correlation_id"] = correlation_id
            self._system_event_bus.emit({
                "type": mt.EVT_ERROR,
                "payload": error_payload,
            })

        if msg_type == mt.CMD_IS_SERVER_READY:
            if self.is_ready:
                print("server is ready block")
                capabilities = self._capabilities_payload()
                response_payload = {
                    "msg": "System Server Ready",
                    "site": self.site,
                    "supported_sensors": capabilities["supported_sensors"],
                    "supported_algorithms": capabilities["supported_algorithms"],
                    "supported_gateways": capabilities["supported_gateways"],
                    "supported_bridges": capabilities["supported_bridges"],
                }
                correlation_id = payload.get("correlation_id")
                if correlation_id:
                    response_payload["correlation_id"] = correlation_id
                self._system_event_bus.emit({
                    "type": mt.EVT_SERVER_READY,
                    "payload": response_payload,
                })
            else:
                response_payload = {"msg": "System Server NOT Ready"}
                correlation_id = payload.get("correlation_id")
                if correlation_id:
                    response_payload["correlation_id"] = correlation_id
                self._system_event_bus.emit({
                    "type": mt.EVT_SERVER_READY,
                    "payload": response_payload,
                })

        elif msg_type == mt.CMD_GET_DEVICE_INFO:
            self._system_event_bus.emit({
                "type": mt.EVT_DEVICE_INFO,
                "payload": self._device_info_payload(correlation_id=payload.get("correlation_id")),
            })

        elif msg_type == mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE:
            message = payload.get("message")
            if not isinstance(message, dict):
                self._system_event_bus.emit({
                    "type": mt.EVT_ERROR,
                    "payload": "Control Center forward failed: invalid message payload",
                })
                return
            self._system_event_bus.emit({
                "type": mt.EVT_CONTROL_CENTER_MESSAGE,
                "payload": message,
            })
                
        elif msg_type == mt.CMD_SYSTEM_SETUP:
            self.si = Core(
                self.site,
                system_event_bus=self._system_event_bus,
                ble_runtime_config=self.ble_runtime_config,
            )
            if self.registry and hasattr(self.si, "compute_orch"):
                self.si.compute_orch.set_registry(self.registry)
            self.si.set_file_path(payload.get("file_path"))
            self._log_plugin_startup_summary()
            self.is_ready = True
            print("System setup complete")
        
        elif msg_type == mt.CMD_INIT_SYSTEM:
            try:
                print(f"initialising System with {payload['subjects']} subject(s)")
                self.si.pending_correlation_id = correlation_id
                self.si.init_core(
                    payload["subjects"],
                    init_label=payload.get("init_label"),
                    app_id=payload.get("app_id"),
                    app_name=payload.get("app_name"),
                )
                self.si.pending_correlation_id = None
            except Exception as exc:
                logger.exception("Failed to initialize system")
                self.si.pending_correlation_id = None
                emit_error(f"System init failed: {exc}")
        
        # this is to decide where to save files
        elif msg_type == mt.CMD_UPDATE_FILE_PATH:
            print("updating file path to", payload["file_path"])
            # this message needs to update Core si with the new path (or lack of it)
            self.si.set_file_path(payload["file_path"])
        elif msg_type == mt.CMD_USB_MOUNT:
            if not self._usb_mount_handler:
                self._system_event_bus.emit({
                    "type": mt.EVT_USB_STATUS,
                    "payload": {
                        "ok": False,
                        "action": "mount",
                        "present": False,
                        "path": None,
                        "error": "USB mount unsupported on this node",
                    },
                })
                return
            self._usb_mount_handler()
        elif msg_type == mt.CMD_USB_SAFE_UNMOUNT:
            if not self._usb_unmount_handler:
                self._system_event_bus.emit({
                    "type": mt.EVT_USB_STATUS,
                    "payload": {
                        "ok": False,
                        "action": "unmount",
                        "present": False,
                        "path": None,
                        "error": "USB unmount unsupported on this node",
                    },
                })
                return
            self._usb_unmount_handler()
        elif msg_type == mt.CMD_GET_USB_STATUS:
            if not self._usb_status_provider:
                self._system_event_bus.emit({
                    "type": mt.EVT_USB_STATUS,
                    "payload": {
                        "ok": False,
                        "action": "status",
                        "present": False,
                        "path": None,
                        "error": "USB status unavailable on this node",
                    },
                })
                return
            status = self._usb_status_provider(action="status", ok=True)
            self._system_event_bus.emit({
                "type": mt.EVT_USB_STATUS,
                "payload": status,
            })

        elif msg_type == mt.CMD_DISCOVER_SENSORS:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.discover_sensors()
            except Exception as exc:
                logger.exception("Failed to discover sensors")
                self.si.pending_correlation_id = None
                emit_error(f"Discover sensors failed: {exc}")
        elif msg_type == mt.CMD_DISCOVER_SENSORS_FOR_SUBJECTS:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.discover_sensors_for_subjects(payload["subject_ids"])
            except Exception as exc:
                logger.exception("Failed to discover sensors for subjects")
                self.si.pending_correlation_id = None
                emit_error(f"Discover sensors for subjects failed: {exc}")
        elif msg_type == mt.CMD_CONNECT_TO_ALL:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.connect_all()
            except Exception as exc:
                logger.exception("Failed to connect all sensors")
                self.si.pending_correlation_id = None
                emit_error(f"Connect all failed: {exc}")
        elif msg_type == mt.CMD_CONNECT_SUBJECTS:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.connect_subjects(payload["subject_ids"])
            except Exception as exc:
                logger.exception("Failed to connect subjects")
                self.si.pending_correlation_id = None
                emit_error(f"Connect subjects failed: {exc}")
        elif msg_type == mt.CMD_DISCONNECT_SUBJECTS:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.disconnect_subjects(payload["subject_ids"])
            except Exception as exc:
                logger.exception("Failed to disconnect subjects")
                self.si.pending_correlation_id = None
                emit_error(f"Disconnect subjects failed: {exc}")
        elif msg_type == mt.CMD_DISCONNECT_ALL:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.disconnect_all()
            except Exception as exc:
                logger.exception("Failed to disconnect all sensors")
                self.si.pending_correlation_id = None
                emit_error(f"Disconnect all failed: {exc}")

        # must broadcast the session timestamp
        elif msg_type == mt.CMD_START_STREAM_FOR_SUBJECTS:
            if self._before_stream_start:
                self._before_stream_start()
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.start_stream_for_subjects(payload)
            except Exception as exc:
                logger.exception("Failed to start stream for subjects")
                self.si.pending_correlation_id = None
                emit_error(f"Start stream for subjects failed: {exc}")
        elif msg_type == mt.CMD_START_STREAM_FOR_ALL:
            if self._before_stream_start:
                self._before_stream_start()
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.start_stream(payload)
            except Exception as exc:
                logger.exception("Failed to start stream for all")
                self.si.pending_correlation_id = None
                emit_error(f"Start stream for all failed: {exc}")
        elif msg_type == mt.CMD_STOP_STREAM_FOR_SUBJECTS:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.stop_stream_for_subjects(payload["subject_ids"], stop_context=payload)
                if self._after_stream_stop and not self.si.has_active_streams():
                    self._after_stream_stop()
            except Exception as exc:
                logger.exception("Failed to stop stream for subjects")
                self.si.pending_correlation_id = None
                emit_error(f"Stop stream for subjects failed: {exc}")
        elif msg_type == mt.CMD_STOP_STREAM_FOR_ALL:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.stop_stream(payload)
                if self._after_stream_stop and not self.si.has_active_streams():
                    self._after_stream_stop()
            except Exception as exc:
                logger.exception("Failed to stop stream for all")
                self.si.pending_correlation_id = None
                emit_error(f"Stop stream for all failed: {exc}")
        elif msg_type == mt.CMD_IDENTIFY_SENSOR:
            self.si.pending_correlation_id = correlation_id
            try:
                self.si.identify_sensor(payload["subject_id"], payload["location"])
            except Exception as exc:
                logger.exception("Failed to identify sensor")
                self.si.pending_correlation_id = None
                emit_error(f"Identify sensor failed: {exc}")
        elif msg_type == mt.CMD_CHECK_BATTERY:
            self.si.check_battery(
                scan_timeout=payload.get("scan_timeout", 5.0),
                read_timeout=payload.get("read_timeout", 10.0),
            )
        elif msg_type == mt.CMD_ROBOT_MOTION:
            if not self._robot_service:
                emit_error("Robot motion command failed: no robot service available on this node")
                return
            try:
                self._robot_service.on_message({
                    "type": "cmd_motion",
                    "action": payload.get("action"),
                    "speed": payload.get("speed", 0.0),
                    "robot_id": payload.get("robot_id"),
                    "source": payload.get("source", "gateway"),
                })
                self._system_event_bus.emit({
                    "type": mt.EVT_ROBOT_STATUS,
                    "payload": {
                        "ok": True,
                        "command": mt.CMD_ROBOT_MOTION,
                        "action": payload.get("action"),
                        "robot_id": payload.get("robot_id"),
                        "correlation_id": correlation_id,
                    },
                })
            except Exception as exc:
                logger.exception("Failed to execute robot motion command")
                emit_error(f"Robot motion command failed: {exc}")
        elif msg_type == mt.CMD_ROBOT_STOP:
            if not self._robot_service:
                emit_error("Robot stop command failed: no robot service available on this node")
                return
            try:
                self._robot_service.on_message({
                    "type": "cmd_stop",
                    "robot_id": payload.get("robot_id"),
                    "source": payload.get("source", "gateway"),
                })
                self._system_event_bus.emit({
                    "type": mt.EVT_ROBOT_STATUS,
                    "payload": {
                        "ok": True,
                        "command": mt.CMD_ROBOT_STOP,
                        "robot_id": payload.get("robot_id"),
                        "correlation_id": correlation_id,
                    },
                })
            except Exception as exc:
                logger.exception("Failed to execute robot stop command")
                emit_error(f"Robot stop command failed: {exc}")
        else:
            self._system_event_bus.emit({
                "type": mt.EVT_ERROR,
                "payload": {"msg": f"Unknown command {msg_type}"}
            })
