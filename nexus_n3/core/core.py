"""Core orchestration for subjects, sensors, compute, and storage."""

import time
import csv
import threading
from datetime import datetime
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.gateway.gateways.gateway_registry import discover_gateways
from nexus_n3.bridge.bridge_registry import discover_bridges
from nexus_n3.plugins.runtime.discovery import get_supported_algorithms as discover_supported_algorithms
from nexus_n3.plugins.runtime.discovery import get_supported_sensors as discover_supported_sensors
from nexus_n3.plugins.runtime.sensor_runtime import resolve_installed_sensor_class
from nexus_n3.core.orchestrators.subject_graph import SubjectGraph
from nexus_n3.core.orchestrators.storage_orchestrator import StorageOrchestrator
from nexus_n3.core.orchestrators.sensor_orchestrator import SensorOrchestrator
from nexus_n3.core.orchestrators.compute_orchestrator import ComputeOrchestrator
from nexus_n3.core.orchestrators.event_assembler import EventAssembler
from nexus_n3.core.pipeline_diagnostics import pipeline_diagnostics
from nexus_n3.core.startup_gate import StartupGateSensorStats
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig

logger = get_module_logger("Core Interface")

class Core:
    """
    Manages subjects, sensors, and file storage in Nexus N3 Core.

    This class provides a high-level interface for:
        - Initializing subjects and assigning sensors.
        - Managing the SensorManager lifecycle (connect, disconnect, stream).
        - Handling callbacks for discovery, connection, identification, and data ingestion.
        - Emitting system events via a provided SystemEventBus.

    Attributes:
        system_event_bus (SystemEventBus | None): Event bus for emitting system events.
        subjects (list[Subject]): List of subjects managed by the system.
        subject_graph (SubjectGraph): Subject and sensor assignment coordination.
        storage (StorageOrchestrator): File output and session handling.
        sensor_orch (SensorOrchestrator): Sensor manager lifecycle coordination.
        compute_orch (ComputeOrchestrator): Algorithm registration and compute coordination.
    """

    def __init__(self, site, system_event_bus=None, ble_runtime_config: BLERuntimeConfig | None = None):
        """
        Initialize the SystemInterface.

        Args:
            site (str): Location of deployment.
            subjects_config (list[dict]): Configuration for subjects and their sensors.
                Each dict must contain:
                    - 'subject_id': unique ID for the subject.
                    - 'sensors': list of sensor configs (with 'local_name', 'number_of').
                    - 'locations': list of body locations for each sensor.
            system_event_bus (SystemEventBus, optional): Event bus to emit system events.
        """
        self.site = site
        self.system_event_bus = system_event_bus
        self.ble_runtime_config = ble_runtime_config or BLERuntimeConfig.from_env()
        self.subject_graph = SubjectGraph(
            system_event_bus=self.system_event_bus,
            max_total_sensors=8,
        )
        self.storage = StorageOrchestrator(self.site)
        self.sensor_orch = SensorOrchestrator(
            self.system_event_bus,
            self._on_error,
            ble_runtime_config=self.ble_runtime_config,
        )
        self.compute_orch = ComputeOrchestrator(self.system_event_bus, self._on_error)
        self.event_assembler = EventAssembler()

        self.subjects = self.subject_graph.get_subjects()

        self.active_subject_ids = None  # used to keep track of subject ids for subject-scoped operations
        self.session_timestamp = None
        self.archived_session_timestamp = None
        self.app_id = None
        self.app_name = None
        self.MAX_TOTAL_SENSORS = 8 # this is a node ble radio limitatio
        self._battery_check_active = False
        self._battery_check_results = {}
        self._battery_check_errors = {}
        self._battery_check_expected = []
        self._battery_check_lock = threading.Lock()
        self.pending_correlation_id = None
        self._startup_lock = threading.RLock()
        self.stream_phase = "idle"
        self._startup_gate_enabled = True
        self._startup_attempt = 0
        self._startup_max_attempts = 1
        self._startup_stability_window_seconds = 3.0
        self._startup_packets_required = 60
        self._startup_min_rate_ratio = 0.95
        self._startup_min_observation_seconds = 2.0
        self._startup_retry_delay_seconds = 5.0
        self._startup_post_connect_settle_seconds = 2.0
        self._startup_subject_ids: list[str] = []
        self._startup_addresses: list[str] = []
        self._startup_stats_by_address: dict[str, StartupGateSensorStats] = {}
        self._startup_retry_pending = False
        self._startup_manual_stop = False
        self._startup_gate_token = 0
        self._pending_stream_tags: dict[str, str | None] = {}
        self._startup_last_failure_reason: str | None = None
        self._stream_stop_finalization_lock = threading.RLock()
        self._stream_stop_finalization_pending = False

    def get_ble_runtime_config(self) -> dict:
        """Return the active BLE runtime backend configuration."""
        return self.ble_runtime_config.as_public_dict()

    def _event_context(self, *, subject_ids: list[str] | None = None, extra: dict | None = None) -> dict:
        """Build common event metadata for downstream filtering/search."""
        payload = {
            "site": self.site,
            "session_timestamp": self.session_timestamp,
            "session_label": self.storage.file_manager.session_label,
            "session_name": self.storage.file_manager.session_name,
        }
        if self.app_id:
            payload["app_id"] = self.app_id
        if self.app_name:
            payload["app_name"] = self.app_name
        if self.pending_correlation_id:
            payload["correlation_id"] = self.pending_correlation_id
        if subject_ids:
            payload["subject_ids"] = list(subject_ids)
        if extra:
            payload.update(extra)
        return payload

    def _startup_policy_payload(self) -> dict:
        min_rate_thresholds = [
            self._effective_startup_min_rate_hz(stats)
            for stats in self._startup_stats_by_address.values()
            if self._effective_startup_min_rate_hz(stats) is not None
        ]
        return {
            "startup_stability_window_seconds": self._startup_stability_window_seconds,
            "startup_total_gate_seconds": (
                self._startup_post_connect_settle_seconds + self._startup_stability_window_seconds
            ),
            "startup_packets_required": self._startup_packets_required,
            "startup_min_rate_hz": min(min_rate_thresholds) if min_rate_thresholds else None,
            "startup_min_rate_ratio": self._startup_min_rate_ratio,
            "startup_min_observation_seconds": self._startup_min_observation_seconds,
            "retry_delay_seconds": self._startup_retry_delay_seconds,
            "post_connect_settle_seconds": self._startup_post_connect_settle_seconds,
        }

    def _reset_startup_gate_state(self, *, subject_ids: list[str], addresses: list[str]) -> None:
        with self._startup_lock:
            self._startup_gate_token += 1
            self._startup_attempt = 1
            self.stream_phase = "starting"
            self._startup_subject_ids = list(subject_ids)
            self._startup_addresses = list(addresses)
            self._startup_retry_pending = False
            self._startup_manual_stop = False
            self._startup_last_failure_reason = None
            self._startup_stats_by_address = {}
            start_command_time = time.monotonic()
            for address in self._startup_addresses:
                location = self._location_for_address(address)
                expected_rate_hz = self._expected_rate_for_address(address)
                stats = StartupGateSensorStats(address, location, expected_rate_hz)
                stats.reset_for_attempt(start_command_time=start_command_time)
                self._startup_stats_by_address[address] = stats

    def _clear_startup_gate_state(self, *, phase: str = "idle") -> None:
        with self._startup_lock:
            self._startup_gate_token += 1
            self.stream_phase = phase
            self._startup_subject_ids = []
            self._startup_addresses = []
            self._startup_retry_pending = False
            self._startup_manual_stop = False
            self._startup_last_failure_reason = None
            self._startup_stats_by_address = {}
            self._startup_attempt = 0
            self._pending_stream_tags = {}

    def _iter_sensor_entries(self):
        for subject in self.subjects:
            for entry in subject.sensors:
                yield subject, entry

    def _location_for_address(self, address: str) -> str | None:
        for _subject, entry in self._iter_sensor_entries():
            sensor = entry.get("sensor")
            if getattr(sensor, "address", None) == address:
                return entry.get("meta", {}).get("location")
        return None

    def _expected_rate_for_address(self, address: str) -> int | None:
        for _subject, entry in self._iter_sensor_entries():
            sensor = entry.get("sensor")
            if getattr(sensor, "address", None) != address:
                continue
            attributes = getattr(sensor, "attributes", {}) or {}
            value = attributes.get("SAMPLING_RATE")
            return int(value) if value else None
        return None

    def _startup_status_payload(self, *, phase: str, reason: str | None = None) -> dict:
        with self._startup_lock:
            sensors = []
            stable_count = 0
            for address in self._startup_addresses:
                stats = self._startup_stats_by_address.get(address)
                if not stats:
                    continue
                status = stats.as_status_payload()
                status["startup_min_rate_hz"] = self._effective_startup_min_rate_hz(stats)
                stable = self._is_sensor_startup_stable(stats)
                status["stable"] = stable
                sensors.append(status)
                if stable:
                    stable_count += 1
            payload = {
                "phase": phase,
                "attempt": self._startup_attempt,
                "max_attempts": self._startup_max_attempts,
                "required_sensor_count": len(self._startup_addresses),
                "connected_sensor_count": len(self._startup_addresses),
                "stable_sensor_count": stable_count,
                "sensors": sensors,
                **self._startup_policy_payload(),
            }
            if reason:
                payload["reason"] = reason
            return payload

    def _emit_startup_event(self, event_type: str, payload: dict) -> None:
        pipeline_diagnostics.record_event(f"startup_gate_{event_type}", **payload)
        self.storage.file_manager.append_session_diagnostics_event(
            self.session_timestamp,
            event_type,
            {
                **payload,
                **self._event_context(subject_ids=self._startup_subject_ids),
            },
        )
        if self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": event_type,
                    "payload": {
                        **payload,
                        **self._event_context(subject_ids=self._startup_subject_ids),
                    },
                }
            )

    def _is_sensor_startup_stable(self, stats: StartupGateSensorStats) -> bool:
        if stats.first_packet_time is None:
            return False
        if stats.startup_packets_received < self._startup_packets_required:
            return False
        if stats.startup_duration_seconds < self._startup_min_observation_seconds:
            return False
        effective_min_rate_hz = self._effective_startup_min_rate_hz(stats)
        if effective_min_rate_hz is not None and stats.startup_observed_rate_hz < effective_min_rate_hz:
            return False
        if stats.startup_gap_events > 0 or stats.startup_estimated_dropped_packets > 0:
            return False
        return True

    def _effective_startup_min_rate_hz(self, stats: StartupGateSensorStats) -> float | None:
        if not stats.expected_rate_hz:
            return None
        return float(stats.expected_rate_hz) * float(self._startup_min_rate_ratio)

    def _evaluate_startup_gate(self, token: int) -> None:
        settle_deadline = time.monotonic() + self._startup_post_connect_settle_seconds
        while time.monotonic() < settle_deadline:
            with self._startup_lock:
                if token != self._startup_gate_token:
                    return
                if self.stream_phase != "warming_up":
                    return
            time.sleep(0.1)

        deadline = time.monotonic() + self._startup_stability_window_seconds
        while time.monotonic() < deadline:
            with self._startup_lock:
                if token != self._startup_gate_token:
                    return
                if self.stream_phase != "warming_up":
                    return
            time.sleep(0.1)

        with self._startup_lock:
            if token != self._startup_gate_token or self.stream_phase != "warming_up":
                return
            stable = bool(self._startup_addresses) and all(
                self._is_sensor_startup_stable(self._startup_stats_by_address[address])
                for address in self._startup_addresses
                if address in self._startup_stats_by_address
            )
            if stable:
                self.stream_phase = "official_streaming"
                self._activate_official_streaming()
                payload = self._startup_status_payload(phase=self.stream_phase)
                self._emit_startup_event(mt.EVT_STREAM_OFFICIAL_STARTED, payload)
                return
            reason = "startup gate stability window elapsed before all sensors became stable"
            if self._startup_attempt < self._startup_max_attempts:
                self.stream_phase = "retrying_startup"
                self._startup_retry_pending = True
                payload = self._startup_status_payload(phase=self.stream_phase, reason=reason)
                self._emit_startup_event(mt.EVT_STREAM_STARTUP_RETRY, payload)
                self.sensor_orch.stop_specific(self._startup_addresses)
                return
            self.stream_phase = "startup_failed"
            self._startup_retry_pending = False
            self._startup_last_failure_reason = reason
            payload = self._startup_status_payload(phase=self.stream_phase, reason=reason)
            self._emit_startup_event(mt.EVT_STREAM_STARTUP_FAILED, payload)
            self.sensor_orch.stop_specific(self._startup_addresses)

    def _schedule_startup_retry(self, token: int) -> None:
        time.sleep(self._startup_retry_delay_seconds)
        with self._startup_lock:
            if token != self._startup_gate_token:
                return
            if self._startup_manual_stop or self.stream_phase != "retrying_startup":
                return
            self._startup_attempt += 1
            self._startup_retry_pending = False
            self.stream_phase = "starting"
            self._prepare_startup_retry_command_state()
            addresses = list(self._startup_addresses)
        self.sensor_orch.start_specific(addresses)

    def _prepare_startup_retry_command_state(self) -> None:
        start_command_time = time.monotonic()
        for stats in self._startup_stats_by_address.values():
            stats.reset_for_attempt(start_command_time=start_command_time)

    def _cleanup_failed_startup_session(self) -> None:
        failed_subject_ids = set(self._startup_subject_ids)
        for sub in self.subjects:
            if sub.subject_id not in failed_subject_ids:
                continue
            self.storage.file_manager.stop_stream(sub)
            sub.is_streaming = False
        reason = self._startup_last_failure_reason or "startup gate failed"
        self.storage.file_manager.finalize_session_diagnostics(
            self.session_timestamp,
            status="startup_failed",
            reason=reason,
            summary_updates={
                "official_stream_started": False,
                "startup_summary": self._startup_status_payload(phase="startup_failed", reason=reason),
                "stop_summary": {
                    "scope": "all",
                    "status": "error",
                    "reason": reason,
                    "failed_startup": True,
                },
            },
        )
        session_info = self.storage.file_manager.describe_session(self.session_timestamp)
        if not self.has_active_streams():
            session_info = self._finalize_session_archive()
        self._emit_stream_drained(
            None,
            scope="all",
            subject_ids=list(failed_subject_ids),
            status="error",
            reason=reason,
            session_info=session_info,
        )

    def _activate_official_streaming(self) -> None:
        active_subject_ids = set(self._startup_subject_ids)
        for sub in self.subjects:
            if sub.subject_id not in active_subject_ids:
                continue
            if not sub.is_streaming:
                tag = self._pending_stream_tags.get(sub.subject_id)
                self.storage.file_manager.start_stream(sub, self.session_timestamp, tag=tag)
                sub.is_streaming = True
        self.storage.file_manager.update_session_diagnostics_summary(
            self.session_timestamp,
            {
                "official_stream_started": True,
                "official_stream_started_at": datetime.now().isoformat(timespec="seconds"),
            },
        )

    # -------------------------
    # Private Initialization
    # -------------------------

    def init_core(
        self,
        subjects_config,
        init_label: str | None = None,
        app_id: str | None = None,
        app_name: str | None = None,
    ):
        """
        Initialize subjects and sensor manager from configuration.

        Args:
            subjects_config: List of subject configuration dicts.
            init_label: Optional top-level label for file output grouping.
            app_id: Optional stable application identifier for event attribution.
            app_name: Optional human-readable application name for event attribution.
        """
        self.compute_orch.reset()
        self.app_id = str(app_id).strip() if app_id else None
        self.app_name = str(app_name).strip() if app_name else None
        if init_label is not None:
            self.storage.set_session_label(init_label)
        success = self._init_subjects(subjects_config)
        if success:
            self._init_sensor_manager()
            self._register_callbacks()
            if self.system_event_bus:
                self.system_event_bus.emit({
                    "type": mt.EVT_SYSTEM_INITIALIZED,
                    "payload": {
                        "message": f"System initialised with {len(self.subjects)} subject(s)",
                        **self._event_context(subject_ids=[sub.subject_id for sub in self.subjects]),
                    },
                })

    def _init_subjects(self, subjects_config):
        """
        Initialize subjects and assign sensors to body locations.

        Validates that the number of sensors matches the number of body locations
        and that body locations are unique for each subject.

        Emits EVT_SYSTEM_INITIALIZED when done.
        """

        success = self.subject_graph.init_subjects(subjects_config, error_cb=self._on_error)
        self.subjects = self.subject_graph.get_subjects()
        return success


    def _init_sensor_manager(self):
        """Initialize the SensorManager with all sensors from all subjects."""
        # this is the sensor object on subject. 
        all_sensors = [entry for sub in self.subjects for entry in sub.sensors]
        self.sensor_orch.init_sensor_manager(all_sensors)

    def _register_callbacks(self):
        """Register all sensor manager callbacks for discover, connect, data, and battery events."""
        self.sensor_orch.register_callbacks({
            "on_discover": self._on_discover,
            "on_connected": self._on_connected,
            "on_disconnected": self._on_disconnected,
            "on_data": self._on_data,
            "on_identify": self._on_identify,
            "on_battery": self._on_battery,
            "on_stream_started": self._on_stream_started,
            "on_stream_stopped": self._on_stream_stopped,
            "on_diagnostics": self._on_diagnostics,
        })

        """ Register callbacks with compute manager"""
        self.compute_orch.register_listeners(self._on_compute_result, self._on_intermediate_result)

    # -------------------------
    # Public Methods
    # -------------------------

    def set_file_path(self, path: str | None):
        """Set the base file path for data storage. If None, use default local path."""
        if path:
            self.storage.set_file_path(path)
            print(f"File path updated to {path}")
            logger.info(f"File path updated to {path}")
        else:
            self.storage.set_file_path(None)
            print("File path reset to default local storage")
            logger.info("File path reset to default local storage")

    def get_subjects(self):
        """Return the list of subjects managed by the system."""
        return self.subjects

    def get_supported_sensors(self):
        """Return supported sensors with locations and computations."""
        return discover_supported_sensors()

    def get_supported_algorithms(self):
        """Return supported algorithm names."""
        return discover_supported_algorithms()

    def get_supported_gateways(self):
        """Return supported gateway keys."""
        return sorted(discover_gateways().keys())

    def get_supported_bridges(self):
        """Return supported remote bridge keys."""
        return sorted(discover_bridges().keys())

    def discover_sensors(self):
        """Discover all sensors in the system via SensorManager."""
        logger.info("Discovering sensors")
        self.sensor_orch.discover()

    def discover_sensors_for_subjects(self, subject_ids):
        """Discover sensors for specific subjects."""
        subjects = self._get_subjects_by_ids(subject_ids)
        self.active_subject_ids = [sub.subject_id for sub in subjects]
        for sub in subjects:
            logger.info(f"Discovering sensors for subject {sub.subject_id}")
            self.sensor_orch.discover_for_subject(sensors=sub.sensor_configs)

    def connect_all(self):
        """Connect all sensors in the system."""
        self.sensor_orch.connect_all()

    def connect_subjects(self, subject_ids):
        """Connect all sensors for a list of subjects by subject_id."""
        subjects = self._get_subjects_by_ids(subject_ids)
        self.active_subject_ids = [sub.subject_id for sub in subjects]
        addresses = [
            entry["sensor"].address
            for sub in subjects
            for entry in sub.sensors
            if entry["sensor"].address
        ]
        if addresses:
            self.sensor_orch.connect_specific(addresses)
            time.sleep(2)

    def disconnect_all(self):
        """Disconnect all sensors."""
        self._ensure_disconnect_allowed()
        self.sensor_orch.disconnect_all()
        time.sleep(2)

    def check_battery(self, scan_timeout: float = 5.0, read_timeout: float = 10.0):
        """
        Pre-init BLE battery check.

        Discovers all battery-capable BLE sensors, connects, reads battery, disconnects,
        and emits EVT_BATTERY_CHECK with results.
        """
        classes = set()
        for sensor_info in discover_supported_sensors():
            sensor_name = str(sensor_info.get("name") or "").strip()
            if not sensor_name:
                continue
            installed_cls = resolve_installed_sensor_class(sensor_name)
            if installed_cls is not None:
                classes.add(installed_cls)
        battery_classes = []
        for cls in classes:
            try:
                spec = cls.load_raw_spec()
            except Exception:
                continue
            if spec.get("sensor", {}).get("adapter") != "BLE":
                continue
            if "notify_battery" in (spec.get("capabilities", []) or []):
                battery_classes.append(cls)

        if not battery_classes:
            payload = {
                "timestamp": datetime.now().isoformat(),
                "results": [],
                "msg": "No BLE sensors with notify_battery capability found.",
            }
            if self.system_event_bus:
                self.system_event_bus.emit({"type": mt.EVT_BATTERY_CHECK, "payload": payload})
            return []

        with self._battery_check_lock:
            self._battery_check_results = {}
            self._battery_check_errors = {}
            self._battery_check_expected = []
            self._battery_check_active = True

        # Temporarily override on_battery listener to use core callback.
        previous_battery_cb = self.sensor_orch.get_listener("on_battery")
        self.sensor_orch.register_listener("on_battery", self._on_battery)

        future = self.sensor_orch.check_battery_preinit(
            battery_classes,
            scan_timeout=scan_timeout,
            read_timeout=read_timeout,
        )
        def _emit_results():
            try:
                summary = future.result(timeout=scan_timeout + read_timeout + 5.0)
                sensors_info = summary.get("sensors", [])
                errors = summary.get("errors", {})
                with self._battery_check_lock:
                    self._battery_check_expected = sensors_info
                    self._battery_check_errors = errors
                # Wait briefly for any late battery callbacks
                time_limit = time.time() + 1.0
                while time.time() < time_limit:
                    with self._battery_check_lock:
                        if len(self._battery_check_results) >= len(sensors_info):
                            break
                    time.sleep(0.05)

                final_results = []
                with self._battery_check_lock:
                    for sensor in sensors_info:
                        address = sensor.get("address")
                        name = sensor.get("name")
                        if address in self._battery_check_results:
                            entry = {
                                "address": address,
                                "name": name,
                                "status": "ok",
                                **self._battery_check_results[address],
                            }
                        elif address in self._battery_check_errors:
                            entry = {
                                "address": address,
                                "name": name,
                                "status": "error",
                                "error": self._battery_check_errors[address],
                            }
                        else:
                            entry = {
                                "address": address,
                                "name": name,
                                "status": "timeout",
                            }
                        final_results.append(entry)

                logger.info("Battery check complete: %d result(s)", len(final_results))
                print(f"[BatteryDebug] Emitting EVT_BATTERY_CHECK with {len(final_results)} result(s)")
                payload = {
                    "timestamp": datetime.now().isoformat(),
                    "results": final_results,
                }
                if self.system_event_bus:
                    self.system_event_bus.emit({"type": mt.EVT_BATTERY_CHECK, "payload": payload})
            except Exception as exc:
                logger.exception("Battery check failed")
                if self.system_event_bus:
                    self.system_event_bus.emit({
                        "type": mt.EVT_ERROR,
                        "payload": f"Battery check failed: {exc}",
                    })
            finally:
                self.sensor_orch.register_listener("on_battery", previous_battery_cb)
                with self._battery_check_lock:
                    self._battery_check_active = False

        threading.Thread(target=_emit_results, daemon=True).start()
        return []

    def disconnect_subjects(self, subject_ids):
        """Disconnect sensors for specific subjects."""
        self._ensure_disconnect_allowed()
        subjects = self._get_subjects_by_ids(subject_ids)
        addresses = [
            entry["sensor"].address
            for sub in subjects
            for entry in sub.sensors
            if entry["sensor"].address
        ]
        if addresses:
            self.sensor_orch.disconnect_addresses(addresses)
            time.sleep(2)

    def start_stream(self, payload):
        """Start streaming data for all subjects."""
        self.session_timestamp = payload.get("session_timestamp", datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.archived_session_timestamp = None
        tag_all = payload.get("tag")
        tags = payload.get("tags") or {}
        self._pending_stream_tags = {}
        stream_addresses = []
        for sub in self.subjects:
            tag = tags.get(sub.subject_id, tag_all)
            self._pending_stream_tags[sub.subject_id] = tag
            sub.is_streaming = False
            for entry in sub.sensors:
                entry["sensor"].raw_data = []
                if entry["sensor"].address:
                    stream_addresses.append(entry["sensor"].address)
        self._reset_startup_gate_state(
            subject_ids=[sub.subject_id for sub in self.subjects],
            addresses=stream_addresses,
        )
        self.storage.file_manager.start_session_diagnostics(
            self.session_timestamp,
            {
                "app_id": self.app_id,
                "app_name": self.app_name,
                "ble_backend": self.ble_runtime_config.backend_label,
                "subject_ids": [sub.subject_id for sub in self.subjects],
                "requested_sensor_addresses": list(stream_addresses),
                "startup_policy": self._startup_policy_payload(),
            },
        )
        self.sensor_orch.start_all()

    def start_stream_for_subjects(self, payload):
        """Start streaming sensors for specific subjects."""
        subject_ids = payload["subject_ids"]
        self.session_timestamp = payload.get("session_timestamp", datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.archived_session_timestamp = None
        tag_all = payload.get("tag")
        tags = payload.get("tags") or {}
        subjects = self._get_subjects_by_ids(subject_ids)
        self.active_subject_ids = [sub.subject_id for sub in subjects]
        self._pending_stream_tags = {}
        addresses = []
        for sub in subjects:
            tag = tags.get(sub.subject_id, tag_all)
            self._pending_stream_tags[sub.subject_id] = tag
            sub.is_streaming = False
            for entry in sub.sensors:
                entry["sensor"].raw_data = []
                if entry["sensor"].address:
                    addresses.append(entry["sensor"].address)
        self._reset_startup_gate_state(
            subject_ids=[sub.subject_id for sub in subjects],
            addresses=addresses,
        )
        self.storage.file_manager.start_session_diagnostics(
            self.session_timestamp,
            {
                "app_id": self.app_id,
                "app_name": self.app_name,
                "ble_backend": self.ble_runtime_config.backend_label,
                "subject_ids": [sub.subject_id for sub in subjects],
                "requested_sensor_addresses": list(addresses),
                "startup_policy": self._startup_policy_payload(),
            },
        )
        if addresses:
            self.sensor_orch.start_specific(addresses)

    def has_active_streams(self) -> bool:
        """Return True when any local subject is still marked as streaming."""
        return any(sub.is_streaming for sub in self.subjects)

    def stop_stream(self, stop_context=None):
        """Stop streaming data for all subjects and print sample counts."""
        self._stop_stream_impl(self.subjects, stop_specific=False, stop_context=stop_context)
    
    def stop_stream_for_subjects(self, subject_ids, stop_context=None):
        """Stop streaming sensors for specific subjects."""
        subjects = self._get_subjects_by_ids(subject_ids)
        self._stop_stream_impl(subjects, stop_specific=True, stop_context=stop_context)

    def _stop_stream_impl(self, subjects, stop_specific: bool, stop_context: dict | None = None):
        """Shared stop logic for full and subject-scoped stop operations."""
        subject_ids = [sub.subject_id for sub in subjects]
        scope = "subjects" if stop_specific else "all"
        self._set_stream_stop_finalization_pending(True)
        with self._startup_lock:
            self._startup_manual_stop = True
            self._startup_retry_pending = False
            if self.stream_phase not in {"idle", "startup_failed"}:
                self.stream_phase = "stopping"
            self._startup_gate_token += 1
        try:
            if stop_specific:
                self.active_subject_ids = subject_ids
                addresses = [
                    entry["sensor"].address
                    for sub in subjects
                    for entry in sub.sensors
                    if entry["sensor"].address
                ]
                if addresses:
                    self.sensor_orch.stop_specific(addresses)
            else:
                self.sensor_orch.stop_all()

            # Allow in-flight sample/compute callbacks to drain before paths are cleared.
            time.sleep(2)
            self._flush_pending_samples(subjects)
            self.storage.file_manager.flush()
            pipeline_diagnostics.record_event(
                "writer_flush",
                subject_ids=subject_ids,
                scope=scope,
            )
            consolidated_payloads = self.compute_orch.run_consolidation(
                subjects,
                self.storage.file_manager,
            )
            raw_write_failures = self.storage.file_manager.get_raw_write_failures(subject_ids)
            partial_markers = self.storage.file_manager.mark_subject_outputs_partial(subject_ids)
            if self.system_event_bus:
                for payload in consolidated_payloads:
                    if isinstance(payload, dict):
                        payload.update(
                            self._event_context(
                                subject_ids=[payload.get("subject_id")] if payload.get("subject_id") else None
                            )
                        )
                    self.system_event_bus.emit(
                        {"type": mt.EVT_CONSOLIDATED_RESULT, "payload": payload}
                    )
            for sub in subjects:
                for entry in sub.sensors:
                    path = entry["meta"].get("file_path")
                    if path and path.exists():
                        with open(path, "r", newline="") as f:
                            reader = csv.reader(f)
                            next(reader, None)
                            count = sum(1 for _ in reader)
                        #print(f"{sub.subject_id} {entry['meta']['location']}: {count} samples")
                self.storage.file_manager.stop_stream(sub)
                sub.is_streaming = False
            status = "error" if raw_write_failures else "ok"
            reason = None
            if raw_write_failures:
                failed_subjects = ", ".join(sorted(raw_write_failures.keys()))
                reason = f"raw write failures recorded for subject(s): {failed_subjects}"
            all_streams_stopped = not self.has_active_streams()
            diagnostics_updates = {
                "official_stream_started": self.stream_phase == "official_streaming",
                "raw_write_failures": raw_write_failures,
                "partial_markers": partial_markers,
                "stop_summary": {
                    "scope": scope,
                    "subject_ids": list(subject_ids),
                    "status": status,
                    "reason": reason,
                    "all_local_streams_stopped": all_streams_stopped,
                },
            }
            if all_streams_stopped:
                self.storage.file_manager.finalize_session_diagnostics(
                    self.session_timestamp,
                    status=status,
                    reason=reason,
                    summary_updates=diagnostics_updates,
                )
            else:
                self.storage.file_manager.update_session_diagnostics_summary(
                    self.session_timestamp,
                    diagnostics_updates,
                )
            archive_info = None
            if all_streams_stopped:
                archive_info = self._finalize_session_archive()
            if raw_write_failures:
                archive_info = archive_info or self.storage.file_manager.describe_session(self.session_timestamp)
                archive_info["partial"] = True
                archive_info["raw_write_failures"] = raw_write_failures
                archive_info["partial_markers"] = partial_markers
            pipeline_diagnostics.record_event(
                "stream_stop_summary",
                subject_ids=subject_ids,
                scope=scope,
                status=status,
                raw_write_failures=raw_write_failures,
                partial_markers=partial_markers,
            )
            pipeline_diagnostics.flush()
            self._set_stream_stop_finalization_pending(False)
            self._emit_stream_drained(
                stop_context,
                scope=scope,
                subject_ids=subject_ids,
                status=status,
                reason=reason,
                session_info=archive_info,
            )
        except Exception as exc:
            self._set_stream_stop_finalization_pending(False)
            self._emit_stream_drained(
                stop_context,
                scope=scope,
                subject_ids=subject_ids,
                status="error",
                reason=str(exc),
                session_info=self.storage.file_manager.describe_session(self.session_timestamp),
            )
            raise
        finally:
            self._set_stream_stop_finalization_pending(False)
            self._clear_startup_gate_state(phase="idle")

    def _finalize_session_archive(self) -> dict:
        """
        Archive the current session once after the last active subject stops.

        Returns:
            dict: Session metadata, including archive details when created.
        """
        if not self.session_timestamp:
            return self.storage.file_manager.describe_session(self.session_timestamp)
        if self.archived_session_timestamp == self.session_timestamp:
            return self.storage.file_manager.describe_session(self.session_timestamp)

        archive_info = self.storage.file_manager.archive_session(self.session_timestamp)
        self.archived_session_timestamp = self.session_timestamp
        return archive_info

    def _emit_stream_drained(
        self,
        stop_context: dict | None,
        *,
        scope: str,
        subject_ids: list[str],
        status: str,
        reason: str | None = None,
        session_info: dict | None = None,
    ):
        """Emit a post-cleanup drain event for stop coordination."""
        payload = {
            "stop_session_id": (stop_context or {}).get("stop_session_id"),
            "scope": scope,
            "subject_ids": list(subject_ids),
            "status": status,
            "all_local_streams_stopped": not self.has_active_streams(),
        }
        payload.update(self._event_context(subject_ids=subject_ids))
        payload.update(session_info or self.storage.file_manager.describe_session(self.session_timestamp))
        if reason:
            payload["reason"] = reason
        self.storage.file_manager.update_session_diagnostics_summary(
            self.session_timestamp,
            {"drain_summary": payload},
        )
        if not self.system_event_bus:
            return
        self.system_event_bus.emit({"type": mt.EVT_STREAM_DRAINED, "payload": payload})

    def _flush_pending_samples(self, subjects):
        """
        Flush any buffered raw samples that did not reach full block size.

        This prevents end-of-stream sample loss when stopping at arbitrary times.
        """
        for sub in subjects:
            for entry in sub.sensors:
                sensor = entry.get("sensor")
                if not sensor:
                    continue
                pending = list(getattr(sensor, "raw_data", []) or [])
                if not pending:
                    continue
                address = getattr(sensor, "address", None)
                pipeline_diagnostics.increment(address, "tail_flush_blocks_enqueued", 1)
                pipeline_diagnostics.increment(address, "tail_flush_samples_enqueued", len(pending))
                self.storage.file_manager.enqueue_block(entry, pending)
                sensor.raw_data = []

    def _set_stream_stop_finalization_pending(self, pending: bool) -> None:
        with self._stream_stop_finalization_lock:
            self._stream_stop_finalization_pending = pending

    def _ensure_disconnect_allowed(self) -> None:
        with self._stream_stop_finalization_lock:
            pending = self._stream_stop_finalization_pending
        if pending:
            raise RuntimeError(
                "Cannot disconnect sensors while stream finalization is still in progress"
            )

    def identify_sensor(self, subject_id, location):
        """Identify a sensor for a subject at a given body location."""
        print(f"Identifying sensor for subject {subject_id} at location {location}")
        subject = self.subject_graph.find_subject_by_id(subject_id)
        if not subject:
            raise ValueError(f"Subject {subject_id} not found")
        entry = next((s for s in subject.sensors if s["meta"]["location"] == location), None)
        if not entry:
            raise ValueError(f"No sensor assigned for {location} in {subject_id}")
        sensor = entry["sensor"]
        if sensor.connection_status.name != "CONNECTED":
            logger.warning(f"Sensor {sensor.address} not connected")
            return

        self.sensor_orch.identify(sensor.address)
        entry["meta"]["identified"] = True

    def _get_subjects_by_ids(self, subject_ids):
        subjects = []
        missing = []
        for subject_id in subject_ids:
            subject = self.subject_graph.find_subject_by_id(subject_id)
            if subject:
                subjects.append(subject)
            else:
                missing.append(subject_id)
        if missing:
            raise ValueError(f"Subject(s) not found: {missing}")
        return subjects

    def stop_manager(self):
        """Stop the SensorManager and all sensor operations."""
        self.sensor_orch.stop_manager()

    # -------------------------
    # Private Callbacks
    # -------------------------
    def _on_discover(self, payload):
        """Handle sensor discovery callbacks."""
        if isinstance(payload, dict) and "valid" in payload:
            if payload.get("valid") is False:
                missing = payload.get("missing", [])
                logger.error(f"Not enough devices found: {missing}")
                if self.system_event_bus:
                    self.system_event_bus.emit({
                        "type": mt.EVT_ERROR,
                        "payload": f"Not enough devices found: {missing}"
                    })
                return
        
        """Callback invoked when sensors are discovered."""
        discovered = []

        # Determine which subjects to process
        subjects_to_process = (
            [sub for sub in self.subjects if sub.subject_id in self.active_subject_ids]
            if getattr(self, "active_subject_ids", None) is not None
            else self.subjects
        )

        self.compute_orch.register_algorithms(subjects_to_process)

        for sub in subjects_to_process:
            discovered_addresses = []

            for entry in sub.sensors:
                sensor = entry["sensor"]
                if sensor.address is None:
                    continue

                discovered_addresses.append(sensor.address)

            discovered.append({
                "subject_id": sub.subject_id,
                "discovered_sensors": discovered_addresses
            })

        if self.system_event_bus:
            self.system_event_bus.emit({
                "type": mt.EVT_SENSORS_DISCOVERED,
                "payload": {
                    "subjects": discovered,
                    **self._event_context(subject_ids=[sub["subject_id"] for sub in discovered]),
                },
            })

        self.active_subject_ids = None
        self.pending_correlation_id = None

    def _on_connected(self, connected_sensors):
        """Callback invoked when sensors connect."""

        # Determine which subjects to process
        if getattr(self, "active_subject_ids", None) is not None:
            subjects_to_process = [
                sub for sub in self.subjects if sub.subject_id in self.active_subject_ids
            ]
        else:
            subjects_to_process = self.subjects  # all subjects

        connected = []
        for sub in subjects_to_process:
            connected_info = []
            for s in connected_sensors:
                entry = next((e for e in sub.sensors if e["sensor"] == s), None)
                if not entry:
                    continue
                connected_info.append({
                    "address": s.address,
                    "status": s.connection_status.name,
                    "location": entry["meta"].get("location"),
                })
            connected.append({
                "subject_id": sub.subject_id,
                "connected_sensors": connected_info
            })
        if self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": mt.EVT_SENSOR_CONNECTED,
                    "payload": {
                        "subjects": connected,
                        **self._event_context(subject_ids=[sub["subject_id"] for sub in connected]),
                    },
                }
            )
        
        self.active_subject_ids = None # reset the subject ids
        self.pending_correlation_id = None

    def _on_disconnected(self, disconnected_sensors):
        """Callback invoked when sensors disconnect."""
        if self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": mt.EVT_SENSOR_DISCONNECTED,
                    "payload": {
                        "disconnected_sensors": disconnected_sensors,
                        **self._event_context(),
                    },
                }
            )
        self.pending_correlation_id = None

    def _on_data(self, payload):
        """Callback invoked when sensor data is received."""
        pipeline_diagnostics.increment(
            getattr(payload, "address", None),
            "core_on_data_count",
            1,
            location=getattr(payload, "location", None),
        )
        address = getattr(payload, "address", None)
        sampling_rate = getattr(payload, "sampling_rate", None)
        with self._startup_lock:
            if address and address in self._startup_stats_by_address and self.stream_phase in {
                "starting",
                "warming_up",
                "retrying_startup",
                "official_streaming",
            } and sampling_rate is not None:
                self._startup_stats_by_address[address].record_sample(payload, time.monotonic())
            official_streaming = self.stream_phase == "official_streaming"
        if not official_streaming:
            return
        subject = self.subject_graph.find_subject_by_address(payload.address)
        if subject:
            subject.ingest_sample(payload, self.storage.file_manager)
            # push to compute manager also
            self.compute_orch.ingest_sample(payload)
        else:
            logger.warning(f"Data for unknown sensor {getattr(payload, 'address', 'unknown')}")

    # this is the callback that is registered with the compute manager
    def _on_intermediate_result(self, result: dict):
        
        """
        Called when an intermediate result is ready (per-sensor averages).
        Adds location metadata for each sensor and emits to the system event bus.
        """
        results = result.get("results", [])
        subjects_map = {}
        for entry in results:
            addr = entry.get("address")
            subject = None
            if addr:
                subject = self.subject_graph.find_subject_by_address(addr)
                if subject:
                    sensor_entry = next((e for e in subject.sensors if e["sensor"].address == addr), None)
                    if sensor_entry:
                        entry["location"] = sensor_entry["meta"].get("location")
            else:
                subject_id = entry.get("subject_id")
                if subject_id:
                    subject = self.subject_graph.find_subject_by_id(subject_id)
            if subject:
                subjects_map.setdefault(subject, []).append(entry)

        for subject, subject_results in subjects_map.items():
            subject_result = {
                "algorithm_name": result.get("algorithm_name"),
                "stage": result.get("stage"),
                "results": subject_results,
            }
            subject.ingest_intermediate_result(subject_result, self.storage.file_manager)

            stage_value = result.get("stage")
            if hasattr(stage_value, "value"):
                stage_value = stage_value.value

            intermediate_payload = self.event_assembler.build_intermediate_payload(
                subject.subject_id,
                result.get("algorithm_name"),
                str(stage_value),
                subject_results,
                context=self._event_context(subject_ids=[subject.subject_id]),
            )
            if self.system_event_bus:
                self.system_event_bus.emit({
                    "type": mt.EVT_INTERMEDIATE_RESULT,
                    "payload": intermediate_payload
                })


    def _on_compute_result(self, result):
        """
        Called when a per-sensor real-time result is received.
        Adds location metadata and emits to the system event bus.
        """
        # find the subject that owns this sensor
        result_address = result.get("address") if isinstance(result, dict) else getattr(result, "address", None)
        subject = self.subject_graph.find_subject_by_address(result_address)

        if subject is None:
            logger.warning(f"Result received for unknown sensor {result_address}")
            return

        # get location from sensor meta
        entry = next((e for e in subject.sensors if e["sensor"].address == result_address), None)
        location = entry["meta"].get("location") if entry else None

        # optionally give the result to the subject for file writing
        subject.ingest_result(result, self.storage.file_manager)

        # convert dataclass to dict and add extra metadata
        subject_result = self.event_assembler.build_compute_payload(
            subject.subject_id,
            result,
            location,
            context=self._event_context(subject_ids=[subject.subject_id]),
        )

        logger.info(
            "Emitting compute_result subject=%s address=%s location=%s algorithm=%s",
            subject.subject_id,
            result_address,
            location,
            result.get("algorithm_name") if isinstance(result, dict) else getattr(result, "algorithm_name", None),
        )

        if self.system_event_bus:
            self.system_event_bus.emit({
                "type": mt.EVT_COMPUTE_RESULT,
                "payload": subject_result
            })

    def _on_identify(self, sensor):
        """Callback invoked when a sensor identification occurs."""
        logger.info(f"Identify invoked for {sensor}")
        if self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": mt.EVT_SENSOR_IDENTIFIED,
                    "payload": {
                        "sensor": str(sensor),
                        **self._event_context(),
                    },
                }
            )
        self.pending_correlation_id = None

    def _on_battery(self, batt):
        """Callback invoked when a battery status is received."""
        logger.info(f"Battery update: {batt}")
        print(f"[BatteryDebug] Core _on_battery received: {batt}")
        address = batt.get("address") if isinstance(batt, dict) else None
        battery = batt.get("battery") if isinstance(batt, dict) else None
        battery_level = getattr(battery, "battery_level", None)
        is_charging = getattr(battery, "is_charging", None)
        if isinstance(battery, dict):
            battery_level = battery.get("battery_level", battery_level)
            is_charging = battery.get("is_charging", is_charging)
        payload = {
            "address": address,
            "battery_level": battery_level,
            "is_charging": is_charging,
        }
        payload.update(self._event_context())
        if self.system_event_bus:
            print("[BatteryDebug] Emitting EVT_BATTERY_UPDATE")
            self.system_event_bus.emit({"type": mt.EVT_BATTERY_UPDATE, "payload": payload})
        if not self._battery_check_active:
            return
        if not address:
            return
        with self._battery_check_lock:
            self._battery_check_results[address] = {
                "battery_level": battery_level,
                "is_charging": is_charging,
            }

    def _on_stream_started(self, addresses):
        """Callback invoked when streaming is started"""
        # Determine which subjects to process
        if getattr(self, "active_subject_ids", None) is not None:
            subjects_to_process = [
                sub for sub in self.subjects if sub.subject_id in self.active_subject_ids
            ]
        else:
            subjects_to_process = self.subjects  # all subjects
        
        streaming_sensors = []
        for sub in subjects_to_process:
            sensor_addresses = [
                entry["sensor"].address
                for entry in sub.sensors
                if entry["sensor"].address in addresses
            ]
            streaming_sensors.append({
                "subject_id": sub.subject_id,
                "streaming_sensors": sensor_addresses
            })
            
        logger.info(f"started streaming for sensors {sensor_addresses}")
        if self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": mt.EVT_STREAM_STARTED,
                    "payload": {
                        "subjects": streaming_sensors,
                        **self._event_context(subject_ids=[sub["subject_id"] for sub in streaming_sensors]),
                    },
                }
            )
        self.storage.file_manager.append_session_diagnostics_event(
            self.session_timestamp,
            mt.EVT_STREAM_STARTED,
            {
                "subjects": streaming_sensors,
                **self._event_context(subject_ids=[sub["subject_id"] for sub in streaming_sensors]),
            },
        )
        should_start_gate = False
        token = None
        with self._startup_lock:
            if self._startup_gate_enabled and self._startup_addresses:
                self.stream_phase = "warming_up"
                token = self._startup_gate_token
                should_start_gate = True
        if should_start_gate:
            self._emit_startup_event(
                mt.EVT_STREAM_WARMUP_STARTED,
                self._startup_status_payload(phase="warming_up"),
            )
            threading.Thread(
                target=self._evaluate_startup_gate,
                args=(token,),
                daemon=True,
            ).start()
        self.active_subject_ids = None # reset the subject ids
        self.pending_correlation_id = None
    
    def _on_stream_stopped(self, addresses):
        """Callback invoked when streaming is started"""
        # Determine which subjects to process
        if getattr(self, "active_subject_ids", None) is not None:
            subjects_to_process = [
                sub for sub in self.subjects if sub.subject_id in self.active_subject_ids
            ]
        else:
            subjects_to_process = self.subjects  # all subjects
        
        streaming_sensors = []
        for sub in subjects_to_process:
            sensor_addresses = [
                entry["sensor"].address
                for entry in sub.sensors
                if entry["sensor"].address in addresses
            ]
            streaming_sensors.append({
                "subject_id": sub.subject_id,
                "discovered_sensors": sensor_addresses
            })
            
        #logger.info(f"stopped streaming for sensors {sensor_addresses}")
        if self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": mt.EVT_STREAM_STOPPED,
                    "payload": {
                        "subjects": streaming_sensors,
                        **self._event_context(subject_ids=[sub["subject_id"] for sub in streaming_sensors]),
                    },
                }
            )
        self.storage.file_manager.append_session_diagnostics_event(
            self.session_timestamp,
            mt.EVT_STREAM_STOPPED,
            {
                "subjects": streaming_sensors,
                **self._event_context(subject_ids=[sub["subject_id"] for sub in streaming_sensors]),
            },
        )
        retry_token = None
        should_retry = False
        with self._startup_lock:
            if self.stream_phase == "retrying_startup" and self._startup_retry_pending and not self._startup_manual_stop:
                retry_token = self._startup_gate_token
                should_retry = True
            elif self.stream_phase == "startup_failed":
                pass
            elif self.stream_phase != "official_streaming":
                self.stream_phase = "idle"
        if should_retry:
            threading.Thread(
                target=self._schedule_startup_retry,
                args=(retry_token,),
                daemon=True,
            ).start()
        elif self.stream_phase == "startup_failed":
            self._cleanup_failed_startup_session()
        self.active_subject_ids = None # reset the subject ids
        self.pending_correlation_id = None

    def _on_diagnostics(self, diagnostics):
        payload = {
            **(diagnostics if isinstance(diagnostics, dict) else {"diagnostics": diagnostics}),
            **self._event_context(),
        }
        pipeline_diagnostics.record_event("transport_diagnostics_event", diagnostics=diagnostics)
        self.storage.file_manager.append_session_diagnostics_event(
            self.session_timestamp,
            mt.EVT_SENSOR_DIAGNOSTICS,
            payload,
        )
        if self.system_event_bus:
            self.system_event_bus.emit({"type": mt.EVT_SENSOR_DIAGNOSTICS, "payload": payload})

    def _on_error(self, error_msg):
        """Handle and emit error messages."""
        logger.error(f"ERROR: {error_msg}")
        self.storage.file_manager.record_session_diagnostics_error(
            self.session_timestamp,
            f"ERROR: {error_msg}",
            context=self._event_context(),
        )
        if self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": mt.EVT_ERROR,
                    "payload": {
                        "message": f"ERROR: {error_msg}",
                        **self._event_context(),
                    },
                }
            )
        self.pending_correlation_id = None
