"""Async sensor manager facade for discovery, connection, and streaming."""

import asyncio
import threading
import platform
import time
from collections import defaultdict
from types import SimpleNamespace
from typing import List

from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from nexus_n3.sensor_manager.utils import utils as utils
from nexus_n3.sensor_manager.adapter_pool import AdapterPool
from nexus_n3.sensor_manager.discovery_service import DiscoveryService
from nexus_n3.sensor_manager.connection_service import ConnectionService
from nexus_n3.sensor_manager.polling_stream_service import PollingStreamService
from nexus_n3.sensor_manager.streaming_service import StreamingService
from nexus_n3.sensor_manager.battery_precheck_service import BatteryPrecheckService
from nexus_n3.sensor_manager.sensor_controller import SensorController
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.sensor_manager.adapters.wifi.config import WifiRuntimeConfig
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


class SensorManager:
    """
    Manages pre-instantiated sensor instances with an asyncio event loop.
    Provides discovery, connection, disconnection, identification, and streaming.
    Uses callbacks to propagate events to clients (e.g., Core).
    """

    def __init__(
        self,
        system_event_bus=None,
        error_cb=None,
        ble_runtime_config: BLERuntimeConfig | None = None,
        wifi_runtime_config: WifiRuntimeConfig | None = None,
    ):
        self.logger = get_module_logger("Sensor Manager")
        self.logger.info("Initialising Sensor Manager.")
        self.system_event_bus = system_event_bus
        self.ble_runtime_config = ble_runtime_config or BLERuntimeConfig.from_env()
        self.wifi_runtime_config = wifi_runtime_config or WifiRuntimeConfig.from_env()

        # Clear any stale BLE connections on Linux
        if platform.system() == "Linux":
            self.logger.info("Clearing stale BLE connections on Linux.")
            utils.remove_all_devices()

        self.sensors: List = []
        self.sensor_meta = {}
        self.routing_table = {}

        self.listeners = {
            "on_discover": None,
            "on_connected": None,
            "on_disconnected": None,
            "on_data": None,
            "on_identify": None,
            "on_battery": None,
            "on_button": None,
            "on_stream_started": None,
            "on_stream_stopped": None,
            "on_diagnostics": None,
            "on_error": None,
        }
        self.register_listener("on_error", error_cb)

        self.loop = asyncio.new_event_loop()
        self.queue = asyncio.Queue()
        self.running = True

        self.adapter_pool = AdapterPool(
            ble_runtime_config=self.ble_runtime_config,
            wifi_runtime_config=self.wifi_runtime_config,
            diagnostics_callback=self._emit_adapter_diagnostics,
        )
        self.adapters = self.adapter_pool.adapters  # compatibility alias

        self.polling_stream_service = PollingStreamService(self.loop)
        self._stream_tasks = self.polling_stream_service._stream_tasks  # compatibility alias
        self._stream_stop_events = self.polling_stream_service._stream_stop_events  # compatibility alias

        self.discovery_service = DiscoveryService(self.adapter_pool, self.logger)
        self.connection_service = ConnectionService(self.adapter_pool, self.logger)
        self.streaming_service = StreamingService(
            adapter_pool=self.adapter_pool,
            polling_stream_service=self.polling_stream_service,
            logger=self.logger,
        )
        self.battery_precheck_service = BatteryPrecheckService(
            loop=self.loop,
            register_listeners_with_sensor=self.register_listeners_with_sensor,
            logger=self.logger,
            ble_runtime_config=self.ble_runtime_config,
        )

        self.controller = SensorController(
            sensors_ref=lambda: self.sensors,
            get_connected_sensors=self.get_connected_sensors,
            get_connected_sensor_by_address=self.get_connected_sensor_by_address,
            set_up_sensor=self.set_up_sensor,
            register_listeners_with_sensor=self.register_listeners_with_sensor,
            emit_to_client=self._emit_to_client,
            loop=self.loop,
            adapter_pool=self.adapter_pool,
            discovery_service=self.discovery_service,
            connection_service=self.connection_service,
            streaming_service=self.streaming_service,
        )

        self.thread = threading.Thread(target=self._start_loop, daemon=True)
        self.thread.start()

        # Per-sensor sample-rate logging (independent of NEXUS_PERF_LOG)
        self._rate_lock = threading.Lock()
        self._rate_interval_seconds = 5.0
        self._rate_active = False
        self._rate_counts = defaultdict(int)
        self._rate_window_start = time.monotonic()
        self._rate_thread = threading.Thread(target=self._rate_logger_loop, daemon=True)
        self._rate_thread.start()

    # ----------------- Event emission ----------------- #
    def _emit_system_event(self, msg_type, payload):
        if self.system_event_bus:
            self.system_event_bus.emit({"type": msg_type, "payload": payload})

    def _emit_to_client(self, event_name, payload):
        if event_name == "on_data":
            address = getattr(payload, "address", None)
            if not address and isinstance(payload, dict):
                address = payload.get("address")
            if address:
                with self._rate_lock:
                    self._rate_counts[address] += 1
            self._route_sensor_output(payload)

        elif event_name == "on_stream_started":
            with self._rate_lock:
                self._rate_active = True
                self._rate_counts.clear()
                self._rate_window_start = time.monotonic()

        elif event_name == "on_stream_stopped":
            self._log_sample_rates(force=True)
            with self._rate_lock:
                self._rate_active = False

        cb = self.listeners.get(event_name)
        if cb:
            cb(payload)

    def _emit_adapter_diagnostics(self, payload):
        self._emit_to_client("on_diagnostics", payload)

    def _log_sample_rates(self, force=False):
        with self._rate_lock:
            if not self._rate_active and not force:
                return

            now = time.monotonic()
            elapsed = now - self._rate_window_start
            if elapsed <= 0:
                return

            rates = {
                addr: round(count / elapsed, 3)
                for addr, count in self._rate_counts.items()
            }
            if rates or force:
                self.logger.info(
                    "sample_rate_hz interval=%.3fs rates=%s samples=%s",
                    elapsed,
                    rates,
                    dict(self._rate_counts),
                )

            self._rate_counts.clear()
            self._rate_window_start = now

    def _rate_logger_loop(self):
        while self.running:
            time.sleep(self._rate_interval_seconds)
            self._log_sample_rates(force=False)

    # ----------------- Sensor management ----------------- #
    def register_listener(self, listener_event, listener_callback):
        if listener_event not in self.listeners:
            self.logger.error("Unsupported event type: %s", listener_event)
            raise ValueError(f"Unsupported event type: {listener_event}")
        self.listeners[listener_event] = listener_callback

    def get_listener(self, listener_event):
        """Return the registered callback for an event, if any."""
        return self.listeners.get(listener_event)

    def unregister_listener(self, listener_event):
        self.listeners[listener_event] = None

    def get_connected_sensors(self):
        return [
            s
            for s in self.sensors
            if getattr(getattr(s, "connection_status", None), "name", None) == ConnectionStatus.CONNECTED.name
        ]

    def get_connected_sensor_by_address(self, address):
        return [
            s
            for s in self.sensors
            if getattr(getattr(s, "connection_status", None), "name", None) == ConnectionStatus.CONNECTED.name
            and s.address == address
        ]

    def register_listeners_with_sensor(self, sensor):
        for event_name in sensor.listeners.keys():
            if event_name in self.listeners:
                sensor.register_listener(
                    event_name,
                    lambda payload, en=event_name: self._emit_to_client(en, payload),
                )

    async def set_up_sensor(self, sensor):
        self.logger.info(
            "Setting up sensor %s with capabilities: %s",
            sensor.name,
            sensor.capabilities,
        )
        setattr(sensor, "_runtime_adapter", self.adapter_pool.for_sensor(sensor))
        if hasattr(sensor, "setup") and callable(getattr(sensor, "setup")):
            await sensor.setup(
                self.adapter_pool.for_sensor(sensor),
                enable_battery=self.listeners["on_battery"] is not None,
                enable_button=self.listeners["on_button"] is not None,
            )
        else:
            self.logger.warning("Sensor %s does not implement setup hook", sensor.name)

    # ----------------- Initialization ----------------- #
    def init_sensor_manager(self, sensors_to_init: list):
        """Initialize manager with pre-instantiated sensors and adapters."""
        self.sensors = []
        self.sensor_meta = {}
        self.routing_table = {}
        self.adapter_pool.reset()

        for entry in sensors_to_init:
            if isinstance(entry, dict):
                sensor = entry.get("sensor")
                meta = entry.get("meta", {})
            else:
                sensor = entry
                meta = {}

            if sensor is None:
                continue

            self.adapter_pool.get_or_create(sensor.adapter)
            self.register_listeners_with_sensor(sensor)

            # this conditional is redundant now everything is a plugin 
            if hasattr(sensor, "bind_manager_runtime") and callable(getattr(sensor, "bind_manager_runtime")):
                sensor.bind_manager_runtime(loop=self.loop)

            location = meta.get("location")
            if location:
                sensor.set_location(location)

            self.sensors.append(sensor)
            self.sensor_meta[sensor] = meta

        self.adapters = self.adapter_pool.adapters
        self.routing_table = self._build_routing_table()
        if (
            self.adapter_pool.requires_initialization()
            or self.adapter_pool.requires_async_shutdown()
        ):
            self.loop.call_soon_threadsafe(
                self.queue.put_nowait,
                {"message": "__initialize_adapters__"},
            )

    # ----------------- Event loop ----------------- #
    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._manager_loop())
        self.logger.info("Sensor Manager event loop thread stopped.")

    async def _manager_loop(self):
        while self.running:
            msg = await self.queue.get()
            try:
                if msg.get("message") == "__stop__":
                    break
                try:
                    if msg.get("message") == "__initialize_adapters__":
                        await self.adapter_pool.initialize_all()
                        handled = True
                    else:
                        handled = await self.controller.dispatch(msg)
                except Exception as exc:
                    command_name = msg.get("message", "unknown")
                    error_msg = f"{command_name} failed: {type(exc).__name__}: {exc}"
                    self.logger.exception("Sensor manager command failed: %s", command_name)
                    self._emit_to_client("on_error", error_msg)
                    handled = True
                if not handled:
                    self.logger.warning(
                        "Unknown sensor manager message: %s",
                        msg.get("message"),
                    )
            finally:
                self.queue.task_done()

    # ----------------- Client-facing APIs ----------------- #
    def discover(self):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, {"message": "discover"})

    def discover_for_subject(self, sensors):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "discover_for_subject", "sensors": sensors},
        )

    def discover_and_connect(self):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "discover_and_connect"},
        )

    def connect_all(self):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, {"message": "connect_all"})

    def connect_specific_sensors(self, addresses: List):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "connect_specific_sensors", "addresses": addresses},
        )

    def disconnect_all(self):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "disconnect_all"},
        )

    def disconnect_addresses(self, addresses: List):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "disconnect_addresses", "addresses": addresses},
        )

    def identify(self, address: str):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "identify", "address": address},
        )

    def start_all(self):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, {"message": "start_all"})

    def start_specific_sensors(self, addresses: List):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "start_specific_sensors", "addresses": addresses},
        )

    def stop_all(self):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, {"message": "stop_all"})

    def stop_specific_sensors(self, addresses: List):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "stop_specific_sensors", "addresses": addresses},
        )

    async def check_battery_preinit(
        self,
        sensor_classes,
        scan_timeout: float = 5.0,
        read_timeout: float = 10.0,
    ):
        """Probe battery-capable sensors before full manager initialisation.

        This supports workflows that need a quick battery-status check before
        constructing the normal discovery/connect/stream runtime.
        """
        return await self.battery_precheck_service.check_battery_preinit(
            sensor_classes=sensor_classes,
            scan_timeout=scan_timeout,
            read_timeout=read_timeout,
        )

    async def collect_transport_diagnostics(self):
        """Collect transport/backend diagnostics from active adapters."""
        return await self.adapter_pool.collect_diagnostics()

    def stop_manager(self):
        """Stop streaming and adapters before stopping the manager loop."""
        if not self.running:
            return
        try:
            asyncio.run_coroutine_threadsafe(self.streaming_service.shutdown(), self.loop).result(timeout=3)
        except Exception:
            pass
        if self.adapter_pool.requires_async_shutdown():
            try:
                asyncio.run_coroutine_threadsafe(
                    self.adapter_pool.shutdown_all(),
                    self.loop,
                ).result(timeout=30)
            except Exception:
                self.adapter_pool.close_all()
        self.running = False
        try:
            self.loop.call_soon_threadsafe(self.queue.put_nowait, {"message": "__stop__"})
        except Exception:
            pass
        self.thread.join(timeout=3)
        for sensor in list(self.sensors):
            if hasattr(sensor, "close_host") and callable(getattr(sensor, "close_host")):
                try:
                    sensor.close_host()
                except Exception:
                    pass
        self.adapter_pool.close_all()

    def _build_routing_table(self):
        """Pre-compute compatible source->target plugin routes.

        A route exists when one sensor declares outputs that are compatible with
        another sensor/plugin's declared inputs. This keeps routing decisions
        cheap while samples are streaming.
        """
        routing = {}
        source_outputs = {sensor: self._routing_outputs_for_sensor(sensor) for sensor in self.sensors}
        target_inputs = {sensor: self._routing_inputs_for_sensor(sensor) for sensor in self.sensors}
        for source in self.sensors:
            outputs = source_outputs.get(source) or []
            if not outputs:
                continue
            routes = []
            for target in self.sensors:
                if target is source:
                    continue
                inputs = target_inputs.get(target) or []
                if not inputs:
                    continue
                if self._routes_compatible(outputs, inputs):
                    routes.append(target)
            if routes:
                routing[source] = routes
        return routing

    def _route_sensor_output(self, payload):
        """Forward one emitted sensor payload to compatible downstream plugins.

        Routing only occurs for `on_data` payloads that can be mapped back to a
        source sensor by address. Payloads are wrapped in a small envelope so
        downstream plugins receive consistent metadata about the producer.
        """
        source = self._find_sensor_for_payload(payload)
        if source is None:
            return
        routes = self.routing_table.get(source) or []
        if not routes:
            return
        payload_outputs = self._payload_outputs_for_sensor(source, payload)
        if not payload_outputs:
            return
        envelope = self._build_routing_envelope(source, payload, payload_outputs[0])
        source_plugin_id = envelope.source_plugin_id
        for target in routes:
            if not self._routes_compatible(payload_outputs, self._routing_inputs_for_sensor(target)):
                continue
            try:
                result = target.consume_input(source_plugin_id, envelope)

                if asyncio.iscoroutine(result):
                    print(
                        "[ROUTING_ASYNC]",
                        "source=", getattr(source, "name", source),
                        "target=", getattr(target, "name", target),
                        "address=", getattr(payload, "address", None),
                        "timestamp=", getattr(payload, "timestamp", None),
                        flush=True,
                    )

                    try:
                        running_loop = asyncio.get_running_loop()
                    except RuntimeError:
                        running_loop = None

                    if running_loop is self.loop:
                        running_loop.create_task(result)
                    else:
                        asyncio.run_coroutine_threadsafe(result, self.loop).result(timeout=30.0)
            except Exception as exc:    
                self.logger.exception(
                    "Error routing output from %s to %s: %s",
                    getattr(source, "name", source),
                    getattr(target, "name", target),
                    exc,
                )

    def _find_sensor_for_payload(self, payload):
        """Resolve the originating sensor instance from a routed payload."""
        address = getattr(payload, "address", None)
        if address is None and isinstance(payload, dict):
            address = payload.get("address")
        if not address:
            return None
        return next((sensor for sensor in self.sensors if getattr(sensor, "address", None) == address), None)

    @staticmethod
    def _routes_compatible(outputs, inputs):
        output_names = {str(item.get("name") or "").strip().lower() for item in outputs if item.get("name")}
        output_schemas = {str(item.get("schema") or "").strip().lower() for item in outputs if item.get("schema")}
        for item in inputs or []:
            input_name = str(item.get("name") or "").strip().lower()
            input_schema = str(item.get("schema") or "").strip().lower()
            if input_name and input_name in output_names:
                return True
            if input_schema and input_schema in output_schemas:
                return True
        return False

    def _routing_inputs_for_sensor(self, sensor):
        """Return normalized routing input declarations for one sensor/plugin."""
        inputs = getattr(sensor, "routing_inputs", None)
        if inputs is not None:
            return list(inputs)
        spec = getattr(sensor, "spec", {}) or {}
        return self._normalize_routing_entries(spec.get("inputs") or [])

    def _routing_outputs_for_sensor(self, sensor):
        """Return normalized routing output declarations for one sensor/plugin.

        If the sensor does not declare explicit routing outputs, derive them
        from `spec["data_streams"]` so routing can still work from the sensor
        specification alone.
        """
        outputs = getattr(sensor, "routing_outputs", None)
        if outputs:
            return list(outputs)
        spec = getattr(sensor, "spec", {}) or {}
        data_streams = (spec.get("data_streams") or {})
        entries = []
        for stream_name, stream_meta in data_streams.items():
            if not isinstance(stream_meta, dict):
                continue
            sample_type = str(stream_meta.get("sample_type") or "").strip()
            entries.append(
                {
                    "name": str(stream_name),
                    "schema": self._normalize_schema_name(sample_type) if sample_type else None,
                }
            )
        return entries

    def _payload_outputs_for_sensor(self, sensor, payload):
        """Resolve which declared output best matches a concrete payload.

        This narrows routing when a sensor exposes multiple stream types and the
        emitted payload advertises a `sample_type`.
        """
        outputs = list(self._routing_outputs_for_sensor(sensor))
        sample_type = getattr(payload, "sample_type", None)
        if sample_type is None and isinstance(payload, dict):
            sample_type = payload.get("sample_type")
        sample_type_key = str(sample_type or "").strip().lower()
        if not sample_type_key:
            return outputs
        matched = [
            item
            for item in outputs
            if str(item.get("schema") or "").strip().lower() == sample_type_key
            or str(item.get("name") or "").strip().lower() == sample_type_key
        ]
        return matched or outputs

    def _build_routing_envelope(self, source, payload, output):
        """Build the object passed into `consume_input` on downstream plugins.

        Dictionary payloads are converted to nested namespaces so consumers can
        use attribute access (`payload.foo.bar`) in the same way they would for
        sample-model objects.
        """
        payload_value = payload
        if isinstance(payload, dict):
            payload_value = self._normalize_routed_payload(payload)
        return SimpleNamespace(
            source_plugin_id=self._sensor_plugin_id(source),
            source_sensor_name=getattr(source, "name", None),
            source_address=getattr(source, "address", None),
            output_name=output.get("name"),
            schema=output.get("schema"),
            payload=payload_value,
        )

    @staticmethod
    def _sensor_plugin_id(sensor):
        """Return a stable identifier for the source plugin/sensor."""
        plugin_id = getattr(sensor, "plugin_id", None)
        if plugin_id:
            return str(plugin_id)
        return f"{sensor.__class__.__module__}:{sensor.__class__.__name__}"

    @staticmethod
    def _normalize_routing_entries(values):
        """Normalize routing declarations into `{name, schema}` dictionaries."""
        entries = []
        for value in values or []:
            if isinstance(value, str):
                text = value.strip()
                if text:
                    entries.append({"name": text, "schema": None})
                continue
            if not isinstance(value, dict):
                continue
            name = str(value.get("name") or "").strip() or None
            schema = SensorManager._normalize_schema_name(value.get("schema"))
            if name or schema:
                entries.append({"name": name, "schema": schema})
        return entries

    @staticmethod
    def _normalize_schema_name(value):
        """Canonicalize schema names for loose routing compatibility checks."""
        text = str(value or "").strip().lower()
        if text.endswith("sample"):
            text = text[:-6]
        return text or None

    @classmethod
    def _normalize_routed_payload(cls, value):
        """Recursively convert dict/list payloads into attribute-addressable objects.

        This is needed because routed payloads can originate either as rich
        sample objects or as plain dictionaries. Converting dictionaries into
        nested `SimpleNamespace` instances lets downstream plugin code treat
        both forms consistently with attribute access instead of having to mix
        `payload.foo` and `payload["foo"]` styles.
        """
        if isinstance(value, dict):
            return SimpleNamespace(**{key: cls._normalize_routed_payload(val) for key, val in value.items()})
        if isinstance(value, list):
            return [cls._normalize_routed_payload(item) for item in value]
        return value
