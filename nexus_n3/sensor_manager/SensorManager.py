"""Async sensor manager facade for discovery, connection, and streaming."""

import asyncio
import threading
import platform
import time
from collections import defaultdict
from types import SimpleNamespace
from typing import List
from concurrent.futures import Future, ThreadPoolExecutor

from nexus_n3.core.subject_connection_sequence import connection_step_capture
from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from nexus_n3.sensor_manager.utils import utils as utils
from nexus_n3.sensor_manager.adapter_pool import AdapterPool
from nexus_n3.sensor_manager.discovery_service import DiscoveryService
from nexus_n3.sensor_manager.connection_service import ConnectionService
from nexus_n3.sensor_manager.polling_stream_service import PollingStreamService
from nexus_n3.sensor_manager.streaming_service import StreamingService, PHYSICAL_STOP_TIMEOUT_S
from nexus_n3.plugins.common.operations import SensorOperation, current_operation, operation_scope
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

        self._routing_condition = threading.Condition()
        self._routing_enabled = False
        self._active_routed_calls = 0
        # One scheduled/running job and one replaceable value per route.
        # All access is protected by _routing_condition.
        self._routing_pending = {}
        self._routing_blocked_addresses = set()
        self._routing_executors = {}
        self._routing_operations = {}
        self._routing_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="nexus-routing",
        )

        self.listeners = {
            "on_discover": None,
            "on_connected": None,
            "on_disconnected": None,
            "on_cleanup_complete": None,
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
        self.recovery_obligations = {}
        self.connection_service.recovery_obligations = self.recovery_obligations
        self._recovery_future = None
        self._recovery_request_lock = threading.Lock()
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
        capture = connection_step_capture.get()
        if capture is not None and event_name in {"on_discover", "on_connected", "on_error"}:
            capture[event_name] = payload
            return
        if event_name == "on_cleanup_complete":
            for result in payload.get("results", []):
                if result.get("status") in ("failed", "unconfirmed"):
                    sensor = next((sensor for sensor in self.sensors if sensor.address == result.get("address")), None)
                    if sensor is not None:
                        self.recovery_obligations[str(sensor.address).upper()] = sensor
        if event_name == "on_data":
            address = getattr(payload, "address", None)
            if not address and isinstance(payload, dict):
                address = payload.get("address")
            sensor = self._find_sensor_for_payload(payload)
            if sensor is not None and getattr(sensor, "physical_stop_quarantined", False):
                return
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
                self._rate_active = self._routing_enabled and any(
                    getattr(sensor, "measurement_cleanup_pending", False)
                    and sensor.address not in self._routing_blocked_addresses
                    for sensor in self.sensors
                )

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
            and not getattr(s, "physical_stop_quarantined", False)
            and str(s.address).upper() not in self.recovery_obligations
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
        sensor.connection_setup_complete = False
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
            capture = connection_step_capture.get()
            if capture is not None and capture.get("on_error"):
                raise RuntimeError(str(capture["on_error"]))
            sensor.connection_setup_complete = True
        else:
            self.logger.warning("Sensor %s does not implement setup hook", sensor.name)

    # ----------------- Initialization ----------------- #
    def init_sensor_manager(self, sensors_to_init: list):
        """Install a sensor configuration, retiring any previous one first.

        Reconfiguration is serialized on the manager loop so that queued
        stop/disconnect work finishes before the live sensor and adapter
        references are replaced. The first configuration can be installed
        directly because there is no previous runtime state to retire.
        """

        if self.sensors or self.adapter_pool.adapters:
            if threading.current_thread() is self.thread:
                raise RuntimeError(
                    "Sensor manager reinitialization cannot block its event-loop thread"
                )

            self._quiesce_routing()
            completion = Future()
            self.loop.call_soon_threadsafe(
                self.queue.put_nowait,
                {
                    "message": "__reinitialize_sensors__",
                    "sensors": list(sensors_to_init),
                    "deadline": time.monotonic() + 55,
                    "_completion": completion,
                },
            )
            completion.result(timeout=60)
            return

        try:
            self._install_sensor_configuration(sensors_to_init)
            self._queue_adapter_initialization_if_needed()
        except Exception:
            self.sensors = []
            self.sensor_meta = {}
            self.routing_table = {}
            raise

    async def _reinitialize_sensor_manager(self, sensors_to_init: list):
        """Dispose the old runtime in transport-safe order, then install new sensors."""

        previous_sensors = list(self.sensors)
        for sensor in previous_sensors:
            if sensor.address and (getattr(sensor, "measurement_cleanup_pending", False)
                                   or getattr(sensor, "physical_stop_quarantined", False)):
                self.recovery_obligations.setdefault(str(sensor.address).upper(), sensor)
        previous_meta = self.sensor_meta
        previous_routing = self.routing_table
        previous_cleanup_flags = [sensor.cleanup_in_progress for sensor in previous_sensors]
        for sensor in previous_sensors:
            sensor.cleanup_in_progress = True
        try:
            await asyncio.to_thread(self._wait_for_routing_to_drain)
            await self.streaming_service.shutdown()

            connected_sensors = [
                sensor
                for sensor in previous_sensors
                if getattr(getattr(sensor, "connection_status", None), "name", None)
                == ConnectionStatus.CONNECTED.name
                and str(sensor.address).upper() not in self.recovery_obligations
            ]
            if connected_sensors:
                await self.connection_service.disconnect(
                    sensors_to_disconnect=connected_sensors,
                    emit_to_client=lambda _event, _payload: None,
                    disconnected_status=ConnectionStatus.DISCONNECTED,
                )

            for sensor in previous_sensors:
                for event_name in list(getattr(sensor, "listeners", {})):
                    try:
                        sensor.unregister_listener(event_name)
                    except Exception:
                        pass

                if str(sensor.address).upper() in self.recovery_obligations:
                    # Detach recording listeners, retaining the original host,
                    # transport identity and measurement STOP obligation.
                    continue

                if hasattr(sensor, "close_host") and callable(getattr(sensor, "close_host")):
                    try:
                        await asyncio.to_thread(sensor.close_host)
                    except Exception:
                        self.logger.exception(
                            "Failed to close plugin host for sensor %s",
                            getattr(sensor, "name", type(sensor).__name__),
                        )

                callbacks = getattr(sensor, "_adapter_callbacks", None)
                if isinstance(callbacks, dict):
                    callbacks.clear()
                sensor.set_transport_client(None)
                if hasattr(sensor, "_runtime_adapter"):
                    sensor._runtime_adapter = None
                if hasattr(sensor, "_manager_loop"):
                    sensor._manager_loop = None

            operation = current_operation.get()
            if operation is not None:
                operation.check()
            self.sensors = []
            self.sensor_meta = {}
            self.routing_table = {}
            for executor in set(self._routing_executors.values()) - {self._routing_executor}:
                executor.shutdown(wait=True)
            self._routing_executors.clear()
            preserve = {key for key, adapter in self.adapter_pool.adapters.items()
                        if getattr(adapter, "gateway_client", None) is not None}
            preserve.update(str(sensor.adapter).upper() for sensor in self.recovery_obligations.values()
                            if sensor is not None)
            self.adapter_pool.reset(preserve_types=preserve)

            self._install_sensor_configuration(sensors_to_init)
            # Await cleanup/initialization here instead of merely queueing it. The
            # completion returned to Core is the reconfiguration barrier.
            await self.adapter_pool.initialize_all()
            if operation is not None:
                operation.check()
        except (Exception, asyncio.CancelledError):
            if current_operation.get() is None or not current_operation.get().cancelled.is_set():
                self.sensors = previous_sensors
                self.sensor_meta = previous_meta
                self.routing_table = previous_routing
            raise
        finally:
            # Failed retirement must not leave the old configuration permanently
            # excluded from discovery, writes, or a subsequent cleanup attempt.
            for sensor, previous_flag in zip(previous_sensors, previous_cleanup_flags):
                sensor.cleanup_in_progress = previous_flag


    def _install_sensor_configuration(self, sensors_to_init: list):
        """Populate manager state from pre-instantiated sensors."""

        for entry in sensors_to_init:
            if isinstance(entry, dict):
                sensor = entry.get("sensor")
                meta = entry.get("meta", {})
            else:
                sensor = entry
                meta = {}

            if sensor is None:
                continue

            sensor.cleanup_in_progress = False
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

    def _queue_adapter_initialization_if_needed(self):
        """Queue first-use adapter initialization behind earlier manager work."""

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
            completion = msg.get("_completion")
            capture = {} if msg.get("connection_sequence_step") else None
            capture_token = connection_step_capture.set(capture)

            try:
                if msg.get("message") == "__stop__":
                    if completion is not None and not completion.done():
                        completion.set_result(True)
                    break

                try:
                    if msg.get("message") == "__reinitialize_sensors__":
                        operation = SensorOperation(msg.get("deadline", time.monotonic() + 55))
                        previous = (self.sensors, self.sensor_meta, self.routing_table)
                        try:
                            await self.controller.cleanup_service._bounded(
                                self._reinitialize_sensor_manager(msg.get("sensors", [])), operation,
                            )
                        except Exception:
                            self.sensors, self.sensor_meta, self.routing_table = previous
                            raise
                        handled = True
                    elif msg.get("message") == "__initialize_adapters__":
                        await self.adapter_pool.initialize_all()
                        handled = True
                    elif msg.get("message") == "__recover_sensors__":
                        handled = await self._recover_sensors(msg)
                    else:
                        routing_error = None
                        if msg.get("message") in ("stop_all", "stop_specific_sensors"):
                            try:
                                await asyncio.to_thread(
                                    self._wait_for_routing_to_drain,
                                    msg.get("addresses") if msg["message"] == "stop_specific_sensors" else None,
                                )
                            except TimeoutError as exc:
                                routing_error = str(exc)
                        if routing_error:
                            targets = [sensor for sensor in self.sensors
                                       if msg["message"] == "stop_all" or sensor.address in msg["addresses"]]
                            for sensor in targets:
                                sensor.physical_stop_quarantined = True
                            handled = [{"address": sensor.address, "status": "not_attempted", "error": routing_error}
                                       for sensor in targets]
                        else:
                            handled = await self.controller.dispatch(msg)

                except Exception as exc:
                    command_name = msg.get("message", "unknown")
                    error_msg = f"{command_name} failed: {type(exc).__name__}: {exc}"

                    self.logger.exception(
                        "Sensor manager command failed: %s",
                        command_name,
                    )

                    if completion is not None and not completion.done():
                        completion.set_exception(exc)
                    else:
                        self._emit_to_client("on_error", error_msg)

                    handled = True

                if msg.get("message") in ("stop_all", "stop_specific_sensors") and isinstance(handled, list):
                    for result in handled:
                        if result["status"] != "stopped":
                            sensor = next((sensor for sensor in self.sensors if sensor.address == result["address"]), None)
                            if sensor is not None:
                                self.recovery_obligations[str(sensor.address).upper()] = sensor

                if handled is False:
                    command_name = msg.get("message", "unknown")
                    self.logger.warning(
                        "Unknown sensor manager message: %s",
                        command_name,
                    )

                    if completion is not None and not completion.done():
                        completion.set_exception(
                            RuntimeError(
                                f"Unknown sensor manager command: {command_name}"
                            )
                        )

                elif completion is not None and not completion.done():
                    completion.set_result(capture if capture is not None else
                                          handled if isinstance(handled, (list, dict)) else True)

            finally:
                connection_step_capture.reset(capture_token)
                self.queue.task_done()

    def submit_connection_step(self, message, **arguments):
        """Return terminal completion for one existing controller operation."""
        completion = Future()
        self.loop.call_soon_threadsafe(self.queue.put_nowait, {
            "message": message, **arguments, "connection_sequence_step": True,
            "_completion": completion,
        })
        return completion

    # ----------------- Client-facing APIs ----------------- #
    def discover(self):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, {"message": "discover"})

    def discover_for_subject(self, sensors, excluded_addresses=()):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "discover_for_subject", "sensors": sensors, "excluded_addresses": list(excluded_addresses)},
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

    def disconnect_all(self, cleanup_context=None):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "disconnect_all", "cleanup_context": cleanup_context or {}},
        )

    def disconnect_addresses(self, addresses: List, cleanup_context=None):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "disconnect_addresses", "addresses": addresses, "cleanup_context": cleanup_context or {}},
        )

    def identify(self, address: str):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "identify", "address": address},
        )

    def enable_routing(self):
        self._enable_routing()

    def disable_routing(self):
        self._quiesce_routing()

    def start_all(self):
        #self._enable_routing()
        self.loop.call_soon_threadsafe(self.queue.put_nowait, {"message": "start_all"})

    def reset_session_diagnostics(self):
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "reset_session_diagnostics"},
        )

    def start_specific_sensors(self, addresses: List):
        #self._enable_routing()
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {"message": "start_specific_sensors", "addresses": addresses},
        )

    def stop_all(self):
        self._quiesce_routing()

        completion = Future()

        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {
                "message": "stop_all",
                "stop_deadline": time.monotonic() + PHYSICAL_STOP_TIMEOUT_S,
                "_completion": completion,
            },
        )

        return completion

    def stop_specific_sensors(self, addresses: List):
        self._quiesce_routing(addresses)

        completion = Future()

        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            {
                "message": "stop_specific_sensors",
                "stop_deadline": time.monotonic() + PHYSICAL_STOP_TIMEOUT_S,
                "addresses": addresses,
                "_completion": completion,
            },
        )

        return completion

    def recover_sensors(self, addresses=None, *, allow_ble_reset=False, deadline=None,
                        confirmed_stopped_addresses=()):
        """Serialize bounded recovery with manager commands; coalesce Reset."""
        with self._recovery_request_lock:
            if self._recovery_future is not None and not self._recovery_future.done():
                if addresses != self._recovery_addresses:
                    raise RuntimeError("Recovery for a different sensor scope is already in progress")
                return self._recovery_future
            requested = None if addresses is None else {str(a).upper() for a in addresses}
            active = {str(address).upper() for adapter in self.adapter_pool.adapters.values()
                      for address in getattr(getattr(adapter, "gateway_client", None), "active_acquisition_addresses", set())}
            if active and (requested is None or active & requested):
                raise RuntimeError("Stop targeted acquisition before sensor recovery")
            self._quiesce_routing(addresses)
            self._recovery_addresses = addresses
            completion = self._recovery_future = Future()
            self.loop.call_soon_threadsafe(self.queue.put_nowait, {
                "message": "__recover_sensors__", "addresses": addresses,
                "deadline": min(deadline or float("inf"), time.monotonic() + 80), "_completion": completion,
                "allow_ble_reset": allow_ble_reset,
                "confirmed_stopped_addresses": list(confirmed_stopped_addresses),
            })
            return completion

    async def _recover_sensors(self, msg):
        requested = None if msg["addresses"] is None else {str(a).upper() for a in msg["addresses"]}
        targets = {str(sensor.address).upper(): sensor for sensor in self.sensors if sensor.address
                   and (requested is None or str(sensor.address).upper() in requested)}
        targets.update({address: sensor for address, sensor in self.recovery_obligations.items()
                        if requested is None or address in requested})
        active = {str(address).upper() for adapter in self.adapter_pool.adapters.values()
                  for address in getattr(getattr(adapter, "gateway_client", None), "active_acquisition_addresses", set())}
        payload = await self.controller.cleanup_service.reconcile(
            [sensor for sensor in targets.values() if sensor is not None],
            deadline=msg["deadline"], active_addresses=active,
            allow_ble_reset=msg.get("allow_ble_reset", False),
            confirmed_stopped_addresses=msg.get("confirmed_stopped_addresses", ()),
        )
        for result in payload["results"]:
            address = str(result["address"]).upper()
            if result["eligible_for_acquisition"]:
                self.recovery_obligations.pop(address, None)
                retired = targets[address]
                if retired not in self.sensors and hasattr(retired, "close_host"):
                    operation = SensorOperation(min(msg["deadline"], time.monotonic() + 5))
                    try:
                        await self.controller.cleanup_service._bounded(asyncio.to_thread(retired.close_host), operation)
                    except Exception:
                        self.logger.exception("Retired recovered host could not close: %s", address)
            else:
                self.recovery_obligations[address] = targets[address]
        # Inventory links with no retained plugin context must not be silently
        # reused as if a fresh, empty host had confirmed their physical STOP.
        for gateway in payload["gateways"]:
            for address in gateway.get("connected_addresses", []):
                if address not in targets and address not in active:
                    self.recovery_obligations.setdefault(address, None)
        confirmed = {str(a).upper() for a in msg.get("confirmed_stopped_addresses", ())}
        verified_reset = msg.get("allow_ble_reset") is True and any(
            gateway.get("available") and gateway.get("reset_confirmed")
            and not gateway.get("connected_addresses") for gateway in payload["gateways"])
        for address, sensor in list(self.recovery_obligations.items()):
            if sensor is None and (requested is None or address in requested):
                if address in confirmed and verified_reset:
                    self.recovery_obligations.pop(address)
                    payload["results"].append({"address": address, "status": "cleaned",
                        "measurement_stop_confirmed": True, "eligible_for_acquisition": True,
                        "link_connected": False, "confirmation_source": "operator"})
                    continue
                payload["results"].append({"address": address, "status": "unconfirmed",
                    "measurement_stop_confirmed": False, "eligible_for_acquisition": False,
                    "error": "No retained plugin context; physical STOP requires operator intervention"})
        return {**payload, "core_ready_for_configuration": not bool(active),
                "gateway_communication_available": all(g["available"] for g in payload["gateways"]),
                "recovery_complete": not self.recovery_obligations}

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

        for sensor in self.sensors:
            sensor.cleanup_in_progress = True
        self._quiesce_routing()
        self._wait_for_routing_to_drain()

        try:
            asyncio.run_coroutine_threadsafe(
                self.streaming_service.shutdown(),
                self.loop,
            ).result(timeout=3)
        except Exception:
            pass

        if self.adapter_pool.requires_async_shutdown():
            try:
                asyncio.run_coroutine_threadsafe(
                    self.adapter_pool.shutdown_all(),
                    self.loop,
                ).result(timeout=30)
            except Exception:
                pass

        self.running = False

        try:
            self.loop.call_soon_threadsafe(
                self.queue.put_nowait,
                {"message": "__stop__"},
            )
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
        for executor in set(self._routing_executors.values()) | {self._routing_executor}:
            executor.shutdown(wait=True)

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

    def _execute_routed_call(
        self,
        *,
        source,
        target,
        source_plugin_id: str,
        envelope,
        payload,
    ) -> None:
        try:
            started = time.monotonic()

            print(
                "[ROUTE_BEGIN]",
                "source=", getattr(source, "name", source),
                "target=", getattr(target, "name", target),
                "sample_type=", getattr(payload, "sample_type", None),
                "address=", getattr(payload, "address", None),
                "timestamp=", getattr(payload, "timestamp", None),
                flush=True,
            )

            with operation_scope(self._routing_operations[(source, target)]):
                result = target.consume_input(source_plugin_id, envelope)
                if asyncio.iscoroutine(result):
                    asyncio.run(result)

            elapsed = time.monotonic() - started

            print(
                "[ROUTE_END]",
                "source=", getattr(source, "name", source),
                "target=", getattr(target, "name", target),
                "sample_type=", getattr(payload, "sample_type", None),
                "elapsed_ms=", round(elapsed * 1000.0, 3),
                flush=True,
            )

        except Exception as exc:
            self.logger.exception(
                "Error routing output from %s to %s: %s",
                getattr(source, "name", source),
                getattr(target, "name", target),
                exc,
            )

    def _executor_for_route(self, route):
        # Independent recording subjects must not share a blocked consumer
        # worker. Unlabelled graphs retain the existing single-worker ordering.
        source, target = route
        owner = (self.sensor_meta.get(target, {}).get("subject_id")
                 or self.sensor_meta.get(source, {}).get("subject_id"))
        if owner not in self._routing_executors:
            self._routing_executors[owner] = (self._routing_executor if not self._routing_executors
                                             else ThreadPoolExecutor(max_workers=1, thread_name_prefix="nexus-routing"))
        return self._routing_executors[owner]

    def _submit_routed_value(self, source, target, call):
        """Coalesce before submission, including jobs waiting for the worker."""
        route = (source, target)
        with self._routing_condition:
            if not self._routing_enabled or any(getattr(sensor, "address", None) in self._routing_blocked_addresses
                                                for sensor in route):
                return
            if route in self._routing_pending:
                self._routing_pending[route] = call
                return

            self._routing_pending[route] = call
            self._active_routed_calls += 1
            try:
                self._executor_for_route(route).submit(self._run_routed_route, route)
            except Exception:
                self._finish_routed_route(route)
                raise

    def _run_routed_route(self, route):
        """Consume the latest value, then yield the worker to other routes."""
        with self._routing_condition:
            call = self._routing_pending[route]
            self._routing_pending[route] = None
            if any(getattr(sensor, "address", None) in self._routing_blocked_addresses for sensor in route):
                call = None
            if call is not None:
                self._routing_operations[route] = SensorOperation(time.monotonic() + 30)

        try:
            if call is not None:
                self._execute_routed_call(**call)
        finally:
            with self._routing_condition:
                operation = self._routing_operations.pop(route, None)
                if operation is not None:
                    operation.cancelled.set()
                if self._routing_pending[route] is not None:
                    try:
                        self._executor_for_route(route).submit(self._run_routed_route, route)
                    except Exception:
                        self._finish_routed_route(route)
                        raise
                else:
                    self._finish_routed_route(route)

    def _finish_routed_route(self, route):
        """Retire a route job while holding _routing_condition."""
        del self._routing_pending[route]
        self._active_routed_calls -= 1
        self._routing_condition.notify_all()

    def _route_sensor_output(self, payload):
        """Forward one emitted sensor payload to compatible downstream plugins.

        Routing only occurs for `on_data` payloads that can be mapped back to a
        source sensor by address. Payloads are wrapped in a small envelope so
        downstream plugins receive consistent metadata about the producer.
        Each source/target route keeps only the latest pending value before
        executor submission; intermediate values are intentionally dropped.
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
            if not self._routes_compatible(
                payload_outputs,
                self._routing_inputs_for_sensor(target),
            ):
                continue

            self._submit_routed_value(
                source,
                target,
                dict(
                    source=source,
                    target=target,
                    source_plugin_id=source_plugin_id,
                    envelope=envelope,
                    payload=payload,
                ),
            )

    def _find_sensor_for_payload(self, payload):
        """Resolve the originating sensor instance from a routed payload."""
        address = getattr(payload, "address", None)
        if address is None and isinstance(payload, dict):
            address = payload.get("address")
        if not address:
            return None
        return next((sensor for sensor in self.sensors if getattr(sensor, "address", None) == address), None)

    def _quiesce_routing(self, addresses=None):
        """Block only affected routes for subject STOP; all routes for full STOP."""
        with self._routing_condition:
            if addresses is None:
                self._routing_enabled = False
            else:
                self._routing_blocked_addresses.update(addresses)
            for route in self._routing_pending:
                if addresses is None or any(getattr(sensor, "address", None) in self._routing_blocked_addresses
                                            for sensor in route):
                    self._routing_pending[route] = None
                    operation = self._routing_operations.get(route)
                    if operation is not None:
                        operation.cancelled.set()

    def _wait_for_routing_to_drain(self, addresses=None):
        with self._routing_condition:
            def drained():
                return not any(addresses is None or any(getattr(sensor, "address", None) in addresses
                                                       for sensor in route)
                               for route in self._routing_pending)
            if not self._routing_condition.wait_for(drained, timeout=5):
                raise TimeoutError("Targeted sensor routing did not drain")

    def _enable_routing(self):
        with self._routing_condition:
            self._routing_enabled = True
            self._routing_blocked_addresses.clear()
        self.update_acquisition_scope([sensor.address for sensor in self.get_connected_sensors()])

    def update_acquisition_scope(self, addresses):
        for adapter in self.adapter_pool.adapters.values():
            client = getattr(adapter, "gateway_client", None)
            if client is not None:
                client.set_acquisition_scope(addresses)

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
