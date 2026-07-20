"""Sensor manager coordination and callback wiring."""

import asyncio

from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.logger.logger import get_module_logger

logger = get_module_logger("SensorOrchestrator")


class SensorOrchestrator:
    """Owns SensorManager lifecycle and callback registration."""

    def __init__(
        self,
        system_event_bus=None,
        error_cb=None,
        ble_runtime_config: BLERuntimeConfig | None = None,
    ):
        self.manager = SensorManager(
            system_event_bus,
            error_cb,
            ble_runtime_config=ble_runtime_config,
        )

    def init_sensor_manager(self, sensors):
        logger.info("Initialising Sensor Manager with %s sensors", sensors)
        self.manager.init_sensor_manager(sensors)

    def register_callbacks(self, callbacks):
        for name, cb in callbacks.items():
            self.manager.register_listener(name, cb)

    def register_listener(self, name, callback):
        """Register a single sensor-manager listener callback."""
        self.manager.register_listener(name, callback)

    def get_listener(self, name):
        """Return the current listener callback for a manager event."""
        return self.manager.get_listener(name)

    def discover(self):
        self.manager.discover()

    def discover_for_subject(self, sensors):
        self.manager.discover_for_subject(sensors=sensors)

    def connect_all(self):
        self.manager.connect_all()

    def connect_specific(self, addresses):
        self.manager.connect_specific_sensors(addresses)

    def disconnect_all(self):
        self.manager.disconnect_all()

    def disconnect_addresses(self, addresses):
        self.manager.disconnect_addresses(addresses)

    def start_all(self):
        self.manager.start_all()

    def start_specific(self, addresses):
        self.manager.start_specific_sensors(addresses)

    def stop_all(self):
        self.manager.stop_all()

    def stop_specific(self, addresses):
        self.manager.stop_specific_sensors(addresses)

    def identify(self, address):
        self.manager.identify(address)

    def stop_manager(self):
        self.manager.stop_manager()

    def check_battery_preinit(self, sensor_classes, scan_timeout=5.0, read_timeout=10.0):
        """
        Run a pre-init BLE battery check using the manager's event loop.

        Returns a concurrent.futures.Future with a dict payload:
            {"sensors": [...], "errors": {...}}
        """
        return asyncio.run_coroutine_threadsafe(
            self.manager.check_battery_preinit(
                sensor_classes=sensor_classes,
                scan_timeout=scan_timeout,
                read_timeout=read_timeout,
            ),
            self.manager.loop,
        )

    def collect_transport_diagnostics(self):
        """Return a future with adapter diagnostics snapshots."""
        return asyncio.run_coroutine_threadsafe(
            self.manager.collect_transport_diagnostics(),
            self.manager.loop,
        )
