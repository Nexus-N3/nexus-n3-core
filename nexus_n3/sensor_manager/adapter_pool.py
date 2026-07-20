"""Adapter creation and grouping utilities."""

from collections import defaultdict

from nexus_n3.sensor_manager.adapter_registry import resolve_adapter_class
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


class AdapterPool:
    """Owns adapter instances and grouping logic."""

    def __init__(
        self,
        ble_runtime_config: BLERuntimeConfig | None = None,
        diagnostics_callback=None,
    ):
        self.adapters = {}
        self.ble_runtime_config = ble_runtime_config or BLERuntimeConfig.from_env()
        self.diagnostics_callback = diagnostics_callback

    def reset(self):
        self.close_all()
        self.adapters = {}

    def get_or_create(self, adapter_type):
        key = str(adapter_type).upper()
        adapter = self.adapters.get(key)
        if adapter is not None:
            return adapter
        adapter_cls = resolve_adapter_class(key, self.ble_runtime_config)
        adapter = adapter_cls()
        if self.diagnostics_callback and self.has_method(adapter, "set_diagnostics_callback"):
            adapter.set_diagnostics_callback(self.diagnostics_callback)
        self.adapters[key] = adapter
        return adapter

    def for_sensor(self, sensor):
        return self.get_or_create(sensor.adapter)

    def group_sensors(self, sensors):
        grouped = defaultdict(list)
        for sensor in sensors:
            grouped[self.for_sensor(sensor)].append(sensor)
        return grouped

    @staticmethod
    def has_method(adapter, method_name):
        return hasattr(adapter, method_name) and callable(getattr(adapter, method_name))

    def close_all(self):
        for adapter in list(self.adapters.values()):
            if hasattr(adapter, "close") and callable(getattr(adapter, "close")):
                try:
                    adapter.close()
                except Exception:
                    pass

    async def collect_diagnostics(self):
        snapshots = {}
        for key, adapter in self.adapters.items():
            if not self.has_method(adapter, "get_diagnostics_snapshot"):
                continue
            snapshot = await adapter.get_diagnostics_snapshot()
            if snapshot:
                snapshots[key] = snapshot
        return snapshots
