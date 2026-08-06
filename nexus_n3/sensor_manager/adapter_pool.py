"""Adapter creation and grouping utilities."""

import asyncio
from collections import defaultdict
import inspect

from nexus_n3.sensor_manager.adapter_registry import resolve_adapter_class
from nexus_n3.sensor_manager.adapters.wifi.config import WifiRuntimeConfig
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


class AdapterPool:
    """Owns adapter instances and grouping logic."""

    def __init__(
        self,
        ble_runtime_config: BLERuntimeConfig | None = None,
        wifi_runtime_config: WifiRuntimeConfig | None = None,
        diagnostics_callback=None,
    ):
        self.adapters = {}
        self.ble_runtime_config = ble_runtime_config or BLERuntimeConfig.from_env()
        self.wifi_runtime_config = wifi_runtime_config or WifiRuntimeConfig.from_env()
        self.diagnostics_callback = diagnostics_callback
        self._retired_adapters = []
        self._initialized_adapter_ids = set()
        self._shutdown_adapter_ids = set()
        self._lifecycle_lock = asyncio.Lock()

    def reset(self):
        """Retire current adapters for async cleanup before replacements start."""

        for adapter in self.adapters.values():
            if self.has_method(adapter, "shutdown"):
                self._retired_adapters.append(adapter)
            elif self.has_method(adapter, "close"):
                try:
                    adapter.close()
                except Exception:
                    pass
                finally:
                    self._initialized_adapter_ids.discard(id(adapter))
                    self._shutdown_adapter_ids.add(id(adapter))
        self.adapters = {}

    def get_or_create(self, adapter_type):
        key = str(adapter_type).upper()
        adapter = self.adapters.get(key)
        if adapter is not None:
            return adapter
        adapter_cls = resolve_adapter_class(key, self.ble_runtime_config)
        if key == "WIFI":
            adapter = adapter_cls(config=self.wifi_runtime_config)
        else:
            adapter = adapter_cls()
        self._shutdown_adapter_ids.discard(id(adapter))
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

    def requires_initialization(self):
        return any(
            self.has_method(adapter, "initialize")
            for adapter in self.adapters.values()
        )

    def requires_async_shutdown(self):
        adapters = [*self._retired_adapters, *self.adapters.values()]
        return any(self.has_method(adapter, "shutdown") for adapter in adapters)

    async def initialize_all(self):
        """Initialize each active adapter once, after retiring prior adapters."""

        async with self._lifecycle_lock:
            await self._shutdown_retired()
            for adapter in list(self.adapters.values()):
                adapter_id = id(adapter)
                if adapter_id in self._initialized_adapter_ids:
                    continue
                if self.has_method(adapter, "initialize"):
                    result = adapter.initialize()
                    if inspect.isawaitable(result):
                        await result
                self._initialized_adapter_ids.add(adapter_id)

    async def shutdown_all(self):
        """Shut down active and retired adapters once."""

        async with self._lifecycle_lock:
            await self._shutdown_retired()
            await self._shutdown_adapters(reversed(list(self.adapters.values())))

    async def _shutdown_retired(self):
        retired = self._retired_adapters
        self._retired_adapters = []
        await self._shutdown_adapters(reversed(retired))

    async def _shutdown_adapters(self, adapters):
        first_error = None
        for adapter in adapters:
            adapter_id = id(adapter)
            if adapter_id in self._shutdown_adapter_ids:
                continue
            succeeded = False
            try:
                if self.has_method(adapter, "shutdown"):
                    result = adapter.shutdown()
                    if inspect.isawaitable(result):
                        await result
                elif self.has_method(adapter, "close"):
                    result = adapter.close()
                    if inspect.isawaitable(result):
                        await result
                succeeded = True
            except Exception as exc:
                if first_error is None:
                    first_error = exc
            finally:
                self._initialized_adapter_ids.discard(adapter_id)
                if succeeded:
                    self._shutdown_adapter_ids.add(adapter_id)
        if first_error is not None:
            raise first_error

    def close_all(self):
        """Best-effort synchronous fallback for legacy callers and failed loops."""

        adapters = [*self._retired_adapters, *self.adapters.values()]
        self._retired_adapters = []
        for adapter in adapters:
            adapter_id = id(adapter)
            if adapter_id in self._shutdown_adapter_ids:
                continue
            if hasattr(adapter, "close") and callable(getattr(adapter, "close")):
                try:
                    adapter.close()
                except Exception:
                    pass
                finally:
                    self._initialized_adapter_ids.discard(adapter_id)
                    self._shutdown_adapter_ids.add(adapter_id)

    async def collect_diagnostics(self):
        snapshots = {}
        for key, adapter in self.adapters.items():
            if not self.has_method(adapter, "get_diagnostics_snapshot"):
                continue
            snapshot = await adapter.get_diagnostics_snapshot()
            if snapshot:
                snapshots[key] = snapshot
        return snapshots
