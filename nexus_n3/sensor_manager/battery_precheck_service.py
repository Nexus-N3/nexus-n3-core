"""Standalone pre-init battery check workflow."""

import asyncio
from typing import Dict

from nexus_n3.sensor_manager.adapter_registry import resolve_adapter_class
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.utils import utils as utils


class BatteryPrecheckService:
    """Runs battery checks independently from subject init flow."""

    def __init__(
        self,
        loop,
        register_listeners_with_sensor,
        logger,
        ble_runtime_config: BLERuntimeConfig | None = None,
    ):
        self.loop = loop
        self.register_listeners_with_sensor = register_listeners_with_sensor
        self.logger = logger
        self.ble_runtime_config = ble_runtime_config or BLERuntimeConfig.from_env()

    async def check_battery_preinit(
        self,
        sensor_classes,
        scan_timeout: float = 5.0,
        read_timeout: float = 10.0,
    ):
        if not sensor_classes:
            return []

        name_map = utils.build_battery_name_map(sensor_classes)
        if not name_map:
            return []

        names_sorted = sorted(name_map.keys(), key=len, reverse=True)
        names_sorted_lower = [(name, name.lower()) for name in names_sorted]

        adapter_cls = resolve_adapter_class("BLE", self.ble_runtime_config)
        adapter = adapter_cls()
        self.logger.info("Battery check scan for %d name(s)", len(name_map.keys()))
        try:
            devices = await adapter.discover_devices(list(name_map.keys()), timeout=scan_timeout)

            matched_sensors = []
            for addr, (device, adv_data) in devices.items():
                local_name = getattr(adv_data, "local_name", None)
                matched_name = utils.match_sensor_name(local_name, names_sorted, names_sorted_lower)
                if not matched_name:
                    continue
                cls = name_map[matched_name]
                sensor = utils.instantiate_sensor_class(cls)
                if sensor is None:
                    continue
                address = getattr(device, "address", None) or getattr(device, "path", None) or addr
                sensor.address = address
                sensor.set_transport_client(adapter.create_transport_client(address, loop=self.loop))
                matched_sensors.append(sensor)

            if not matched_sensors:
                self.logger.info("Battery check: no matching BLE sensors discovered.")
                return {"sensors": [], "errors": {}}

            for sensor in matched_sensors:
                self.register_listeners_with_sensor(sensor)
                if hasattr(sensor, "bind_manager_runtime") and callable(getattr(sensor, "bind_manager_runtime")):
                    sensor.bind_manager_runtime(loop=self.loop)
                setattr(sensor, "_runtime_adapter", adapter)

            errors = await self._connect_setup_disconnect(
                sensors=matched_sensors,
                adapter=adapter,
                enable_battery=True,
                read_timeout=read_timeout,
            )

            sensors_info = [
                {"address": sensor.address, "name": sensor.name}
                for sensor in matched_sensors
            ]
            self.logger.info("Battery check complete for %d device(s)", len(sensors_info))
            return {"sensors": sensors_info, "errors": errors}
        finally:
            if hasattr(adapter, "close") and callable(getattr(adapter, "close")):
                try:
                    adapter.close()
                except Exception:
                    pass

    async def _connect_setup_disconnect(
        self,
        sensors,
        adapter,
        enable_battery: bool,
        read_timeout: float,
    ) -> Dict[str, str]:
        errors: Dict[str, str] = {}
        try:
            self.logger.info("Battery check connecting to %d device(s)", len(sensors))
            await asyncio.wait_for(
                adapter.connect_all(sensors, adapter),
                timeout=max(10.0, read_timeout),
            )
        except asyncio.TimeoutError:
            for sensor in sensors:
                errors[sensor.address] = "connect timed out"
        except Exception as exc:
            for sensor in sensors:
                errors[sensor.address] = f"connect failed: {exc}"

        for sensor in sensors:
            if sensor.address in errors:
                continue
            if hasattr(sensor, "setup") and callable(getattr(sensor, "setup")):
                try:
                    await asyncio.wait_for(
                        sensor.setup(adapter, enable_battery=enable_battery, enable_button=False),
                        timeout=read_timeout,
                    )
                except asyncio.TimeoutError:
                    errors[sensor.address] = "setup timed out"
                except Exception as exc:
                    errors[sensor.address] = f"setup failed: {exc}"

        await asyncio.sleep(0.5)

        for sensor in sensors:
            try:
                await asyncio.wait_for(
                    adapter.disconnect(sensor.transport_client),
                    timeout=5.0,
                )
            except Exception:
                pass

        return errors
