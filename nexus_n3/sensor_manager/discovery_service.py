"""Sensor discovery and address assignment service."""

import asyncio

from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from nexus_n3.sensor_manager.utils import utils as utils


def _build_disconnect_callback(sensor):
    def handle_disconnect(_client):
        sensor.set_connection_status(ConnectionStatus.DISCONNECTED)
        sensor._emit("on_disconnected", {"address": sensor.address})

    return handle_disconnect


class DiscoveryService:
    """Discovery workflows for all sensors or subject-scoped sensors."""

    def __init__(self, adapter_pool, logger):
        self.adapter_pool = adapter_pool
        self.logger = logger

    async def discover_all(self, sensors, loop, register_listeners_with_sensor, emit_to_client):
        pending_sensors = [
            s for s in sensors if s.connection_status != ConnectionStatus.CONNECTED
        ]
        return await self._discover_pending(
            pending_sensors=pending_sensors,
            loop=loop,
            register_listeners_with_sensor=register_listeners_with_sensor,
            emit_to_client=emit_to_client,
        )

    async def discover_for_subject(
        self,
        sensors,
        requested_sensors,
        loop,
        register_listeners_with_sensor,
        emit_to_client,
    ):
        pending_sensors = []
        for req in requested_sensors:
            matching = [
                s
                for s in sensors
                if s.name == req["local_name"]
                and s.connection_status != ConnectionStatus.CONNECTED
            ]
            pending_sensors.extend(matching[:req.get("number_of", len(matching))])

        return await self._discover_pending(
            pending_sensors=pending_sensors,
            loop=loop,
            register_listeners_with_sensor=register_listeners_with_sensor,
            emit_to_client=emit_to_client,
        )

    async def _discover_pending(
        self,
        pending_sensors,
        loop,
        register_listeners_with_sensor,
        emit_to_client,
    ):
        if not pending_sensors:
            return []

        discovered_sensors = []
        adapter_groups = self.adapter_pool.group_sensors(pending_sensors)

        for adapter, sensors_for_adapter in adapter_groups.items():
            if not self.adapter_pool.has_method(adapter, "discover_devices"):
                continue

            sensor_names = [s.name for s in sensors_for_adapter]
            matched = []
            validation = None
            for attempt in range(1, 3):
                devices = await adapter.discover_devices(sensors_for_adapter)
                matched = utils.match_devices(sensor_names, devices)
                validation = utils.validate_matched_devices(sensors_for_adapter, matched)
                if validation.valid:
                    break
                if attempt < 2:
                    self.logger.warning(
                        "Discovery incomplete for sensors=%s missing=%s; retrying scan",
                        sensor_names,
                        validation.missing,
                    )
                    await asyncio.sleep(1.0)

            if not validation.valid:
                emit_to_client(
                    "on_discover",
                    {"valid": validation.valid, "missing": validation.missing},
                )
                return []

            for sensor, entry in zip(sensors_for_adapter, matched):
                device, _adv_data = entry[0], entry[1]
                address = getattr(device, "address", None) or getattr(device, "path", None)
                sensor.address = address
                if self.adapter_pool.has_method(adapter, "create_transport_client"):
                    sensor.set_transport_client(
                        adapter.create_transport_client(
                            address,
                            loop=loop,
                            disconnected_callback=_build_disconnect_callback(sensor),
                        )
                    )
                elif self.adapter_pool.has_method(adapter, "create_ble_client"):
                    sensor.set_transport_client(
                        adapter.create_ble_client(
                            address,
                            loop=loop,
                            disconnected_callback=_build_disconnect_callback(sensor),
                        )
                    )
                register_listeners_with_sensor(sensor)
                discovered_sensors.append(sensor)

        emit_to_client("on_discover", discovered_sensors)
        return discovered_sensors
