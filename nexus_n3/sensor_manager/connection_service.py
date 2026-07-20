"""Connection and setup/disconnect service."""

import platform


class ConnectionService:
    """Connect/disconnect flows across adapter groups."""

    def __init__(self, adapter_pool, logger):
        self.adapter_pool = adapter_pool
        self.logger = logger

    async def _connect_and_setup_sequentially(self, sensors_for_adapter, adapter, set_up_sensor):
        connected = []

        for sensor in sensors_for_adapter:
            ok = await adapter.connect_to_device(sensor, adapter)

            if not ok:
                continue

            if (
                getattr(sensor, "connection_status", None) is not None
                and sensor.connection_status.name == "CONNECTED"
            ):
                await set_up_sensor(sensor)
                connected.append(sensor)

        return connected

    async def connect_all(self, sensors, set_up_sensor, emit_to_client):
        connected = []
        adapter_groups = self.adapter_pool.group_sensors(sensors)

        for adapter, sensors_for_adapter in adapter_groups.items():
            if not self.adapter_pool.has_method(adapter, "connect_all"):
                self.logger.warning(
                    "Adapter %s does not implement connect_all",
                    getattr(adapter, "adapter_type", str(adapter)),
                )
                continue

            if platform.system() == "Linux" and self.adapter_pool.has_method(adapter, "connect_to_device"):
                connected.extend(
                    await self._connect_and_setup_sequentially(
                        sensors_for_adapter, adapter, set_up_sensor
                    )
                )
            else:
                await adapter.connect_all(sensors_for_adapter, adapter)
                for sensor in sensors_for_adapter:
                    if (
                        getattr(sensor, "connection_status", None) is not None
                        and sensor.connection_status.name == "CONNECTED"
                    ):
                        await set_up_sensor(sensor)
                        connected.append(sensor)

        emit_to_client("on_connected", connected)
        return connected

    async def connect_specific(self, sensors_to_connect, set_up_sensor, emit_to_client):
        if not sensors_to_connect:
            return []

        connected = []
        adapter_groups = self.adapter_pool.group_sensors(sensors_to_connect)

        for adapter, sensors_for_adapter in adapter_groups.items():
            if not self.adapter_pool.has_method(adapter, "connect_all"):
                self.logger.warning(
                    "Adapter %s does not implement connect_all",
                    getattr(adapter, "adapter_type", str(adapter)),
                )
                continue

            if platform.system() == "Linux" and self.adapter_pool.has_method(adapter, "connect_to_device"):
                connected.extend(
                    await self._connect_and_setup_sequentially(
                        sensors_for_adapter, adapter, set_up_sensor
                    )
                )
            else:
                await adapter.connect_all(sensors_for_adapter, adapter)
                for sensor in sensors_for_adapter:
                    if (
                        getattr(sensor, "connection_status", None) is not None
                        and sensor.connection_status.name == "CONNECTED"
                    ):
                        await set_up_sensor(sensor)
                        connected.append(sensor)

        emit_to_client("on_connected", connected)
        return connected

    async def disconnect(self, sensors_to_disconnect, emit_to_client, disconnected_status):
        disconnected = []
        adapter_groups = self.adapter_pool.group_sensors(sensors_to_disconnect)

        for adapter, sensors_for_adapter in adapter_groups.items():
            if not self.adapter_pool.has_method(adapter, "disconnect"):
                self.logger.warning(
                    "Adapter %s does not implement disconnect",
                    getattr(adapter, "adapter_type", str(adapter)),
                )
                continue

            for sensor in sensors_for_adapter:
                if await adapter.disconnect(sensor.transport_client):
                    sensor.set_connection_status(disconnected_status)
                    disconnected.append(sensor.address)

        emit_to_client("on_disconnected", disconnected)
        return disconnected