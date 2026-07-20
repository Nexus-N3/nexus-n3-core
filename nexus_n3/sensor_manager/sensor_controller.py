"""Command routing/controller for sensor operations."""

from nexus_n3.sensor_manager.types.connections import ConnectionStatus


class SensorController:
    """Dispatches queued commands to services."""

    def __init__(
        self,
        sensors_ref,
        get_connected_sensors,
        get_connected_sensor_by_address,
        set_up_sensor,
        register_listeners_with_sensor,
        emit_to_client,
        loop,
        adapter_pool,
        discovery_service,
        connection_service,
        streaming_service,
    ):
        self._sensors_ref = sensors_ref
        self.get_connected_sensors = get_connected_sensors
        self.get_connected_sensor_by_address = get_connected_sensor_by_address
        self.set_up_sensor = set_up_sensor
        self.register_listeners_with_sensor = register_listeners_with_sensor
        self.emit_to_client = emit_to_client
        self.loop = loop
        self.adapter_pool = adapter_pool
        self.discovery_service = discovery_service
        self.connection_service = connection_service
        self.streaming_service = streaming_service

        self.handlers = {
            "discover": (self.handle_discover, None),
            "discover_for_subject": (self.handle_discover_for_subject, "sensors"),
            "connect_all": (self.handle_connect_all, None),
            "connect_specific_sensors": (self.handle_connect_specific_sensors, "addresses"),
            "discover_and_connect": (self.handle_discover_and_connect, None),
            "disconnect_all": (self.handle_disconnect_all, None),
            "disconnect_addresses": (self.handle_disconnect_addresses, "addresses"),
            "start_all": (self.handle_start_all, None),
            "start_specific_sensors": (self.handle_start_specific_sensors, "addresses"),
            "stop_all": (self.handle_stop_all, None),
            "stop_specific_sensors": (self.handle_stop_specific_sensors, "addresses"),
            "identify": (self.handle_identify, "address"),
        }

    def _sensors(self):
        return self._sensors_ref()

    async def dispatch(self, msg):
        message_type = msg.get("message")
        handler_entry = self.handlers.get(message_type)
        if handler_entry is None:
            return False
        handler, arg_key = handler_entry
        if arg_key is None:
            await handler()
        else:
            await handler(msg[arg_key])
        return True

    async def handle_discover(self):
        return await self.discovery_service.discover_all(
            sensors=self._sensors(),
            loop=self.loop,
            register_listeners_with_sensor=self.register_listeners_with_sensor,
            emit_to_client=self.emit_to_client,
        )

    async def handle_discover_for_subject(self, sensors):
        return await self.discovery_service.discover_for_subject(
            sensors=self._sensors(),
            requested_sensors=sensors,
            loop=self.loop,
            register_listeners_with_sensor=self.register_listeners_with_sensor,
            emit_to_client=self.emit_to_client,
        )

    async def handle_connect_all(self):
        return await self.connection_service.connect_all(
            sensors=self._sensors(),
            set_up_sensor=self.set_up_sensor,
            emit_to_client=self.emit_to_client,
        )

    async def handle_connect_specific_sensors(self, addresses):
        sensors_to_connect = [s for s in self._sensors() if s.address in addresses]
        return await self.connection_service.connect_specific(
            sensors_to_connect=sensors_to_connect,
            set_up_sensor=self.set_up_sensor,
            emit_to_client=self.emit_to_client,
        )

    async def handle_discover_and_connect(self):
        await self.handle_discover()
        return await self.handle_connect_all()

    async def handle_disconnect_all(self):
        return await self.connection_service.disconnect(
            sensors_to_disconnect=self.get_connected_sensors(),
            emit_to_client=self.emit_to_client,
            disconnected_status=ConnectionStatus.DISCONNECTED,
        )

    async def handle_disconnect_addresses(self, addresses):
        sensors_to_disconnect = [s for s in self._sensors() if s.address in addresses]
        return await self.connection_service.disconnect(
            sensors_to_disconnect=sensors_to_disconnect,
            emit_to_client=self.emit_to_client,
            disconnected_status=ConnectionStatus.DISCONNECTED,
        )

    async def handle_start_all(self):
        return await self.streaming_service.start(
            sensors=self.get_connected_sensors(),
            emit_to_client=self.emit_to_client,
        )

    async def handle_start_specific_sensors(self, addresses):
        filtered = [s for s in self.get_connected_sensors() if s.address in addresses]
        return await self.streaming_service.start(
            sensors=filtered,
            emit_to_client=self.emit_to_client,
        )

    async def handle_stop_all(self):
        return await self.streaming_service.stop(
            sensors=self.get_connected_sensors(),
            emit_to_client=self.emit_to_client,
        )

    async def handle_stop_specific_sensors(self, addresses):
        filtered = [s for s in self.get_connected_sensors() if s.address in addresses]
        return await self.streaming_service.stop(
            sensors=filtered,
            emit_to_client=self.emit_to_client,
        )

    async def handle_identify(self, address):
        sensors = self.get_connected_sensor_by_address(address)
        if not sensors:
            return
        sensor = sensors[0]
        if hasattr(sensor, "identify") and callable(getattr(sensor, "identify")):
            self.emit_to_client("on_identify", sensor.address)
            await sensor.identify(self.adapter_pool.for_sensor(sensor))
