"""Streaming orchestration service."""

from nexus_n3.core.pipeline_diagnostics import pipeline_diagnostics


class StreamingService:
    """Start/stop streaming for push and polling sensor modes."""

    def __init__(self, adapter_pool, polling_stream_service, logger):
        self.adapter_pool = adapter_pool
        self.polling_stream_service = polling_stream_service
        self.logger = logger

    async def start(self, sensors, emit_to_client):
        adapter_groups = self.adapter_pool.group_sensors(sensors)
        for adapter, sensors_for_adapter in adapter_groups.items():
            for sensor in sensors_for_adapter:
                await self._start_sensor_stream(sensor, adapter)
        emit_to_client("on_stream_started", [sensor.address for sensor in sensors])

    async def stop(self, sensors, emit_to_client):
        adapter_groups = self.adapter_pool.group_sensors(sensors)
        for adapter, sensors_for_adapter in adapter_groups.items():
            for sensor in sensors_for_adapter:
                await self._stop_sensor_stream(sensor, adapter)
        emit_to_client("on_stream_stopped", [sensor.address for sensor in sensors])
        diagnostics = await self.adapter_pool.collect_diagnostics()
        if diagnostics:
            emit_to_client(
                "on_diagnostics",
                {
                    "trigger": "stream_stopped",
                    "addresses": [sensor.address for sensor in sensors],
                    "diagnostics": diagnostics,
                },
            )

    async def shutdown(self):
        await self.polling_stream_service.stop_all()

    async def _start_sensor_stream(self, sensor, adapter):
        pipeline_diagnostics.mark_stream_start_command(
            getattr(sensor, "address", None),
            location=getattr(sensor, "location", None),
        )
        if hasattr(sensor, "start_stream") and callable(getattr(sensor, "start_stream")):
            await sensor.start_stream(adapter)
            return
        if hasattr(sensor, "request_sample") and callable(getattr(sensor, "request_sample")):
            await self.polling_stream_service.start(sensor, adapter)
            return
        self.logger.warning("Sensor %s does not implement streaming hooks", sensor.name)

    async def _stop_sensor_stream(self, sensor, adapter):
        await self.polling_stream_service.stop(sensor)
        if hasattr(sensor, "stop_stream") and callable(getattr(sensor, "stop_stream")):
            await sensor.stop_stream(adapter)
            return
        if hasattr(sensor, "request_sample") and callable(getattr(sensor, "request_sample")):
            return
        self.logger.warning("Sensor %s does not implement stop-stream hooks", sensor.name)
