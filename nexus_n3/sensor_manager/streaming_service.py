"""Streaming orchestration service."""

import time

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
        self.logger.info(
            "sensor stop sequence starting sensors=%s",
            [
                {
                    "address": getattr(sensor, "address", None),
                    "name": getattr(sensor, "name", type(sensor).__name__),
                    "location": getattr(sensor, "location", None),
                }
                for sensor in sensors
            ],
        )
        adapter_groups = self.adapter_pool.group_sensors(sensors)
        for adapter, sensors_for_adapter in adapter_groups.items():
            for sensor in sensors_for_adapter:
                await self._stop_sensor_stream(sensor, adapter)
        self.logger.info(
            "sensor stop sequence completed addresses=%s",
            [getattr(sensor, "address", None) for sensor in sensors],
        )
        
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
        emit_to_client("on_stream_stopped", [sensor.address for sensor in sensors])

    async def shutdown(self):
        await self.polling_stream_service.stop_all()

    async def _start_sensor_stream(self, sensor, adapter):
        address = getattr(sensor, "address", None)
        sensor_name = getattr(sensor, "name", type(sensor).__name__)
        location = getattr(sensor, "location", None)
        started_ns = time.monotonic_ns()

        self.logger.info(
            "sensor start starting address=%s name=%s location=%s adapter=%s",
            address,
            sensor_name,
            location,
            type(adapter).__name__,
        )

        pipeline_diagnostics.mark_stream_start_command(
            address,
            location=location,
        )

        try:
            if hasattr(sensor, "start_stream") and callable(
                getattr(sensor, "start_stream")
            ):
                await sensor.start_stream(adapter)

            elif hasattr(sensor, "request_sample") and callable(
                getattr(sensor, "request_sample")
            ):
                await self.polling_stream_service.start(
                    sensor,
                    adapter,
                )

            else:
                self.logger.warning(
                    "Sensor %s does not implement streaming hooks",
                    sensor_name,
                )

        except Exception as exc:
            self.logger.error(
                "sensor start failed address=%s name=%s "
                "location=%s duration_ms=%.3f error=%s: %s",
                address,
                sensor_name,
                location,
                (time.monotonic_ns() - started_ns)
                / 1_000_000.0,
                type(exc).__name__,
                exc,
            )
            raise

        self.logger.info(
            "sensor start completed address=%s name=%s "
            "location=%s duration_ms=%.3f",
            address,
            sensor_name,
            location,
            (time.monotonic_ns() - started_ns)
            / 1_000_000.0,
        )

    async def _stop_sensor_stream(self, sensor, adapter):
        address = getattr(sensor, "address", None)
        sensor_name = getattr(sensor, "name", type(sensor).__name__)
        location = getattr(sensor, "location", None)
        started_ns = time.monotonic_ns()
        self.logger.info(
            "sensor stop starting address=%s name=%s location=%s adapter=%s",
            address,
            sensor_name,
            location,
            type(adapter).__name__,
        )
        try:
            await self.polling_stream_service.stop(sensor)
            if hasattr(sensor, "stop_stream") and callable(getattr(sensor, "stop_stream")):
                await sensor.stop_stream(adapter)
            elif not (
                hasattr(sensor, "request_sample")
                and callable(getattr(sensor, "request_sample"))
            ):
                self.logger.warning("Sensor %s does not implement stop-stream hooks", sensor_name)
        except Exception as exc:
            self.logger.error(
                "sensor stop failed address=%s name=%s location=%s duration_ms=%.3f error=%s: %s",
                address,
                sensor_name,
                location,
                (time.monotonic_ns() - started_ns) / 1_000_000.0,
                type(exc).__name__,
                exc,
            )
            raise
        self.logger.info(
            "sensor stop completed address=%s name=%s location=%s duration_ms=%.3f",
            address,
            sensor_name,
            location,
            (time.monotonic_ns() - started_ns) / 1_000_000.0,
        )
