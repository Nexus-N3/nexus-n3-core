"""Streaming orchestration service."""

import time
import asyncio

from nexus_n3.plugins.common.operations import SensorOperation, current_operation, operation_scope

SENSOR_STOP_TIMEOUT_S = 15.0
PHYSICAL_STOP_TIMEOUT_S = 90.0

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
            gateway = getattr(adapter, "gateway_client", None)
            if gateway is not None:
                gateway.set_acquisition_scope((sensor.address for sensor in sensors_for_adapter), append=True)
            for sensor in sensors_for_adapter:
                await self._start_sensor_stream(sensor, adapter)
        emit_to_client("on_stream_started", [sensor.address for sensor in sensors])

    async def stop(self, sensors, emit_to_client, deadline=None):
        """Bound each STOP and retain an outcome for every targeted sensor."""
        deadline = min(deadline or float("inf"), time.monotonic() + PHYSICAL_STOP_TIMEOUT_S)
        results = []
        for adapter, targets in self.adapter_pool.group_sensors(sensors).items():
            for sensor in targets:
                result = {"address": sensor.address, "status": "unconfirmed"}
                if getattr(sensor, "physical_stop_quarantined", False):
                    result.update(status="not_attempted", error="Previous STOP is isolated; reconciliation required")
                elif time.monotonic() >= deadline:
                    result.update(status="not_attempted", error="Physical STOP operation deadline reached")
                elif getattr(getattr(sensor, "connection_status", None), "name", "CONNECTED") != "CONNECTED":
                    if getattr(sensor, "measurement_cleanup_pending", False):
                        result.update(status="not_attempted", error="Sensor link unavailable; measurement STOP unconfirmed")
                    else:
                        result["status"] = "stopped"
                else:
                    operation = SensorOperation(min(deadline, time.monotonic() + SENSOR_STOP_TIMEOUT_S))
                    with operation_scope(operation):
                        task = asyncio.create_task(self._stop_sensor_stream(sensor, adapter))
                    try:
                        done, _ = await asyncio.wait({task}, timeout=max(0, operation.deadline - time.monotonic()))
                        if not done:
                            sensor.physical_stop_quarantined = True
                            raise TimeoutError("Physical sensor STOP deadline reached")
                        task.result()
                        result["status"] = "stopped"
                    except asyncio.CancelledError:
                        sensor.physical_stop_quarantined = True
                        raise
                    except Exception as exc:
                        result["error"] = f"{type(exc).__name__}: {exc}"
                        # A nested timeout can leave an RPC/thread running too.
                        if time.monotonic() >= operation.deadline or isinstance(exc, TimeoutError) or (getattr(sensor, "_stop_worker", None) is not None
                                and not sensor._stop_worker.done()):
                            sensor.physical_stop_quarantined = True
                    finally:
                        operation.cancelled.set()
                        if not task.done():
                            task.cancel()
                            task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
                results.append(result)
        # Diagnostics are bounded too and may not initiate shared recovery.
        remaining = min(5.0, deadline - time.monotonic())
        if remaining > 0 and hasattr(self.adapter_pool, "collect_diagnostics"):
            operation = SensorOperation(time.monotonic() + remaining)
            with operation_scope(operation):
                diagnostic_task = asyncio.create_task(self.adapter_pool.collect_diagnostics())
            try:
                done, _ = await asyncio.wait({diagnostic_task}, timeout=remaining)
                if done:
                    diagnostics = diagnostic_task.result()
                    if diagnostics:
                        emit_to_client("on_diagnostics", {"trigger": "stream_stopped",
                                       "addresses": [s.address for s in sensors], "diagnostics": diagnostics})
            except Exception:
                self.logger.exception("STOP diagnostics unavailable")
            finally:
                operation.cancelled.set()
                if not diagnostic_task.done():
                    diagnostic_task.cancel()
                    diagnostic_task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        emit_to_client("on_stream_stopped", [r["address"] for r in results if r["status"] == "stopped"])
        return results

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

        if getattr(sensor, "physical_stop_quarantined", False):
            raise RuntimeError("Sensor requires physical STOP reconciliation before reuse")
        sensor.measurement_cleanup_pending = True
        if hasattr(getattr(sensor, "transport_client", None), "retain_for_recovery"):
            sensor.transport_client.retain_for_recovery = True
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
                stopped = await sensor.stop_stream(adapter)
                if stopped is False:
                    raise RuntimeError("Plugin reported unsuccessful STOP")
                operation = current_operation.get()
                if operation is not None:
                    operation.check()
                sensor.measurement_cleanup_pending = False
                if hasattr(getattr(sensor, "transport_client", None), "retain_for_recovery"):
                    sensor.transport_client.retain_for_recovery = False
            elif not (
                hasattr(sensor, "request_sample")
                and callable(getattr(sensor, "request_sample"))
            ):
                raise RuntimeError(f"Sensor {sensor_name} does not implement stop-stream hooks")
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
        if hasattr(sensor, "request_sample") and not hasattr(sensor, "stop_stream"):
            operation = current_operation.get()
            if operation is not None:
                operation.check()
            sensor.measurement_cleanup_pending = False
            if hasattr(getattr(sensor, "transport_client", None), "retain_for_recovery"):
                sensor.transport_client.retain_for_recovery = False
        self.logger.info(
            "sensor stop completed address=%s name=%s location=%s duration_ms=%.3f",
            address,
            sensor_name,
            location,
            (time.monotonic_ns() - started_ns) / 1_000_000.0,
        )
