"""Explicit operator cleanup, independent of current link availability."""

import asyncio
import time

from nexus_n3.sensor_manager.adapters.gateway_ble_client import GatewayOperationPolicy
from nexus_n3.sensor_manager.types.connections import ConnectionStatus


# Cleanup is a single stop/teardown attempt, not acquisition recovery.
# 20s reconnect + 15s plugin stop + 5s disconnect + 5s local work.
SENSOR_CLEANUP_TIMEOUT_S = 45
CLEANUP_OPERATION_TIMEOUT_S = 90
CLEANUP_CANCEL_RESERVE_S = 5


class CleanupService:
    def __init__(self, adapter_pool, polling_stream_service):
        self.adapter_pool = adapter_pool
        self.polling_stream_service = polling_stream_service

    async def cleanup(self, sensors, deadline=None):
        deadline = min(float(deadline or float("inf")), time.time() + CLEANUP_OPERATION_TIMEOUT_S)
        results = []
        for sensor in sensors:
            try:
                remaining = deadline - time.time() - CLEANUP_CANCEL_RESERVE_S
                if remaining <= 0:
                    results.append({"address": sensor.address, "name": sensor.name,
                                    "status": "unconfirmed" if sensor.measurement_cleanup_pending else "failed",
                                    "error": "Cleanup operation deadline reached; sensor cleanup unfinished"})
                    continue
                if sensor.transport_client is not None:
                    sensor.transport_client.cleanup_deadline_unix_s = deadline - CLEANUP_CANCEL_RESERVE_S
                result = await asyncio.wait_for(self._cleanup_sensor(sensor), min(SENSOR_CLEANUP_TIMEOUT_S, remaining))
                result["link_connected"] = bool(getattr(
                    sensor.transport_client, "is_connected",
                    getattr(sensor.connection_status, "name", None) == "CONNECTED",
                ))
                results.append(result)
            except Exception as exc:
                if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
                    await self._abort(sensor, deadline)
                results.append({"address": sensor.address,
                                "status": "unconfirmed" if sensor.measurement_cleanup_pending else "failed",
                                "error": f"{type(exc).__name__}: {exc}"})
        return results

    async def _abort(self, sensor, deadline):
        abort = getattr(sensor, "abort_cleanup", None)
        if callable(abort):
            try:
                await asyncio.wait_for(asyncio.to_thread(abort), max(.001, min(5, deadline - time.time())))
            except Exception:
                pass  # Cleanup stays unconfirmed; never recover shared transports.

    async def _cleanup_sensor(self, sensor):
        result = {"address": sensor.address, "name": sensor.name,
                  "location": sensor.location, "status": "already_clean"}
        client = sensor.transport_client
        pending = bool(getattr(sensor, "measurement_cleanup_pending", False))
        connected = bool(getattr(client, "is_connected", False))
        adapter = self.adapter_pool.for_sensor(sensor)
        previous_policy = getattr(client, "operation_policy", None)
        sensor.cleanup_in_progress = True
        if previous_policy is not None:
            client.operation_policy = GatewayOperationPolicy.SENSOR_CLEANUP
        try:
            await asyncio.wait_for(self.polling_stream_service.stop(sensor), timeout=5)
            if pending and not connected:
                reconnect = getattr(adapter, "connect_for_cleanup", None)
                if reconnect is None or client is None:
                    result.update(status="unconfirmed", error="Safe cleanup reconnect unavailable")
                    return result
                connected = bool(await asyncio.wait_for(reconnect(client), timeout=20))
                if not connected:
                    result.update(status="unconfirmed", error="Sensor unreachable")
                    return result
                await asyncio.wait_for(asyncio.to_thread(sensor.set_connection_status, ConnectionStatus.CONNECTED), timeout=5)

            if pending and connected:
                stop_task = None
                try:
                    stop_task = asyncio.create_task(sensor.stop_stream(adapter))
                    stopped = await asyncio.wait_for(stop_task, timeout=15)
                    if stopped is False:
                        raise RuntimeError("Plugin reported unsuccessful stop")
                    sensor.measurement_cleanup_pending = False
                except Exception as exc:
                    if stop_task is not None and stop_task.cancelled():
                        await self._abort(sensor, time.time() + 5)
                    result.update(status="unconfirmed", error=f"Measurement stop failed: {type(exc).__name__}: {exc}")

            if connected:
                try:
                    if hasattr(adapter, "disconnect_sensor"):
                        ok = await asyncio.wait_for(adapter.disconnect_sensor(sensor), timeout=5)
                    else:
                        ok = await asyncio.wait_for(adapter.disconnect(client), timeout=5)
                    if not ok:
                        raise RuntimeError("Sensor disconnect was not confirmed")
                    await asyncio.wait_for(asyncio.to_thread(sensor.set_connection_status, ConnectionStatus.DISCONNECTED), timeout=5)
                    if result["status"] != "unconfirmed":
                        result["status"] = "cleaned"
                except Exception as exc:
                    result.update(status="failed", error=f"Disconnect failed: {type(exc).__name__}: {exc}")

            if not getattr(sensor, "measurement_cleanup_pending", False) and result["status"] != "failed":
                release = getattr(adapter, "release_transport_client", None)
                if release and client is not None:
                    release(client)
                if hasattr(sensor, "close_host"):
                    await asyncio.wait_for(asyncio.to_thread(sensor.close_host), timeout=5)
                sensor.set_transport_client(None)
            return result
        except Exception as exc:
            if isinstance(exc, asyncio.TimeoutError):
                await self._abort(sensor, time.time() + 5)
            result.update(status="unconfirmed" if getattr(sensor, "measurement_cleanup_pending", False) else "failed",
                          error=f"{type(exc).__name__}: {exc}")
            return result
        finally:
            # A timed-out RPC may still finish in its worker thread. Keep unresolved
            # clients isolated so late plugin operations cannot reset the gateway.
            if previous_policy is not None and not sensor.measurement_cleanup_pending:
                client.operation_policy = previous_policy
            sensor.cleanup_in_progress = False
