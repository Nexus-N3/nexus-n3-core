"""Explicit operator cleanup, independent of current link availability."""

import asyncio
import time

from nexus_n3.sensor_manager.adapters.gateway_ble_client import GatewayOperationPolicy
from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from nexus_n3.plugins.common.operations import SensorOperation, operation_scope


# Cleanup is a single stop/teardown attempt, not acquisition recovery.
# 20s reconnect + 15s plugin stop + 5s disconnect + 5s local work.
SENSOR_CLEANUP_TIMEOUT_S = 45
CLEANUP_OPERATION_TIMEOUT_S = 90
CLEANUP_CANCEL_RESERVE_S = 5


class CleanupService:
    def __init__(self, adapter_pool, polling_stream_service):
        self.adapter_pool = adapter_pool
        self.polling_stream_service = polling_stream_service

    async def reconcile(self, sensors, *, deadline, active_addresses=(), allow_ble_reset=False,
                        confirmed_stopped_addresses=()):
        """Recover UART and STOP retained identities without shared BLE reset."""
        results, gateways = [], []
        active = {str(address).upper() for address in active_addresses}
        confirmed = {str(address).upper() for address in confirmed_stopped_addresses}
        groups = self.adapter_pool.group_sensors(sensors)
        for adapter in self.adapter_pool.adapters.values():
            groups.setdefault(adapter, [])
        for adapter, targets in groups.items():
            gateway_error = None
            recover = getattr(adapter, "recover_connections", None)
            if recover:
                adopt = getattr(adapter, "adopt_recovery_client", None)
                if adopt:
                    for sensor in targets:
                        if str(sensor.address).upper() not in active:
                            adopt(sensor)
                operation = SensorOperation(min(deadline, time.monotonic() + 8))
                try:
                    inventory = await self._bounded(recover(), operation)
                    if not inventory.get("reset_confirmed", True) or confirmed:
                        if allow_ble_reset is not True:
                            gateway_error = "Previous BLE reset outcome remains uncertain; explicit controlled reset required"
                        else:
                            reset_operation = SensorOperation(min(deadline, time.monotonic() + 7))
                            async def reset():
                                await adapter.execute(adapter.gateway_client.reset_session, 5.0)
                                return await adapter.recover_connections()
                            try:
                                inventory = await self._bounded(reset(), reset_operation)
                            except Exception as exc:
                                gateway_error = f"Gateway BLE reset remains unconfirmed: {exc}"
                    gateways.append({"available": True, **inventory})
                    if inventory.get("ble_ready") is False:
                        gateway_error = "Gateway UART responds, but BLE is not ready; retry recovery"
                except Exception as exc:
                    gateway_error = f"Gateway communication unavailable: {exc}; restore UART and retry Reset"
                    gateways.append({"available": False, "error": str(exc)})
            for sensor in targets:
                result = {"address": sensor.address, "name": sensor.name,
                          "location": sensor.location, "status": "unconfirmed",
                          "measurement_stop_confirmed": not sensor.measurement_cleanup_pending,
                          "eligible_for_acquisition": False}
                if str(sensor.address).upper() in active:
                    result.update(status="failed", error="Sensor is actively acquiring; stop its subject before recovery")
                elif gateway_error:
                    result["error"] = gateway_error
                elif time.monotonic() >= deadline:
                    result["error"] = "Recovery deadline reached; retry Reset"
                else:
                    operation = SensorOperation(min(deadline, time.monotonic() + SENSOR_CLEANUP_TIMEOUT_S))
                    try:
                        if str(sensor.address).upper() in confirmed:
                            if not recover or allow_ble_reset is not True or str(sensor.address).upper() in inventory["connected_addresses"]:
                                raise RuntimeError("External STOP confirmation requires a confirmed controlled BLE reset")
                            await self._bounded(self._retire_after_external_stop(sensor, adapter, operation), operation)
                            result["confirmation_source"] = "operator"
                        else:
                            await self._bounded(self._reconcile_sensor(sensor, adapter, operation), operation)
                        result.update(status="cleaned", measurement_stop_confirmed=True,
                                      eligible_for_acquisition=True, link_connected=False)
                    except Exception as exc:
                        result["error"] = (f"{type(exc).__name__}: {exc}. Retry Reset with the sensor in range; "
                                           "if STOP remains unconfirmed, power cycle the sensor.")
                        sensor.physical_stop_quarantined = True
                        result["measurement_stop_confirmed"] = not sensor.measurement_cleanup_pending
                results.append(result)
                result.setdefault("link_connected", bool(getattr(sensor.transport_client, "is_connected", False)))
        return {"results": results, "gateways": gateways}

    @staticmethod
    async def _retire_after_external_stop(sensor, adapter, operation):
        retire = getattr(sensor, "release_after_verified_stop", getattr(sensor, "close_host", None))
        if retire:
            await asyncio.to_thread(retire)
        operation.check()
        sensor.measurement_cleanup_pending = False
        sensor.physical_stop_quarantined = False
        sensor.connection_status = ConnectionStatus.DISCONNECTED
        if sensor.transport_client is not None:
            adapter.release_transport_client(sensor.transport_client)

    @staticmethod
    async def _bounded(coro, operation):
        with operation_scope(operation):
            task = asyncio.create_task(coro)
        try:
            done, _ = await asyncio.wait({task}, timeout=max(0, operation.deadline - time.monotonic()))
            if not done:
                raise TimeoutError("Recovery operation deadline reached")
            operation.check()
            return task.result()
        finally:
            operation.cancelled.set()
            if not task.done():
                task.cancel()
                task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)

    async def _reconcile_sensor(self, sensor, adapter, operation):
        prepare = getattr(sensor, "prepare_recovery", None)
        if prepare:
            await prepare()
        else:
            worker = getattr(sensor, "_stop_worker", None)
            if worker is not None and not worker.done():
                raise RuntimeError("Previous STOP is still executing; retry Reset later")
            sensor.physical_stop_quarantined = False
        client = sensor.transport_client
        if client is not None:
            if hasattr(client, "retain_for_recovery"):
                client.retain_for_recovery = bool(sensor.measurement_cleanup_pending)
            client.cleanup_deadline_unix_s = time.time() + max(0, operation.deadline - time.monotonic())
            if hasattr(client, "operation_policy"):
                client.operation_policy = GatewayOperationPolicy.SENSOR_CLEANUP
        sensor.cleanup_in_progress = True
        try:
            await self.polling_stream_service.stop(sensor)
            if sensor.measurement_cleanup_pending:
                if not bool(getattr(client, "is_connected", False)):
                    reconnect = getattr(adapter, "reconnect_for_recovery", None)
                    if reconnect:
                        connected = await reconnect(sensor)
                    else:
                        reconnect = getattr(adapter, "connect_for_cleanup", None)
                        connected = client is not None and reconnect is not None and await reconnect(client)
                    if not connected:
                        raise RuntimeError("Sensor unreachable or safe reconnection unavailable")
                    # Avoid firing a new setup before STOP: the host retains its
                    # original measurement/subscription state.
                sensor.connection_status = ConnectionStatus.CONNECTED
                stop_operation = SensorOperation(min(operation.deadline, time.monotonic() + 15))
                stopped = await self._bounded(sensor.stop_stream(adapter), stop_operation)
                operation.check()
                if stopped is False:
                    raise RuntimeError("Plugin reported unsuccessful STOP")
                sensor.measurement_cleanup_pending = False
                if client is not None and hasattr(client, "retain_for_recovery"):
                    client.retain_for_recovery = False
            if bool(getattr(client, "is_connected", False)):
                if hasattr(adapter, "disconnect_sensor"):
                    disconnected = await adapter.disconnect_sensor(sensor)
                else:
                    disconnected = await adapter.disconnect(client)
                operation.check()
                if disconnected is False:
                    raise RuntimeError("Physical disconnection unconfirmed")
            sensor.connection_status = ConnectionStatus.DISCONNECTED
            # Keep an idle host until configuration retirement. Reset must not
            # erase plugin context while an unresolved STOP still needs it.
            sensor.physical_stop_quarantined = False
            if client is not None:
                client.cleanup_deadline_unix_s = None
                if hasattr(client, "operation_policy"):
                    client.operation_policy = GatewayOperationPolicy.NORMAL
        finally:
            sensor.cleanup_in_progress = False

    async def cleanup(self, sensors, deadline=None):
        deadline = min(float(deadline or float("inf")), time.time() + CLEANUP_OPERATION_TIMEOUT_S)
        results = []
        for sensor in sensors:
            operation = None
            try:
                remaining = deadline - time.time() - CLEANUP_CANCEL_RESERVE_S
                if remaining <= 0:
                    results.append({"address": sensor.address, "name": sensor.name,
                                    "status": "unconfirmed" if sensor.measurement_cleanup_pending else "failed",
                                    "error": "Cleanup operation deadline reached; sensor cleanup unfinished"})
                    continue
                if sensor.transport_client is not None:
                    sensor.transport_client.cleanup_deadline_unix_s = deadline - CLEANUP_CANCEL_RESERVE_S
                operation = SensorOperation(time.monotonic() + min(SENSOR_CLEANUP_TIMEOUT_S, remaining))
                result = await self._bounded(self._cleanup_sensor(sensor), operation)
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
            finally:
                if operation is not None:
                    operation.cancelled.set()
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
        if getattr(sensor, "physical_stop_quarantined", False):
            result.update(status="unconfirmed", error="Physical STOP timed out; sensor isolated pending reconciliation")
            return result
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
                    if client is not None and hasattr(client, "retain_for_recovery"):
                        client.retain_for_recovery = False
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
