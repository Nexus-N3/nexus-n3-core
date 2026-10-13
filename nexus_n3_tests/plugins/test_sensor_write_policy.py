"""Write policy and failure results survive the plugin RPC boundary."""
import asyncio
import socket
import threading
from types import SimpleNamespace

import pytest

from nexus_n3.plugins.common.jsonrpc import JsonRpcConnection
from nexus_n3.plugins.runtime.sensor_host import HostAdapterProxy
from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy, SensorHostClient
from nexus_n3.sensor_manager.adapters.ble_adapter import BLEAdapter


@pytest.mark.parametrize("best_effort", [False, True])
@pytest.mark.parametrize("write_result,expected", [(False, False), (True, True), (None, True)])
def test_rpc_preserves_write_policy_and_result(best_effort, write_result, expected):
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    calls = []
    async def write(client, uuid, payload, *, best_effort=False):
        calls.append((client, uuid, payload, best_effort))
        return write_result
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    proxy._runtime_adapter = SimpleNamespace(write=write)
    proxy.transport_client = "CORE"
    proxy._manager_loop = loop
    bridge = SensorHostClient.__new__(SensorHostClient)
    bridge.proxy = proxy
    left, right = socket.socketpair()
    streams = [left.makefile("r"), left.makefile("w"), right.makefile("r"), right.makefile("w")]
    host_rpc = JsonRpcConnection(streams[0], streams[1])
    core_rpc = JsonRpcConnection(streams[2], streams[3])
    core_rpc.register_handler("adapter.write", bridge._handle_adapter_write)
    adapter = HostAdapterProxy(host_rpc)
    try:
        assert asyncio.run(adapter.write("CORE", "uuid", b"\x13\x66", best_effort=best_effort)) is expected
        assert calls == [("CORE", "uuid", b"\x13\x66", best_effort)]
    finally:
        host_rpc.close()
        core_rpc.close()
        left.shutdown(socket.SHUT_RDWR)
        right.shutdown(socket.SHUT_RDWR)
        host_rpc._reader_thread.join(timeout=2)
        core_rpc._reader_thread.join(timeout=2)
        for stream in streams:
            stream.close()
        left.close()
        right.close()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)
        loop.close()


@pytest.mark.parametrize("fails", [False, True])
def test_native_ble_best_effort_reports_success_or_failure(fails):
    async def write(uuid, payload, *, response):
        assert response is True
        if fails:
            raise TimeoutError("sensor timeout")
    device = SimpleNamespace(write_gatt_char=write)
    assert asyncio.run(BLEAdapter.write(device, "uuid", b"hr", best_effort=True)) is (not fails)
