"""STOP fault injection without changing sensor plugin contracts."""
import asyncio
import threading
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy
from nexus_n3.sensor_manager import streaming_service as module
from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.adapters.gateway_ble_client import GatewaySerialClient
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.types.connections import ConnectionStatus


def sensor(address, stop):
    return NS(address=address, name=address, location='test',
              connection_status=ConnectionStatus.CONNECTED,
              measurement_cleanup_pending=True, stop_stream=stop)


def service(sensors):
    return module.StreamingService(
        NS(group_sensors=lambda targets: {'adapter': targets}, collect_diagnostics=AsyncMock(return_value={})),
        NS(stop=AsyncMock()), Mock())


@pytest.mark.parametrize('failure', ['exception', 'false', 'timeout'])
def test_failed_first_stop_does_not_skip_second(monkeypatch, failure):
    monkeypatch.setattr(module, 'SENSOR_STOP_TIMEOUT_S', .03)
    calls = []
    async def first(_):
        calls.append('first')
        if failure == 'exception': raise RuntimeError('sensor failed')
        if failure == 'false': return False
        await asyncio.Event().wait()
    async def second(_): calls.append('second')
    first_sensor, second_sensor = sensor('first', first), sensor('second', second)
    outcomes = asyncio.run(service([]).stop([first_sensor, second_sensor], Mock()))
    assert [r['status'] for r in outcomes] == ['unconfirmed', 'stopped']
    assert calls == ['first', 'second']
    assert first_sensor.measurement_cleanup_pending
    assert not second_sensor.measurement_cleanup_pending
    if failure == 'timeout': assert first_sensor.physical_stop_quarantined


def test_expired_attempt_reports_every_target_without_commands():
    stop = AsyncMock()
    outcomes = asyncio.run(service([]).stop([sensor('a', stop), sensor('b', stop)], Mock(), deadline=time.monotonic()-1))
    assert [r['status'] for r in outcomes] == ['not_attempted', 'not_attempted']
    stop.assert_not_awaited()


def test_uart_timeout_during_stop_cannot_reset_other_subject(monkeypatch):
    gateway = GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    gateway.active_acquisition_addresses = {'subject2'}
    gateway.send = Mock()  # UART supplies no replies: not a BLE disconnect.
    gateway._recover_transport = Mock()
    gateway._handle_transport_failure = Mock()
    adapter = GatewayBLEAdapter()
    adapter.gateway_client = gateway
    adapter.ble_runtime_config = BLERuntimeConfig(backend='gateway', gateway_write_timeout_s=.01)
    client = adapter.create_transport_client('subject1')
    client.is_connected = True
    async def stop(_): await adapter.write(client, 'uuid', b'\x01')
    target = sensor('subject1', stop)
    outcomes = asyncio.run(service([]).stop([target], Mock()))
    assert outcomes[0]['status'] == 'unconfirmed'
    assert gateway.send.call_count == 1
    gateway._recover_transport.assert_not_called()
    gateway._handle_transport_failure.assert_not_called()
    assert gateway.pending_requests == {}
    assert client.is_connected
    with pytest.raises(RuntimeError, match='prohibited'):
        gateway.reset_session()


def test_late_rpc_worker_cannot_send_after_stop_deadline(monkeypatch):
    monkeypatch.setattr(module, 'SENSOR_STOP_TIMEOUT_S', .03)
    released = threading.Event()
    wire = Mock()
    gateway = GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    gateway.started = gateway.running = True
    gateway.ser = NS(write=wire, flush=Mock())
    old_errors = []
    def plugin_stop():
        assert released.wait(2)
        try: gateway.send({'type': 'gatt_write'})
        except TimeoutError as exc: old_errors.append(str(exc))
        return {'ok': True}
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    proxy.address, proxy.name, proxy.location = 'late', 'late', 'test'
    proxy.connection_status = ConnectionStatus.CONNECTED
    proxy.measurement_cleanup_pending = True
    host = NS(stop_stream=plugin_stop, transport=NS(abort=Mock()))
    proxy._ensure_client = lambda: host
    async def run():
        try:
            outcomes = await service([]).stop([proxy], Mock())
            assert outcomes[0]['status'] == 'unconfirmed'
            assert proxy.physical_stop_quarantined
            assert not proxy._stop_worker.done()
            repeated = await service([]).stop([proxy], Mock())
            assert repeated[0]['status'] == 'not_attempted'
        finally:
            released.set()
            await proxy._stop_worker
    asyncio.run(run())
    assert proxy.measurement_cleanup_pending
    assert old_errors
    wire.assert_not_called()
    host.transport.abort.assert_not_called()


def test_normal_retry_is_also_guarded_during_acquisition():
    gateway = GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    gateway.active_acquisition_addresses = {'subject2'}
    gateway.send = Mock()
    gateway._recover_transport = Mock()
    with pytest.raises(TimeoutError): gateway.write_gatt('subject1', 'uuid', '01', .001)
    assert gateway.send.call_count == 1
    gateway._recover_transport.assert_not_called()
