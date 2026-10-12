"""Bounded command writes and reset/acquisition admission, without hardware."""
import json
import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
import serial

from nexus_n3.sensor_manager.adapters import gateway_ble_client as module
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.plugins.common.operations import SensorOperation, operation_scope
import time


def ready_client():
    client = module.GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    client.started = client.running = True
    client.ser = NS(write=Mock(side_effect=lambda data: len(data)), flush=Mock(side_effect=AssertionError('flush must not be used')))
    return client


def test_healthy_complete_commands_remain_ordered_without_drain():
    client = ready_client()
    for value in (1, 2): client.send({'type': 'command', 'value': value})
    assert [json.loads(call.args[0])['value'] for call in client.ser.write.call_args_list] == [1, 2]
    assert all(call.args[0].endswith(b'\n') for call in client.ser.write.call_args_list)
    client.ser.flush.assert_not_called()


def test_short_write_is_failure_not_success():
    client = ready_client()
    client.ser.write.return_value = 2
    client.ser.write.side_effect = None
    with operation_scope(SensorOperation(time.monotonic()+1)), pytest.raises(serial.SerialTimeoutException):
        client.send({'type': 'command'})
    assert not client.write_lock.locked()


def test_stalled_writer_cannot_block_scope_update_or_new_attempt():
    client = ready_client()
    entered, release = threading.Event(), threading.Event()
    def write(data):
        entered.set()
        assert release.wait(3)
        return len(data)
    client.ser.write.side_effect = write
    worker = threading.Thread(target=lambda: client.send({'type': 'command'}))
    worker.start()
    try:
        assert entered.wait(1)
        # Finalisation updates scope without acquiring the stalled I/O lock.
        client.set_acquisition_scope({'subject2'})
        assert client.active_acquisition_addresses == {'subject2'}
        with operation_scope(SensorOperation(time.monotonic()+.03)), pytest.raises(TimeoutError, match='lock deadline'):
            client.send({'type': 'command'})
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive()


def test_reset_blocks_acquisition_until_ack_and_reconciliation(monkeypatch):
    client = ready_client()
    sent, ack = threading.Event(), threading.Event()
    def write(data):
        msg = json.loads(data)
        if msg['type'] == 'reset_session':
            sent.set()
            assert ack.wait(2)
            client._route_json({'type': 'reset_session_complete', 'request_id': msg['request_id'], 'ok': True})
        return len(data)
    client.ser.write.side_effect = write
    errors = []
    def reset():
        try: client.reset_session(.5)
        except Exception as exc: errors.append(exc)
    worker = threading.Thread(target=reset)
    worker.start()
    try:
        assert sent.wait(1)
        with pytest.raises(RuntimeError, match='must complete'): client.set_acquisition_scope({'subject2'}, append=True)
        with pytest.raises(RuntimeError, match='already in progress'): client.reset_session()
    finally:
        ack.set()
        worker.join(2)
    assert not errors
    client.set_acquisition_scope({'subject2'})
    with pytest.raises(RuntimeError, match='prohibited'): client.reset_session()
    assert client.ser.write.call_count == 1


def test_reset_timeout_keeps_acquisition_blocked_until_success():
    client = ready_client()
    with pytest.raises(TimeoutError): client.reset_session(.001)
    with pytest.raises(RuntimeError, match='must complete'): client.set_acquisition_scope({'subject2'})
    def complete(data):
        msg = json.loads(data)
        client._route_json({'type': 'reset_session_complete', 'request_id': msg['request_id'], 'ok': True})
        return len(data)
    client.ser.write.side_effect = complete
    client.reset_session(.1)
    client.set_acquisition_scope({'subject2'})
    assert not client._reset_unconfirmed


def test_normal_cold_start_and_command_acknowledgements(monkeypatch):
    client = module.GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    sent = []
    def write(data):
        msg = json.loads(data)
        sent.append(msg['type'])
        response = {'reset_session': 'reset_session_complete', 'hello': 'hello_ack', 'gatt_write': 'write_complete'}[msg['type']]
        client._route_json({'type': response, 'request_id': msg['request_id'], 'ok': True})
        return len(data)
    port = NS(write=Mock(side_effect=write), flush=Mock(side_effect=AssertionError('drain')), setDTR=Mock(), setRTS=Mock(), reset_input_buffer=Mock(), close=Mock())
    monkeypatch.setattr(module.serial, 'Serial', Mock(return_value=port))
    monkeypatch.setattr(module, 'resolve_gateway_serial_port', lambda _: 'fake')
    sleeps = []
    monkeypatch.setattr(module.time, 'sleep', sleeps.append)
    monkeypatch.setattr(client, '_read_loop', lambda: None)
    try:
        client.start()
        assert sent == ['reset_session', 'hello']
        assert sleeps == [.5]  # Only the existing startup settle.
        client.set_acquisition_scope({'dot'}, append=True)
        assert client.write_gatt('dot', 'uuid', '01', .1)
        assert sent == ['reset_session', 'hello', 'gatt_write']
        assert not client._reset_unconfirmed and client._startup_owner is None
        port.flush.assert_not_called()
    finally: client.close()


def test_startup_admission_remains_reserved_through_hello(monkeypatch):
    client = module.GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    hello_sent, release = threading.Event(), threading.Event()
    errors = []

    def write(data):
        msg = json.loads(data)
        response = 'reset_session_complete'
        if msg['type'] == 'hello':
            hello_sent.set()
            assert release.wait(2)
            response = 'hello_ack'
        client._route_json({'type': response, 'request_id': msg['request_id'], 'ok': True})
        return len(data)

    port = NS(write=write, setDTR=Mock(), setRTS=Mock(), reset_input_buffer=Mock(), close=Mock())
    constructor = Mock(return_value=port)
    monkeypatch.setattr(module.serial, 'Serial', constructor)
    monkeypatch.setattr(module, 'resolve_gateway_serial_port', lambda _: 'fake')
    monkeypatch.setattr(module.time, 'sleep', lambda _: None)
    monkeypatch.setattr(client, '_read_loop', lambda: None)

    def start():
        try:
            client.start()
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=start)
    worker.start()
    try:
        assert hello_sent.wait(1)
        assert client.started and client._reset_owner is None
        with pytest.raises(RuntimeError, match='must complete'):
            client.set_acquisition_scope({'dot'}, append=True)
        with pytest.raises(RuntimeError, match='startup already in progress'):
            client.start()
        with pytest.raises(RuntimeError, match='startup already in progress'):
            client.reset_session()
    finally:
        release.set()
        worker.join(2)
        client.close()
    assert not worker.is_alive() and not errors
    assert constructor.call_count == 1
    assert constructor.call_args.kwargs['write_timeout'] == 1.0


def test_startup_cannot_touch_serial_during_active_acquisition(monkeypatch):
    client = module.GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    client.set_acquisition_scope({'subject2'})
    constructor = Mock()
    monkeypatch.setattr(module.serial, 'Serial', constructor)
    with pytest.raises(RuntimeError, match='prohibited'): client.start()
    constructor.assert_not_called()


def test_bluetooth_fixture_prevents_real_system_bus_access(monkeypatch):
    from nexus_n3.sensor_manager.SensorManager import SensorManager
    from nexus_n3.sensor_manager.utils import utils
    bus = Mock(side_effect=AssertionError('real BlueZ access'))
    monkeypatch.setattr(utils, 'DBUS_AVAILABLE', True)
    monkeypatch.setattr(utils, 'dbus', NS(SystemBus=bus), raising=False)
    manager = SensorManager()
    try:
        utils.remove_devices(['fake'])
        bus.assert_not_called()
    finally:
        manager.stop_manager()
        manager.loop.close()


def test_healthy_connection_stream_stop_disconnect_and_next_connection():
    from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
    from nexus_n3.sensor_manager.streaming_service import StreamingService
    from nexus_n3.sensor_manager.types.connections import ConnectionStatus
    import asyncio
    from unittest.mock import AsyncMock
    client = ready_client()
    commands = []
    def write(data):
        msg = json.loads(data)
        commands.append(msg['type'])
        response = {'request_id': msg['request_id']}
        if msg['type'] == 'connect_addresses':
            response.update(type='sensor_connected', address=msg['addresses'][0], sensor_id=1)
        elif msg['type'] == 'disconnect_addresses':
            response.update(type='sensor_disconnected', address=msg['addresses'][0], reason=19)
        else:
            response.update(type='write_complete', ok=True)
        client._observe_json(response)
        client._route_json(response)
        return len(data)
    client.ser.write.side_effect = write
    adapter = GatewayBLEAdapter()
    adapter.gateway_client = client
    transport = adapter.create_transport_client('AA')
    async def start(_): return await adapter.write(transport, 'plugin-owned-start', b'\x02')
    async def stop(_): return await adapter.write(transport, 'plugin-owned-stop', b'\x01')
    target = NS(address='AA', name='test', location='test', connection_status=ConnectionStatus.CONNECTED,
                measurement_cleanup_pending=False, physical_stop_quarantined=False, start_stream=start, stop_stream=stop)
    stream = StreamingService(NS(group_sensors=lambda targets: {adapter: targets},
                                 collect_diagnostics=AsyncMock(return_value={})), NS(stop=AsyncMock()), Mock())
    async def normal_session():
        assert await adapter.connect(transport)
        await stream.start([target], Mock())
        assert client.active_acquisition_addresses == {'AA'}
        assert target.measurement_cleanup_pending and not target.physical_stop_quarantined
        outcomes = await stream.stop([target], Mock())
        assert outcomes == [{'address': 'AA', 'status': 'stopped'}]
        assert not target.measurement_cleanup_pending and not target.physical_stop_quarantined
        client.set_acquisition_scope(set())
        assert await adapter.disconnect(transport)
        replacement = adapter.create_transport_client('AA')
        assert await adapter.connect(replacement)
        assert replacement.is_connected
    asyncio.run(normal_session())
    assert commands == ['connect_addresses', 'gatt_write', 'gatt_write', 'disconnect_addresses', 'connect_addresses']
    assert not client.pending_requests and not client._reset_unconfirmed


def test_raw_reset_cannot_bypass_admission():
    client = ready_client()
    with pytest.raises(RuntimeError, match='admission'): client.send({'type': 'reset_session'})
    client.ser.write.assert_not_called()


def test_normal_retry_reserves_acquisition_before_transport_close(monkeypatch):
    client = ready_client()
    closing, release = threading.Event(), threading.Event()
    close_transport = client._close_transport
    errors, commands, results = [], [], []

    def close():
        closing.set()
        assert release.wait(2)
        close_transport()

    def write(data):
        msg = json.loads(data)
        commands.append(msg['type'])
        response = {'reset_session': 'reset_session_complete', 'hello': 'hello_ack'}[msg['type']]
        client._route_json({'type': response, 'request_id': msg['request_id'], 'ok': True})
        return len(data)

    port = NS(write=write, open=Mock(), setDTR=Mock(), setRTS=Mock(), reset_input_buffer=Mock(), close=Mock())
    monkeypatch.setattr(module.serial, 'Serial', Mock(return_value=port))
    monkeypatch.setattr(module, 'resolve_gateway_serial_port', lambda _: 'fake')
    monkeypatch.setattr(module.time, 'sleep', lambda _: None)
    monkeypatch.setattr(client, '_read_loop', lambda: None)
    monkeypatch.setattr(client, '_close_transport', close)
    monkeypatch.setattr(client, 'get_connection_inventory', Mock(side_effect=[serial.SerialException('UART down'), {'connected_addresses': [], 'reset_confirmed': True}]))
    attempt = Mock(side_effect=[TimeoutError('Timed out waiting for gateway response'), True])

    def retry():
        try:
            results.append(client._execute_with_transport_retry('normal', attempt))
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=retry)
    worker.start()
    try:
        assert closing.wait(1)
        with pytest.raises(RuntimeError, match='must complete'):
            client.set_acquisition_scope({'dot'}, append=True)
    finally:
        release.set()
        worker.join(2)
    try:
        assert not worker.is_alive() and not errors and results == [True]
        assert attempt.call_count == 2 and commands == []
        client.set_acquisition_scope({'dot'})
        assert not client._reset_unconfirmed
    finally:
        client.close()


def test_core_initialization_discovery_connection_and_reinitialization(tmp_path, monkeypatch):
    from nexus_n3.core import core as core_module
    from nexus_n3.file_manager.FileManager import FileManager
    from nexus_n3.sensor_manager.types.connections import ConnectionStatus
    from nexus_n3_tests.sensor_manager.test_reinitialization_lifecycle import (
        _GatewayState, _LifecycleAdapter, _LifecycleSensor, _submit,
    )

    state = _GatewayState()
    monkeypatch.setattr(_LifecycleAdapter, 'state', state, raising=False)
    monkeypatch.setattr('nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class', lambda *_: _LifecycleAdapter)
    monkeypatch.setattr('nexus_n3.core.orchestrators.subject_graph.resolve_installed_sensor_class',
                        lambda _: lambda _device: _LifecycleSensor('AA'))
    # Exercise real Core, storage and SensorManager initialization; no plugin
    # process or algorithm is required for this connection lifecycle check.
    monkeypatch.setattr(core_module, 'ComputeOrchestrator', Mock(return_value=Mock()))
    monkeypatch.setattr('nexus_n3.core.orchestrators.storage_orchestrator.FileManager',
                        lambda site, **kwargs: FileManager(site, base_dir=tmp_path, **kwargs))
    bus = Mock()
    core = core_module.Core('init-regression', system_event_bus=bus,
                            ble_runtime_config=BLERuntimeConfig(backend='bleak'))
    config = [{'subject_id': 'subject1', 'sensors': [
        {'local_name': 'Lifecycle Sensor', 'number_of': 1, 'locations': ['CHEST']},
    ]}]
    manager = core.sensor_orch.manager
    try:
        for _ in range(2):
            core.init_core(config, init_label='healthy')
            assert len(core.subjects) == 1 and len(manager.sensors) == 1
            assert manager.sensor_meta[manager.sensors[0]]['subject_id'] == 'subject1'
            _submit(manager, 'discover_and_connect')
            assert manager.sensors[0].connection_status is ConnectionStatus.CONNECTED
            assert state.connected == {'AA'}
            _submit(manager, 'start_all')
            assert manager.sensors[0].streaming
            assert manager.stop_all().result(timeout=3) == [{'address': 'AA', 'status': 'stopped'}]
            assert not manager.sensors[0].physical_stop_quarantined
        assert sum(call.args[0]['type'] == 'system_initialized' for call in bus.emit.call_args_list) == 2
        assert not any(call.args[0]['type'] == 'error' for call in bus.emit.call_args_list)
    finally:
        core.stop_manager()
        manager.loop.close()
