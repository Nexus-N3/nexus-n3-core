import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
import pytest
import serial
from nexus_n3.sensor_manager.adapters.gateway_ble_client import GatewaySerialClient, GatewayOperationPolicy
from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.cleanup_service import CleanupService
from nexus_n3.sensor_manager.discovery_service import _build_disconnect_callback
from nexus_n3.sensor_manager.types.connections import ConnectionStatus
SAFE = GatewayOperationPolicy.SENSOR_CLEANUP

@pytest.fixture
def gateway(monkeypatch):
    client = GatewaySerialClient(BLERuntimeConfig(backend='gateway'))
    for name in ('start', '_close_transport', '_handle_transport_failure', '_recover_transport', 'reset_session'):
        monkeypatch.setattr(client, name, Mock())
    return client

@pytest.mark.parametrize('operation', ['connect', 'write', 'disconnect', 'read', 'subscribe', 'unsubscribe'])
def test_safe_timeout_one_attempt_no_shared_recovery(gateway, monkeypatch, operation):
    send = Mock()
    monkeypatch.setattr(gateway, 'send', send)
    with pytest.raises(TimeoutError):
        if operation == 'connect': gateway.connect(['DOT'], .001, policy=SAFE)
        elif operation == 'write': gateway.write_gatt('DOT', 'uuid', '01', .001, policy=SAFE)
        elif operation == 'read': gateway.read_gatt('DOT', 'uuid', .001, policy=SAFE)
        elif operation == 'subscribe':
            monkeypatch.setattr(gateway, 'assert_connected', Mock())
            monkeypatch.setattr(gateway, 'is_disconnected', Mock(return_value=False))
            gateway.subscribe_with_retry('DOT', 'uuid', .001, policy=SAFE)
        elif operation == 'unsubscribe': gateway.unsubscribe('DOT', 'uuid', .001, policy=SAFE)
        else: gateway.disconnect(['DOT'], .001, policy=SAFE)
    assert send.call_count == 1 and gateway.pending_requests == {}
    for name in ('start', '_close_transport', '_handle_transport_failure', '_recover_transport', 'reset_session'):
        getattr(gateway, name).assert_not_called()

@pytest.mark.parametrize('error', [serial.SerialException('removed'), OSError('broken serial')])
def test_serial_failure_identifiable(gateway, monkeypatch, error):
    monkeypatch.setattr(gateway, 'send', Mock(side_effect=error))
    with pytest.raises(type(error)) as caught:
        gateway.connect(['DOT'], .001, policy=SAFE)
    assert caught.value is error
    gateway._recover_transport.assert_not_called()

@pytest.mark.parametrize('error,expected', [(TimeoutError('sensor timeout'), False), (OSError('serial'), True)])
def test_timeout_classified_before_oserror(error, expected):
    assert isinstance(TimeoutError(), OSError)
    assert GatewaySerialClient._is_transport_retryable(error) == expected


def test_safe_send_does_not_start_unavailable_transport(gateway):
    with pytest.raises(serial.SerialException):
        gateway._execute_with_transport_retry('connect', lambda: gateway.send({}), policy=SAFE)
    gateway.start.assert_not_called()
    gateway._close_transport.assert_not_called()


def scenario(connected=True, pending=True, reconnect_error=None, stop_error=None):
    trace = []
    client = NS(is_connected=connected, operation_policy=GatewayOperationPolicy.NORMAL)
    sensor = NS(address='DOT', name='Movella DOT', location='RIGHT_ANKLE',
        connection_status=ConnectionStatus.CONNECTED if connected else ConnectionStatus.DISCONNECTED,
        transport_client=client, measurement_cleanup_pending=pending,
        cleanup_in_progress=False)
    sensor.set_connection_status = lambda status: setattr(sensor, 'connection_status', status)
    sensor.set_transport_client = lambda value: setattr(sensor, 'transport_client', value)
    async def reconnect(client):
        assert client.operation_policy == SAFE
        trace.append('reconnect')
        if reconnect_error: raise reconnect_error
        client.is_connected = True
        return True
    async def stop(adapter):
        assert client.operation_policy == SAFE
        trace.append('stop')
        if stop_error: raise stop_error
    async def disconnect(client):
        assert client.operation_policy == SAFE
        trace.append('disconnect')
        client.is_connected = False
        return True
    sensor.stop_stream = stop
    sensor.close_host = lambda: trace.append('close_host')
    adapter = NS(connect_for_cleanup=reconnect, disconnect=disconnect, release_transport_client=lambda _: trace.append('release'))
    service = CleanupService(NS(for_sensor=lambda _: adapter), NS(stop=AsyncMock()))
    return service, sensor, trace

@pytest.mark.parametrize('connected,expected', [(True, ['stop', 'disconnect']), (False, ['reconnect', 'stop', 'disconnect'])])
def test_cleanup_order_idempotence(connected, expected):
    service, sensor, trace = scenario(connected=connected)
    assert asyncio.run(service.cleanup([sensor]))[0]['status'] == 'cleaned'
    assert trace[:len(expected)] == expected
    assert not sensor.measurement_cleanup_pending
    trace.clear()
    assert asyncio.run(service.cleanup([sensor]))[0]['status'] == 'already_clean'
    assert not any(step in trace for step in ['reconnect', 'stop', 'disconnect'])

@pytest.mark.parametrize('failure', ['reconnect', 'stop'])
def test_failure_retains_obligation_and_other_sensors_cleanup(failure):
    service, lost, trace = scenario(connected=False, **{f'{failure}_error': TimeoutError('sensor timeout')})
    other_service, healthy, healthy_trace = scenario()
    healthy.address = 'OTHER'
    lost_adapter, healthy_adapter = service.adapter_pool.for_sensor(lost), other_service.adapter_pool.for_sensor(healthy)
    service.adapter_pool.for_sensor = lambda sensor: lost_adapter if sensor is lost else healthy_adapter
    results = asyncio.run(service.cleanup([lost, healthy]))
    assert [r['status'] for r in results] == ['unconfirmed', 'cleaned']
    assert lost.measurement_cleanup_pending and trace.count('reconnect') == 1
    assert healthy_trace[:2] == ['stop', 'disconnect']


def test_already_stopped_no_reconnect():
    service, sensor, trace = scenario(connected=False, pending=False)
    assert asyncio.run(service.cleanup([sensor]))[0]['status'] == 'already_clean'
    assert 'reconnect' not in trace and 'stop' not in trace


def test_no_plugin_capabilities_needed():
    service, sensor, trace = scenario(connected=False)
    assert not hasattr(sensor, 'capabilities')
    assert asyncio.run(service.cleanup([sensor]))[0]['status'] == 'cleaned'
    assert trace[:3] == ['reconnect', 'stop', 'disconnect']


def test_unexpected_disconnect_preserves_obligation_identity():
    _, sensor, trace = scenario()
    sensor._emit = Mock()
    client = sensor.transport_client
    client.address, client.suppress_disconnect_event = sensor.address, False
    client.disconnected_callback = _build_disconnect_callback(sensor)
    adapter = GatewayBLEAdapter()
    adapter.transport_clients[sensor.address] = client
    adapter._handle_sensor_disconnected({'address': sensor.address})
    assert sensor.connection_status == ConnectionStatus.DISCONNECTED
    assert sensor.measurement_cleanup_pending and sensor.transport_client is client
    assert not client.is_connected and trace == [] and sensor.last_disconnect_was_connected
    adapter._handle_sensor_disconnected({'address': sensor.address})
    assert not sensor.last_disconnect_was_connected


def test_controller_scoping_and_whole_operation_completion():
    from nexus_n3.sensor_manager.sensor_controller import SensorController
    service, lost, trace = scenario(connected=False)
    _, other, _ = scenario()
    other.address = 'OTHER'
    controller = SensorController.__new__(SensorController)
    controller._sensors_ref = lambda: [lost, other]
    controller.cleanup_service = service
    controller.emit_to_client = Mock()
    context = {'correlation_id': 'cleanup-1', 'expected_node_ids': ['worker']}
    results = asyncio.run(controller.handle_disconnect_addresses(['dot'], context))
    assert [r['address'] for r in results] == ['DOT']
    assert other.measurement_cleanup_pending and other.transport_client.is_connected
    completion = controller.emit_to_client.call_args_list[-1]
    assert completion.args == ('on_cleanup_complete', {**context, 'results': results})
    assert trace[:3] == ['reconnect', 'stop', 'disconnect']


def test_stream_stop_excludes_lost_sensor_and_retains_obligation():
    from nexus_n3.sensor_manager.sensor_controller import SensorController
    from nexus_n3.sensor_manager.streaming_service import StreamingService
    _, lost, lost_trace = scenario(connected=False)
    _, healthy, trace = scenario()
    # Lost targets are reported without clearing their physical STOP obligation.
    healthy.stop_stream = AsyncMock()
    pool = NS(group_sensors=lambda sensors: {'adapter': sensors}, collect_diagnostics=AsyncMock(return_value={}))
    stream = StreamingService(pool, NS(stop=AsyncMock()), Mock())
    controller = SensorController.__new__(SensorController)
    controller.get_connected_sensors = lambda: [healthy]
    controller._sensors_ref = lambda: [lost, healthy]
    controller.streaming_service = stream
    controller.emit_to_client = Mock()
    asyncio.run(controller.handle_stop_all())
    healthy.stop_stream.assert_awaited_once()
    assert not healthy.measurement_cleanup_pending
    assert lost.measurement_cleanup_pending and lost_trace == []


def test_start_marks_obligation_before_uncertain_command():
    from nexus_n3.sensor_manager.streaming_service import StreamingService
    _, sensor, _ = scenario(pending=False)
    async def failed_start(_):
        assert sensor.measurement_cleanup_pending
        raise TimeoutError('start acknowledgement lost')
    sensor.start_stream = failed_start
    stream = StreamingService(NS(), NS(), Mock())
    with pytest.raises(TimeoutError):
        asyncio.run(stream._start_sensor_stream(sensor, NS()))
    assert sensor.measurement_cleanup_pending


def test_master_cleanup_snapshots_subject_owners_not_connections():
    from nexus_n3.distributed.master_node import MasterNode
    master = MasterNode.__new__(MasterNode)
    master.node_id = 'master'
    master.registry = NS(get_subjects=lambda: {
        'Subject_1': {'assigned_node': 'worker-1'}, 'Subject_2': {'assigned_node': 'master'},
    })
    master.send_command = Mock()
    handler = NS(_handle_local=Mock())
    master.dispatch_command({'type': 'disconnect_subjects', 'payload': {
        'subject_ids': ['Subject_1'], 'correlation_id': 'cleanup-1',
    }}, handler)
    handler._handle_local.assert_not_called()
    payload = master.send_command.call_args.args[0]['payload']
    assert payload.pop('cleanup_deadline_unix_s') > 0
    assert payload == {'subject_ids': ['Subject_1'], 'correlation_id': 'cleanup-1', 'expected_node_ids': ['worker-1']}
    master.send_command.reset_mock()
    master.dispatch_command({'type': 'disconnect_all', 'payload': {'correlation_id': 'cleanup-2'}}, handler)
    assert master.send_command.call_args.args[0]['payload']['expected_node_ids'] == ['master', 'worker-1']
    assert handler._handle_local.call_args.args[1]['expected_node_ids'] == ['master', 'worker-1']


def test_core_completion_uses_queued_correlation_not_mutable_event_context():
    from nexus_n3.core.core import Core
    core = Core.__new__(Core)
    core.node_id = 'worker'
    core.pending_correlation_id = None
    core.system_event_bus = NS(emit=Mock())
    payload = {'correlation_id': 'cleanup-1', 'expected_node_ids': ['worker', 'master'],
               'results': [{'address': 'DOT', 'status': 'unconfirmed'}]}
    core._on_cleanup_complete(payload)
    event = core.system_event_bus.emit.call_args.args[0]
    assert event['type'] == 'sensor_cleanup_complete'
    assert event['payload'] == {**payload, 'node_id': 'worker'}


@pytest.mark.parametrize('operation', ['connect', 'write', 'read', 'subscribe', 'unsubscribe', 'disconnect'])
def test_real_adapter_bridge_uses_safe_policy_and_preserves_other_stream(gateway, monkeypatch, operation):
    adapter = GatewayBLEAdapter()
    adapter.gateway_client = gateway
    device = adapter.create_transport_client('DOT')
    other = adapter.create_transport_client('OTHER')
    other.is_connected = True
    other.notify_callbacks['uuid'] = Mock()
    gateway.started = gateway.running = True
    gateway.ser = Mock()
    gateway.ser.write.side_effect = TimeoutError('Timed out waiting for gateway response')
    device.operation_policy = SAFE
    monkeypatch.setattr(gateway, 'assert_connected', Mock())
    monkeypatch.setattr(gateway, 'is_disconnected', Mock(return_value=False))
    async def run():
        if operation == 'connect': await adapter.connect_for_cleanup(device)
        elif operation == 'write': await adapter.write(device, 'uuid', b'\x01')
        elif operation == 'read': await adapter.read(device, 'uuid')
        elif operation == 'subscribe': await adapter.set_notify_callback(device, 'uuid', Mock())
        elif operation == 'unsubscribe': await adapter.unset_notify_callback(device, 'uuid')
        else: await adapter.disconnect(device)
    with pytest.raises(TimeoutError):
        asyncio.run(run())
    assert gateway.ser.write.call_count == 1
    assert other.is_connected and other.notify_callbacks
    for name in ('start', '_close_transport', '_recover_transport', 'reset_session'):
        getattr(gateway, name).assert_not_called()


def test_plugin_rpc_adapter_bridge_preserves_safe_write_policy(gateway):
    from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy
    adapter = GatewayBLEAdapter()
    adapter.gateway_client = gateway
    client = adapter.create_transport_client('DOT')
    client.operation_policy = SAFE
    gateway.started = gateway.running = True
    gateway.ser = Mock()
    gateway.ser.write.side_effect = TimeoutError('Timed out waiting for gateway response')
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    proxy.transport_client = client
    proxy._runtime_adapter = adapter
    async def run():
        proxy._manager_loop = asyncio.get_running_loop()
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(proxy._adapter_request, 'write', 'uuid', b'\x01')
    asyncio.run(run())
    assert gateway.ser.write.call_count == 1
    gateway.start.assert_not_called()
    gateway._recover_transport.assert_not_called()
    gateway._close_transport.assert_not_called()


def test_adapter_without_safe_reconnect_retains_obligation():
    service, sensor, trace = scenario(connected=False)
    del service.adapter_pool.for_sensor(sensor).connect_for_cleanup
    result = asyncio.run(service.cleanup([sensor]))[0]
    assert result['status'] == 'unconfirmed'
    assert result['error'] == 'Safe cleanup reconnect unavailable'
    assert sensor.measurement_cleanup_pending and trace == []


def test_current_transport_state_overrides_stale_sensor_status():
    service, sensor, trace = scenario(connected=False)
    sensor.connection_status = ConnectionStatus.CONNECTED
    assert asyncio.run(service.cleanup([sensor]))[0]['status'] == 'cleaned'
    assert trace[:3] == ['reconnect', 'stop', 'disconnect']


def test_explicit_stop_failure_is_unconfirmed_and_disconnect_still_runs():
    service, sensor, trace = scenario()
    sensor.stop_stream = AsyncMock(return_value=False)
    assert asyncio.run(service.cleanup([sensor]))[0]['status'] == 'unconfirmed'
    assert sensor.measurement_cleanup_pending
    assert trace == ['disconnect']
    assert sensor.transport_client.operation_policy == SAFE


def test_local_teardown_is_bounded_and_other_targets_continue(monkeypatch):
    import nexus_n3.sensor_manager.cleanup_service as module
    monkeypatch.setattr(module, 'SENSOR_CLEANUP_TIMEOUT_S', .01)
    service, sensor, trace = scenario(connected=False)
    _, other, _ = scenario()
    async def polling_stop(target):
        if target is sensor:
            await asyncio.Event().wait()
    service.polling_stream_service.stop = polling_stop
    results = asyncio.run(service.cleanup([sensor, other]))
    assert [result['status'] for result in results] == ['unconfirmed', 'cleaned']
    assert sensor.measurement_cleanup_pending


def test_master_announces_missing_participants_before_dispatch():
    from nexus_n3.distributed.master_node import MasterNode
    master = MasterNode.__new__(MasterNode)
    master.node_id = 'master'
    master.registry = NS(get_subjects=lambda: {'S': {'assigned_node': 'missing-worker'}})
    master.system_event_bus = Mock()
    master.send_command = Mock()
    master.dispatch_command({'type': 'disconnect_all', 'payload': {'correlation_id': 'c'}})
    event = master.system_event_bus.emit.call_args.args[0]
    assert event['payload'].pop('cleanup_deadline_unix_s') > 0
    assert event == {
        'type': 'sensor_cleanup_started',
        'payload': {'correlation_id': 'c', 'expected_node_ids': ['missing-worker']},
    }


def test_cleanup_rpc_preserves_explicit_plugin_stop_failure():
    from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy
    # Use the proxy's actual stop bridge with a minimal host client.
    proxy = NS(_ensure_client=lambda: NS(stop_stream=lambda: {'ok': True, 'result': False}))
    assert asyncio.run(InstalledSensorProxy.stop_stream(proxy, NS())) is False


def test_catalog_and_core_have_no_plugin_cleanup_opt_in():
    from pathlib import Path
    root = Path(__file__).resolve().parents[3]
    removed = '_'.join(('cleanup', 'requires', 'reconnect', 'to', 'stop'))
    for directory in (root / 'nexus-n3-core/nexus_n3', root / 'nexus-n3-plugin-catalog/sensors'):
        for path in directory.rglob('*'):
            if path.suffix in ('.py', '.yaml', '.json'):
                assert removed not in path.read_text(), path


@pytest.mark.parametrize('operation,expected', [('connect', 20), ('write', 3), ('read', 3), ('subscribe', 3), ('unsubscribe', 3), ('disconnect', 5)])
def test_cleanup_timeouts_are_short_sensor_scoped_bounds(gateway, monkeypatch, operation, expected):
    monkeypatch.setattr(gateway, 'send', Mock())
    monkeypatch.setattr(gateway, 'assert_connected', Mock())
    monkeypatch.setattr(gateway, 'is_disconnected', Mock(return_value=False))
    timeouts = []
    def fail(request_id, queue, response_type, timeout_s):
        timeouts.append(timeout_s)
        raise TimeoutError('cleanup deadline')
    monkeypatch.setattr(gateway, '_wait_for_success', fail)
    def fail_message(queue, deadline):
        import time
        timeouts.append(round(deadline - time.time(), 2))
        raise TimeoutError('cleanup deadline')
    monkeypatch.setattr(gateway, '_wait_for_message', fail_message)
    with pytest.raises(TimeoutError):
        if operation == 'connect': gateway.connect(['S'], 100, policy=SAFE)
        elif operation == 'write': gateway.write_gatt('S', 'uuid', '01', 100, policy=SAFE)
        elif operation == 'read': gateway.read_gatt('S', 'uuid', 100, policy=SAFE)
        elif operation == 'subscribe': gateway.subscribe_with_retry('S', 'uuid', 100, policy=SAFE)
        elif operation == 'unsubscribe': gateway.unsubscribe('S', 'uuid', 100, policy=SAFE)
        else: gateway.disconnect(['S'], 100, policy=SAFE)
    assert timeouts == [expected]
    assert gateway.send.call_count == 1
    gateway._recover_transport.assert_not_called()


def test_plugin_rpc_subscription_uses_cleanup_policy(gateway):
    from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy
    adapter = GatewayBLEAdapter()
    adapter.gateway_client = gateway
    client = adapter.create_transport_client('S')
    client.operation_policy = SAFE
    gateway.started = gateway.running = True
    gateway.ser = Mock()
    gateway.ser.write.side_effect = TimeoutError('sensor timeout')
    gateway.assert_connected = Mock()
    gateway.is_disconnected = Mock(return_value=False)
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    proxy.transport_client = client
    proxy._runtime_adapter = adapter
    proxy._adapter_callbacks = {}
    async def run():
        proxy._manager_loop = asyncio.get_running_loop()
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(proxy._register_host_callback, callback_id='stop-response',
                                    notify_uuid='uuid', indicate=True, transport=Mock())
    asyncio.run(run())
    assert gateway.ser.write.call_count == 1
    for name in ('start', '_close_transport', '_handle_transport_failure', '_recover_transport', 'reset_session'):
        getattr(gateway, name).assert_not_called()



def test_hard_operation_deadline_stops_new_work_preserves_results_and_aborts_active(monkeypatch):
    import nexus_n3.sensor_manager.cleanup_service as module
    import time
    monkeypatch.setattr(module, 'CLEANUP_CANCEL_RESERVE_S', .1)
    service, first, trace = scenario()
    _, stuck, _ = scenario()
    _, never_started, _ = scenario()
    stuck.address = 'STUCK'
    never_started.address = 'NOT_STARTED'
    abort = stuck.abort_cleanup = Mock()
    async def stop(target):
        if target is stuck:
            await asyncio.Event().wait()
    service.polling_stream_service.stop = stop
    result = asyncio.run(service.cleanup([first, stuck, never_started], deadline=time.time() + .3))
    assert [r['status'] for r in result] == ['cleaned', 'unconfirmed', 'unconfirmed']
    assert trace[:2] == ['stop', 'disconnect']
    abort.assert_called_once()
    assert stuck.measurement_cleanup_pending and never_started.measurement_cleanup_pending
    assert never_started.transport_client.is_connected


def test_abort_cancels_only_this_sensor_adapter_work_and_retains_host_state():
    from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    future = Mock()
    transport = NS(abort=Mock())
    client = NS(transport=transport)
    proxy._plugin_client = client
    proxy._adapter_futures = {future}
    proxy.abort_cleanup()
    future.cancel.assert_called_once()
    transport.abort.assert_not_called()
    assert proxy._plugin_client is client and proxy._cleanup_aborted


def test_gateway_cleanup_cannot_start_operations_after_deadline():
    import time
    adapter = GatewayBLEAdapter()
    client = adapter.create_transport_client('S')
    client.operation_policy = SAFE
    client.cleanup_deadline_unix_s = time.time() - 1
    adapter.gateway_client.connect = Mock()
    with pytest.raises(TimeoutError):
        asyncio.run(adapter.connect_for_cleanup(client))
    adapter.gateway_client.connect.assert_not_called()


def test_plugin_reported_timeout_keeps_existing_host_state_for_later_cleanup():
    service, sensor, trace = scenario(stop_error=TimeoutError('GATT response unavailable'))
    sensor.abort_cleanup = Mock()
    result = asyncio.run(service.cleanup([sensor]))[0]
    assert result['status'] == 'unconfirmed'
    assert sensor.measurement_cleanup_pending and trace == ['stop', 'disconnect']
    sensor.abort_cleanup.assert_not_called()


def test_stalled_host_abort_kills_only_its_process_and_releases_rpc_waiters():
    from nexus_n3.plugins.runtime.transport import StdioJsonRpcTransport
    transport = StdioJsonRpcTransport.__new__(StdioJsonRpcTransport)
    transport._rpc = Mock()
    transport._process = Mock()
    transport._process.poll.return_value = None
    transport.abort()
    transport._rpc.close.assert_called_once()
    transport._rpc.request.assert_not_called()
    transport._process.kill.assert_called_once()
    transport._process.wait.assert_called_once_with(timeout=1.0)
