"""Core recovery through the production gateway protocol, without hardware."""
import asyncio
import json
import time
import threading
from pathlib import Path
from concurrent.futures import Future
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest
import serial

from nexus_n3.core import core as core_module
from nexus_n3.file_manager.FileManager import FileManager
from nexus_n3.gateway.messaging.message_handler import MessageHandler
from nexus_n3.plugins.common.operations import SensorOperation, operation_scope
from nexus_n3.sensor_manager.adapters import gateway_ble_client as gateway_module
from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.sensor_handle import SensorBase
from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from nexus_n3_tests.sensor_manager.test_reinitialization_lifecycle import _submit


class Sensor(SensorBase):
    def __init__(self, _device):
        super().__init__(NS(local_name='Recovery Sensor'), {
            'sensor': {'adapter': 'BLE'}, 'events': ['on_data'],
            'locations': {'supported': ['CHEST']},
        })
        self.stop_context = None
        self.stop_calls = 0
        self.host_closed = False
        self.control_response = asyncio.Event()

    async def setup(self, adapter, **kwargs):
        loop = asyncio.get_running_loop()
        def reply(_uuid, _data):
            loop.call_soon_threadsafe(self.control_response.set)
        await adapter.set_notify_callback(self.transport_client, 'plugin-control', reply, indicate=True)

    async def start_stream(self, adapter):
        self.stop_context = object()
        return await adapter.write(self.transport_client, 'plugin-start', b'start')

    async def stop_stream(self, adapter):
        assert self.stop_context is not None, 'original plugin context must survive'
        assert self.connection_status is ConnectionStatus.CONNECTED
        self.stop_calls += 1
        self.control_response.clear()
        result = await adapter.write(self.transport_client, 'plugin-stop', b'stop')
        await asyncio.wait_for(self.control_response.wait(), timeout=.2)
        return result

    def close_host(self):
        self.host_closed = True


class UART:
    def __init__(self, gateway):
        self.gateway = gateway
        self.links = set()
        self.measuring = set()
        self.commands = []
        self.subscriptions = set()
        self.available = True
        self.fail_stop = False
        self.dtr = self.rts = True
        self.port = 'fake'
        self.open = Mock()
        self.close = Mock()

    def write(self, data):
        msg = json.loads(data)
        self.commands.append(msg)
        if not self.available:
            raise serial.SerialException('UART unavailable')
        reply = {'request_id': msg['request_id']}
        kind = msg['type']
        if kind == 'get_status':
            reply.update(type='status', gateway={'state': 'ready', 'connected_count': len(self.links),
                                                'connected_addresses': sorted(self.links)})
        elif kind == 'connect_addresses':
            address = msg['addresses'][0]
            self.links.add(address)
            reply.update(type='sensor_connected', address=address, sensor_id=1)
        elif kind == 'disconnect_addresses':
            address = msg['addresses'][0]
            self.links.discard(address)
            self.subscriptions = {entry for entry in self.subscriptions if entry[0] != address}
            reply.update(type='sensor_disconnected', address=address)
        elif kind == 'subscribe':
            self.subscriptions.add((msg['address'], msg['characteristic_uuid']))
            reply.update(type='subscribe_complete', ok=True)
        elif kind == 'gatt_write':
            address = msg['address']
            if msg['characteristic_uuid'] == 'plugin-stop':
                if self.fail_stop:
                    return len(data)  # Firmware operation already submitted; no ACK.
                self.measuring.discard(address)
            else:
                self.measuring.add(address)
            reply.update(type='write_complete', ok=True)
        else:
            raise AssertionError(f'Unexpected gateway-wide command: {kind}')
        self.gateway._observe_json(reply)
        self.gateway._route_json(reply)
        if kind == 'gatt_write' and msg['characteristic_uuid'] == 'plugin-stop' and ('AA', 'plugin-control') in self.subscriptions:
            self.gateway._route_json({'type': 'notification', 'address': 'AA',
                                      'characteristic_uuid': 'plugin-control', 'payload_hex': '01'})
        return len(data)


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    config = BLERuntimeConfig(backend='gateway', gateway_write_timeout_s=.03)
    adapter = GatewayBLEAdapter()
    adapter.ble_runtime_config = config
    gateway = adapter.gateway_client
    gateway.started = gateway.running = True
    uart = UART(gateway)
    gateway.ser = uart
    monkeypatch.setattr(gateway_module.serial, 'Serial', Mock(return_value=uart))
    monkeypatch.setattr(gateway_module, 'resolve_gateway_serial_port', lambda _: 'fake')
    monkeypatch.setattr(gateway, '_read_loop', lambda: None)
    monkeypatch.setattr('nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class', lambda *_: lambda: adapter)
    monkeypatch.setattr('nexus_n3.core.orchestrators.subject_graph.resolve_installed_sensor_class', lambda _: Sensor)
    compute = Mock()
    compute.run_consolidation.return_value = []
    monkeypatch.setattr(core_module, 'ComputeOrchestrator', Mock(return_value=compute))
    monkeypatch.setattr('nexus_n3.core.orchestrators.storage_orchestrator.FileManager',
                        lambda site, **kwargs: FileManager(site, base_dir=tmp_path, **kwargs))
    discovered = []

    async def discover(requested, timeout=5):
        discovered.append([sensor.address for sensor in requested])
        return {'AA': (NS(address='AA', name='Recovery Sensor'),
                       NS(local_name='Recovery Sensor', service_uuids=()))}

    monkeypatch.setattr(adapter, 'discover_devices', discover)
    events = []
    core = core_module.Core('recovery-test', NS(emit=events.append), config)
    manager = core.sensor_orch.manager
    monkeypatch.setattr(manager.adapter_pool, 'collect_diagnostics', AsyncMock(return_value={}))
    core._startup_gate_enabled = False
    configuration = [{'subject_id': 'subject1', 'sensors': [
        {'local_name': 'Recovery Sensor', 'number_of': 1, 'locations': ['CHEST']},
    ]}]
    core.init_core(configuration)
    _submit(manager, 'discover_and_connect')
    yield NS(core=core, manager=manager, adapter=adapter, gateway=gateway, uart=uart,
             events=events, config=configuration, discovered=discovered)
    core.stop_manager()
    manager.loop.close()


def start(runtime, stamp='20261012_220000'):
    runtime.core.start_stream({'session_timestamp': stamp})
    _submit(runtime.manager, '__initialize_adapters__')  # Queue barrier after START.
    runtime.core._activate_official_streaming()
    runtime.core.stream_phase = 'official_streaming'
    runtime.core._official_outputs_active = True
    assert runtime.uart.measuring == {'AA'}


def finish(runtime):
    try:
        runtime.core.stop_stream()
    except RuntimeError:
        assert not runtime.uart.available or runtime.uart.fail_stop
    deadline = time.monotonic() + 5
    while not any(event['type'] == 'stream_drained' for event in runtime.events):
        assert time.monotonic() < deadline
        time.sleep(.01)
    archive = next(event['payload'] for event in runtime.events
                   if isinstance(event['payload'], dict) and event['payload'].get('session_archive_exists'))
    assert Path(archive['session_archive_path']).is_file()


def reset(runtime):
    handler = MessageHandler.__new__(MessageHandler)
    handler.si = runtime.core
    handler._system_event_bus = runtime.core.system_event_bus
    handler._handle_local('disconnect_all', {'recover_session': True, 'correlation_id': 'reset'})
    deadline = time.monotonic() + 5
    while not any(event['type'] == 'sensor_cleanup_complete' and event['payload'].get('recover_session')
                  for event in runtime.events):
        assert time.monotonic() < deadline
        time.sleep(.01)
    return [event['payload'] for event in runtime.events if event['type'] == 'sensor_cleanup_complete'][-1]


@pytest.mark.parametrize('fault,links_survive', [(False, True), (True, True), (True, False)])
def test_finish_reset_new_acquisition_same_core(runtime, fault, links_survive):
    start(runtime)
    original = runtime.manager.sensors[0]
    context = original.stop_context
    runtime.uart.available = not fault
    finish(runtime)
    archived = runtime.core.archived_session_timestamp
    if fault:
        assert original.measurement_cleanup_pending
        assert 'AA' in runtime.uart.measuring
    runtime.uart.available = True
    if not links_survive:
        runtime.uart.links.clear()
        runtime.uart.subscriptions.clear()
    report = reset(runtime)
    assert report['core_ready_for_configuration'] and report['recovery_complete'], report
    assert not runtime.uart.measuring and not runtime.manager.recovery_obligations
    assert original.stop_context is context
    assert runtime.core.archived_session_timestamp == archived
    assert len([event for event in runtime.events if event['type'] == 'stream_drained']) == 1
    runtime.core.init_core(runtime.config)
    _submit(runtime.manager, 'discover_and_connect')
    start(runtime, '20261012_220100')
    assert original.host_closed
    assert not runtime.manager.sensors[0].physical_stop_quarantined
    assert not any(msg['type'] == 'reset_session' for msg in runtime.uart.commands)
    assert len(runtime.discovered) == (3 if not links_survive else 2)


def test_unconfirmed_stop_retained_across_configuration_and_repeat_reset(runtime):
    start(runtime)
    runtime.uart.fail_stop = True
    finish(runtime)
    original = runtime.manager.sensors[0]
    original.transport_client.cleanup_deadline_unix_s = time.time() - 100
    report = reset(runtime)
    assert report['core_ready_for_configuration'] and not report['recovery_complete']
    assert report['results'][0]['measurement_stop_confirmed'] is False
    assert runtime.uart.links == {'AA'} and runtime.uart.measuring == {'AA'}
    runtime.core.init_core(runtime.config)
    assert runtime.manager.recovery_obligations['AA'] is original and not original.host_closed
    with pytest.raises(RuntimeError, match='unresolved physical STOP'):
        _submit(runtime.manager, 'discover_and_connect')
    runtime.uart.fail_stop = False
    runtime.events.clear()
    report = reset(runtime)
    assert report['recovery_complete']
    runtime.core.init_core(runtime.config)
    _submit(runtime.manager, 'discover_and_connect')
    start(runtime, '20261012_220100')


def test_unavailable_gateway_does_not_block_configuration(runtime, monkeypatch):
    start(runtime)
    runtime.uart.available = False
    finish(runtime)
    monkeypatch.setattr(runtime.gateway, 'recover_uart', Mock(side_effect=TimeoutError('UART absent')))
    report = reset(runtime)
    assert report['core_ready_for_configuration'] and not report['gateway_communication_available']
    assert not report['recovery_complete']
    runtime.core.init_core(runtime.config)
    assert runtime.manager.running and 'AA' in runtime.manager.recovery_obligations


def test_reset_rejects_active_subject_and_startup(runtime):
    start(runtime)
    with pytest.raises(RuntimeError, match='Stop all subjects'):
        runtime.core.reset_session()
    assert runtime.uart.measuring == {'AA'}
    runtime.core.subjects[0].is_streaming = False
    runtime.core.stream_phase = 'warming_up'
    with pytest.raises(RuntimeError, match='Stop all subjects'):
        runtime.core.reset_session()


def test_gateway_inventory_requires_complete_real_inventory(runtime):
    assert runtime.gateway.get_connection_inventory(.1)['connected_addresses'] == ['AA']
    runtime.uart.write = lambda data: runtime.gateway._route_json({
        'type': 'status', 'request_id': json.loads(data)['request_id'], 'gateway': {'state': 'ready'},
    }) or len(data)
    with pytest.raises(RuntimeError, match='inventory unavailable'):
        runtime.gateway.get_connection_inventory(.1)


def test_uart_reopen_keeps_lines_links_and_callbacks(runtime):
    transport = runtime.manager.sensors[0].transport_client
    callback = transport.notify_callbacks['measurement'] = Mock()
    runtime.gateway.started = runtime.gateway.running = False
    result = runtime.gateway.recover_uart(.1)
    assert result['connected_addresses'] == ['AA']
    assert transport.notify_callbacks['measurement'] is callback
    assert runtime.uart.links == {'AA'} and runtime.uart.open.call_count == 1
    assert runtime.uart.dtr and runtime.uart.rts
    assert [msg['type'] for msg in runtime.uart.commands][-1] == 'get_status'


def test_repeated_core_reset_coalesces_without_repeating_archive(runtime, monkeypatch):
    start(runtime)
    finish(runtime)
    original = runtime.manager.controller.cleanup_service.reconcile
    release = asyncio.Event()

    async def wait(*args, **kwargs):
        await release.wait()
        return await original(*args, **kwargs)

    reconcile = AsyncMock(side_effect=wait)
    monkeypatch.setattr(runtime.manager.controller.cleanup_service, 'reconcile', reconcile)
    first = runtime.core.reset_session({'correlation_id': 'first'})
    second = runtime.core.reset_session({'correlation_id': 'second'})
    assert first is second
    with pytest.raises(RuntimeError, match='recovery is still in progress'):
        runtime.core.init_core(runtime.config)
    with pytest.raises(RuntimeError, match='recovery is still in progress'):
        runtime.core.start_stream({})
    runtime.manager.loop.call_soon_threadsafe(release.set)
    assert first.result(3)['core_ready_for_configuration']
    assert reconcile.call_count == 1
    assert runtime.core.compute_orch.reset.call_count == 2  # Init and one Reset.
    reports = [e['payload'] for e in runtime.events if e['type'] == 'sensor_cleanup_complete']
    assert {report['correlation_id'] for report in reports} == {'first', 'second'}
    assert len([e for e in runtime.events if e['type'] == 'stream_drained']) == 1


def test_scoped_recovery_preserves_other_subject_connection_and_callbacks(runtime):
    sensor = runtime.manager.sensors[0]
    sensor.stop_context = object()
    sensor.measurement_cleanup_pending = True
    other = runtime.adapter.create_transport_client('BB')
    other.is_connected = True
    other.sensor_id = 2
    received = []
    other.notify_callbacks['measurement'] = lambda uuid, data, *timing: received.append(data)
    runtime.uart.links.update({'AA', 'BB'})
    runtime.uart.measuring.update({'AA', 'BB'})
    runtime.gateway.set_acquisition_scope({'BB'})
    report = runtime.manager.recover_sensors(['AA']).result(3)
    assert report['results'][0]['measurement_stop_confirmed']
    assert report['core_ready_for_configuration'] is False  # BB is still acquiring.
    assert runtime.uart.links == {'BB'} and runtime.uart.measuring == {'BB'}
    runtime.gateway._route_json({'type': 'notification', 'address': 'BB',
                                'characteristic_uuid': 'measurement', 'payload_hex': '01'})
    assert received == [b'\x01'] and other.is_connected
    assert runtime.gateway.active_acquisition_addresses == {'BB'}
    with pytest.raises(RuntimeError, match='prohibited'):
        runtime.gateway.reset_session()


def test_expired_recovery_deadline_cannot_submit_stop(runtime):
    sensor = runtime.manager.sensors[0]
    sensor.measurement_cleanup_pending = True
    runtime.uart.commands.clear()
    report = asyncio.run(runtime.manager.controller.cleanup_service.reconcile(
        [sensor], deadline=time.monotonic() - 1,
    ))
    assert report['results'][0]['measurement_stop_confirmed'] is False
    assert not runtime.uart.commands


def test_uncertain_reset_late_ack_is_reconciled_without_new_reset(runtime, monkeypatch):
    send = runtime.gateway.send
    monkeypatch.setattr(runtime.gateway, 'send', Mock())
    with pytest.raises(TimeoutError):
        runtime.gateway.reset_session(.001)
    request = runtime.gateway.send.call_args.args[0]
    assert runtime.gateway._reset_unconfirmed
    monkeypatch.setattr(runtime.gateway, 'send', send)
    assert runtime.gateway.recover_uart(.1)['reset_confirmed'] is False
    runtime.gateway._route_json({'type': 'reset_session_complete', 'request_id': request['request_id']})
    assert runtime.gateway.recover_uart(.1)['reset_confirmed'] is True
    runtime.gateway.set_acquisition_scope({'AA'})


def test_core_config_failure_preserves_old_graph_and_manager(runtime, monkeypatch):
    previous = runtime.core.subjects
    sensors = runtime.manager.sensors
    monkeypatch.setattr(runtime.manager, 'init_sensor_manager', Mock(side_effect=RuntimeError('Initialization failed')))
    with pytest.raises(RuntimeError, match='Initialization failed'):
        runtime.core.init_core(runtime.config)
    assert runtime.core.subjects is previous
    assert runtime.core.subject_graph.get_subjects() is previous
    assert runtime.manager.sensors is sensors


@pytest.mark.parametrize('other_active', [False, True])
def test_controlled_reset_resolves_uncertainty_only_without_other_acquisition(runtime, other_active):
    sensor = runtime.manager.sensors[0]
    sensor.stop_context = object()
    sensor.measurement_cleanup_pending = True
    runtime.uart.measuring.add('AA')
    runtime.gateway._reset_unconfirmed = True
    runtime.gateway._pending_reset_ids.add('old-reset')
    if other_active:
        runtime.uart.links.add('BB')
        runtime.uart.measuring.add('BB')
        runtime.gateway.set_acquisition_scope(set())
        # The acquisition predates the uncertain reset; model retained scope.
        runtime.gateway.active_acquisition_addresses = {'BB'}
    write = runtime.uart.write
    def respond(data):
        msg = json.loads(data)
        if msg['type'] == 'reset_session':
            runtime.uart.commands.append(msg)
            runtime.uart.links.clear()  # Resetting BLE is not measurement STOP.
            runtime.uart.subscriptions.clear()
            runtime.gateway._route_json({'type': 'reset_session_complete', 'request_id': msg['request_id']})
            return len(data)
        return write(data)
    runtime.uart.write = respond
    report = runtime.manager.recover_sensors(['AA'], allow_ble_reset=True).result(3)
    resets = [msg for msg in runtime.uart.commands if msg['type'] == 'reset_session']
    if other_active:
        assert not resets and runtime.uart.measuring == {'AA', 'BB'}
        assert report['results'][0]['measurement_stop_confirmed'] is False
    else:
        assert len(resets) == 1 and not runtime.uart.measuring
        assert report['results'][0]['measurement_stop_confirmed']
        assert not runtime.gateway._reset_unconfirmed


def test_old_uart_reader_cannot_route_into_new_transport_generation(runtime):
    entered, release = threading.Event(), threading.Event()
    notification = Mock()
    runtime.gateway.register_event_handler('notification', notification)
    def read(_):
        entered.set()
        assert release.wait(2)
        return b'{"type":"notification","address":"AA"}\n'
    runtime.uart.read = read
    worker = threading.Thread(target=gateway_module.GatewaySerialClient._read_loop, args=(runtime.gateway,))
    worker.start()
    try:
        assert entered.wait(1)
        runtime.gateway._transport_generation += 1
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive()
    notification.assert_not_called()


def test_reinitialization_deadline_prevents_late_configuration_commit(runtime, monkeypatch):
    old = runtime.manager.sensors
    old_meta = runtime.manager.sensor_meta
    replacement = Sensor(None)
    release = asyncio.Event()
    async def initialize():
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()  # A slow provider must not commit stale state.
    monkeypatch.setattr(runtime.manager.adapter_pool, 'initialize_all', initialize)
    completion = Future()
    runtime.manager.loop.call_soon_threadsafe(runtime.manager.queue.put_nowait, {
        'message': '__reinitialize_sensors__', 'sensors': [replacement],
        'deadline': time.monotonic() + .03, '_completion': completion,
    })
    with pytest.raises(TimeoutError):
        completion.result(2)
    assert runtime.manager.sensors is old and runtime.manager.sensor_meta is old_meta
    runtime.manager.loop.call_soon_threadsafe(release.set)
    _submit(runtime.manager, '__initialize_adapters__')
    assert runtime.manager.sensors is old and runtime.manager.sensor_meta is old_meta
    assert runtime.core.subjects[0].sensors[0]['sensor'] is old[0]


def test_gateway_waiter_obeys_operation_deadline_not_long_command_timeout(runtime):
    runtime.uart.write = lambda data: len(data)
    operation = SensorOperation(time.monotonic() + .02)
    began = time.monotonic()
    with operation_scope(operation), pytest.raises(TimeoutError):
        runtime.gateway.get_connection_inventory(10)
    assert time.monotonic() - began < .5
    assert not runtime.gateway.pending_requests


@pytest.mark.parametrize('permission', [False, 'false'])
def test_uart_available_does_not_imply_reset_or_acquisition_ready(runtime, permission):
    sensor = runtime.manager.sensors[0]
    sensor.measurement_cleanup_pending = True
    runtime.gateway._reset_unconfirmed = True
    report = runtime.manager.recover_sensors(allow_ble_reset=permission).result(3)
    assert report['core_ready_for_configuration'] and report['gateway_communication_available']
    assert not report['gateways'][0]['reset_confirmed']
    assert not report['results'][0]['eligible_for_acquisition']
    assert not any(msg['type'] == 'reset_session' for msg in runtime.uart.commands)


def test_surviving_inventory_link_repairs_stale_sensor_status_before_stop(runtime):
    sensor = runtime.manager.sensors[0]
    sensor.stop_context = object()
    sensor.measurement_cleanup_pending = True
    sensor.connection_status = ConnectionStatus.DISCONNECTED
    sensor.transport_client.is_connected = False
    runtime.uart.measuring.add('AA')
    runtime.uart.commands.clear()
    report = runtime.manager.recover_sensors().result(3)
    assert report['results'][0]['measurement_stop_confirmed']
    assert not runtime.uart.measuring
    assert [msg['type'] for msg in runtime.uart.commands] == ['get_status', 'gatt_write', 'disconnect_addresses']


def test_operator_confirmation_recovers_lost_context_without_core_restart(runtime):
    sensor = runtime.manager.sensors[0]
    sensor.measurement_cleanup_pending = sensor.physical_stop_quarantined = True
    runtime.manager.recovery_obligations['AA'] = sensor
    runtime.uart.measuring.clear()  # Operator physically stopped/power cycled it.
    runtime.uart.links.clear()
    runtime.uart.subscriptions.clear()
    write = runtime.uart.write
    def respond(data):
        msg = json.loads(data)
        if msg['type'] == 'reset_session':
            runtime.uart.commands.append(msg)
            runtime.gateway._route_json({'type': 'reset_session_complete', 'request_id': msg['request_id']})
            return len(data)
        return write(data)
    runtime.uart.write = respond
    report = runtime.core.reset_session({
        'allow_ble_reset': True, 'confirmed_stopped_sensor_addresses': ['AA'],
    }).result(3)
    assert report['results'][0]['confirmation_source'] == 'operator'
    assert report['recovery_complete'] and not runtime.manager.recovery_obligations
    assert sensor.host_closed
    runtime.core.init_core(runtime.config)
    _submit(runtime.manager, 'discover_and_connect')
    start(runtime)


def test_operator_claim_without_controlled_reset_does_not_release_sensor(runtime):
    sensor = runtime.manager.sensors[0]
    sensor.measurement_cleanup_pending = True
    runtime.manager.recovery_obligations['AA'] = sensor
    report = runtime.manager.recover_sensors(confirmed_stopped_addresses=['AA']).result(3)
    assert not report['results'][0]['eligible_for_acquisition']
    assert sensor.measurement_cleanup_pending and 'AA' in runtime.manager.recovery_obligations
