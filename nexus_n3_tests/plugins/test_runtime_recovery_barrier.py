"""A failed operation is retired without replacing sensor-specific host state."""
import asyncio
import threading
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from nexus_n3.plugins.common.operations import SensorOperation, operation_scope
from nexus_n3.plugins.runtime.sensor_host import SensorHost, HostAdapterProxy
from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy
from nexus_n3.sensor_manager.cleanup_service import CleanupService


def test_host_barrier_waits_for_old_lifecycle_and_fences_expired_command():
    host = SensorHost.__new__(SensorHost)
    host._lifecycle_lock = threading.Lock()
    host._sensor = NS(subscription='retained-subscription')
    entered, release = threading.Event(), threading.Event()
    commands, errors = [], []
    operation = SensorOperation(time.monotonic() + 1)

    def stop(_):
        entered.set()
        assert release.wait(2)
        operation.check()
        commands.append('old STOP')

    def old_request():
        try:
            with operation_scope(operation):
                host.lifecycle_call(stop, {})
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=old_request)
    worker.start()
    try:
        assert entered.wait(1)
        assert host.runtime_barrier({}) == {'idle': False, 'measurement_stop_confirmed': False}
        with pytest.raises(RuntimeError, match='still executing'):
            host.lifecycle_call(lambda _: commands.append('new STOP'), {})
        operation.cancelled.set()
    finally:
        release.set()
        worker.join(2)
    assert len(errors) == 1 and isinstance(errors[0], TimeoutError) and not commands
    assert host.runtime_barrier({}) == {'idle': True, 'measurement_stop_confirmed': False}
    host.lifecycle_call(lambda _: commands.append('new STOP'), {})
    assert commands == ['new STOP'] and host._sensor.subscription == 'retained-subscription'


def test_proxy_recovers_only_after_worker_and_host_are_idle():
    async def scenario():
        proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
        proxy.measurement_cleanup_pending = proxy.physical_stop_quarantined = True
        request = Mock(return_value={'idle': False, 'measurement_stop_confirmed': False})
        context = proxy._plugin_client = NS(transport=NS(request=request), subscription='original')
        release = asyncio.Event()
        proxy._stop_worker = asyncio.create_task(release.wait())
        with pytest.raises(RuntimeError, match='worker is still executing'):
            await proxy.prepare_recovery()
        request.assert_not_called()
        release.set()
        await proxy._stop_worker
        with pytest.raises(RuntimeError, match='host operation is still executing'):
            await proxy.prepare_recovery()
        assert proxy.physical_stop_quarantined
        request.return_value = {'idle': True, 'measurement_stop_confirmed': False}
        await proxy.prepare_recovery()
        assert not proxy.physical_stop_quarantined and proxy._plugin_client is context
        assert context.subscription == 'original' and proxy.measurement_cleanup_pending
        proxy._plugin_state_lost = True
        with pytest.raises(RuntimeError, match='state lost'):
            await proxy.prepare_recovery()
    asyncio.run(scenario())


def test_cancelled_recovery_cannot_clear_stop_obligation_or_issue_late_command():
    async def scenario():
        release = asyncio.Event()
        commands = []
        obligation = NS(pending=True)
        operation = SensorOperation(time.monotonic() + .02)

        async def old_stop():
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()  # Deliberately emulate a stubborn worker.
            operation.check()
            commands.append('STOP')
            obligation.pending = False

        with pytest.raises(TimeoutError):
            await CleanupService._bounded(old_stop(), operation)
        release.set()
        await asyncio.sleep(.01)
        assert operation.cancelled.is_set() and obligation.pending and not commands
    asyncio.run(scenario())


def test_ignored_best_effort_false_cannot_confirm_physical_stop():
    host = SensorHost.__new__(SensorHost)
    connection = Mock()
    connection.request.return_value = {'ok': False}
    host._adapter = HostAdapterProxy(connection)
    async def stop(adapter):
        # Existing plugin contract permits a None return; a missed write still
        # prevents the infrastructure from claiming a confirmed physical STOP.
        await adapter.write('AA', 'plugin-owned-stop', b'stop', best_effort=True)
    host._sensor = NS(stop_stream=stop)
    with operation_scope(SensorOperation(time.monotonic() + 1)):
        assert host.stop_stream({}) == {'ok': False, 'result': None}


def test_recovery_noop_cannot_confirm_stop_but_old_verified_stop_can():
    host = SensorHost.__new__(SensorHost)
    host._lifecycle_lock = threading.Lock()
    connection = Mock()
    connection.request.return_value = {'ok': True}
    host._adapter = HostAdapterProxy(connection)
    async def noop(_adapter):
        return None
    host._sensor = NS(stop_stream=noop)
    with operation_scope(SensorOperation(time.monotonic() + 1)):
        assert host.stop_stream({'require_transport_confirmation': True})['ok'] is False
    assert not host.runtime_barrier({})['measurement_stop_confirmed']
    async def acknowledged_stop(adapter):
        await adapter.write('AA', 'plugin-owned-stop', b'stop')
    host._sensor.stop_stream = acknowledged_stop
    with operation_scope(SensorOperation(time.monotonic() + 1)):
        assert host.stop_stream({'require_transport_confirmation': True})['ok']
    assert host.runtime_barrier({})['measurement_stop_confirmed']
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    proxy.measurement_cleanup_pending = proxy.physical_stop_quarantined = True
    proxy._plugin_client = NS(transport=NS(request=lambda *_: host.runtime_barrier({})))
    asyncio.run(proxy.prepare_recovery())
    assert not proxy.measurement_cleanup_pending  # Recover verified late completion.


def test_plugin_cannot_hide_transport_exception_and_claim_stop_success():
    host = SensorHost.__new__(SensorHost)
    connection = Mock()
    connection.request.side_effect = RuntimeError('Transport unavailable')
    host._adapter = HostAdapterProxy(connection)
    async def stop(adapter):
        try:
            await adapter.write('AA', 'plugin-stop', b'stop')
        except RuntimeError:
            return True
    host._sensor = NS(stop_stream=stop)
    with operation_scope(SensorOperation(time.monotonic() + 1)):
        assert host.stop_stream({'require_transport_confirmation': True})['ok'] is False


def test_expired_start_worker_cannot_issue_commands_or_be_hidden_by_cancellation():
    async def scenario():
        proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
        proxy.physical_stop_quarantined = False
        entered, release = threading.Event(), threading.Event()
        commands = []
        from nexus_n3.plugins.common.operations import current_operation
        def start():
            operation = current_operation.get()
            entered.set()
            assert release.wait(2)
            operation.check()
            commands.append('late START')
        client = NS(start_stream=start, transport=NS(request=Mock(return_value={'idle': True})))
        proxy._ensure_client = lambda: client
        proxy._plugin_client = client
        proxy.measurement_cleanup_pending = True
        operation = SensorOperation(time.monotonic() + .02)
        with operation_scope(operation), pytest.raises(TimeoutError):
            await proxy._call_lifecycle('start_stream')
        assert entered.is_set() and proxy.physical_stop_quarantined
        with pytest.raises(RuntimeError, match='lifecycle worker is still executing'):
            await proxy.prepare_recovery()
        operation.cancelled.set()
        release.set()
        try:
            await proxy._lifecycle_worker
        except TimeoutError:
            pass
        await proxy.prepare_recovery()
        assert not commands and proxy.measurement_cleanup_pending
    asyncio.run(scenario())
