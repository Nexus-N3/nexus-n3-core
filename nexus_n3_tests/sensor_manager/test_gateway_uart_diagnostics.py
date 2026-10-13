"""UART A/B diagnostics use fake serial ports and never touch hardware."""
import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import serial

from nexus_n3.sensor_manager.adapters import gateway_ble_client as module
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


@pytest.fixture
def logs(caplog, monkeypatch):
    monkeypatch.delenv('NEXUS_N3_UART_TX_FLUSH', raising=False)
    module.logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        module.logger.removeHandler(caplog.handler)


def messages(logs):
    return [r.getMessage() for phase in ('setup', 'call')
            for r in logs.get_records(phase) if r.levelno == logging.INFO]


def client():
    return module.GatewaySerialClient(BLERuntimeConfig(backend='gateway'))


@pytest.mark.parametrize('mode', [None, '0', '1'])
def test_flush_mode_and_write_order(logs, monkeypatch, mode):
    if mode is not None:
        monkeypatch.setenv('NEXUS_N3_UART_TX_FLUSH', mode)
    c = client()
    c.started = c.running = True
    calls = []

    def write(payload):
        assert c.write_lock.locked()
        calls.append(('write', json.loads(payload)['value']))
        return len(payload)

    def flush():
        assert c.write_lock.locked()
        calls.append(('flush', None))

    c.ser = SimpleNamespace(write=write, flush=Mock(side_effect=flush))
    c.send({'type': 'test', 'value': 1})
    c.send({'type': 'test', 'value': 2})
    expected = [('write', 1), ('write', 2)]
    if mode == '1':
        expected = [('write', 1), ('flush', None), ('write', 2), ('flush', None)]
    assert calls == expected
    assert c.ser.flush.call_count == (2 if mode == '1' else 0)
    mode_logs = [m for m in messages(logs) if m.startswith('UART_TX_FLUSH ')]
    assert len(mode_logs) == 1
    assert f'mode={int(mode == "1")}' in mode_logs[0]
    assert f'diagnostic_tcdrain_can_block={mode == "1"}' in mode_logs[0]
    assert not c.write_lock.locked()
    # send() instrumentation must not emit per-command INFO messages.
    assert messages(logs) == mode_logs


@pytest.mark.parametrize('mode', ['0', '1'])
def test_incomplete_write_does_not_flush_or_retry(logs, monkeypatch, mode):
    monkeypatch.setenv('NEXUS_N3_UART_TX_FLUSH', mode)
    c = client()
    c.started = c.running = True
    c.ser = SimpleNamespace(write=Mock(return_value=1), flush=Mock())
    monkeypatch.setattr(c, '_handle_transport_failure', Mock())
    with pytest.raises(serial.SerialTimeoutException, match='Incomplete'):
        c.send({'type': 'test'})
    c.ser.write.assert_called_once()
    c.ser.flush.assert_not_called()
    c._handle_transport_failure.assert_called_once()
    assert not c.write_lock.locked()


@pytest.fixture
def fake_uart(logs, monkeypatch, request):
    monkeypatch.setenv('NEXUS_N3_UART_TX_FLUSH', getattr(request, 'param', '0'))
    c = client()
    commands = []

    def write(payload):
        command = json.loads(payload)
        commands.append(command['type'])
        response = {'reset_session': 'reset_session_complete', 'hello': 'hello_ack',
                    'get_status': 'status'}[command['type']]
        c._route_json({'type': response, 'request_id': command['request_id'], 'ok': True,
                       'gateway': {'state': 'ready', 'connected_addresses': ['AA:BB'], 'connected_count': 1}})
        return len(payload)

    port = SimpleNamespace(write=Mock(side_effect=write), flush=Mock(), close=Mock(), open=Mock(),
                           setDTR=Mock(), setRTS=Mock(), reset_input_buffer=Mock(), dtr=True, rts=True)
    factory = Mock(return_value=port)
    monkeypatch.setattr(module.serial, 'Serial', factory)
    monkeypatch.setattr(module, 'resolve_gateway_serial_port', lambda _: '/fake/uart')
    sleep = Mock()
    monkeypatch.setattr(module.time, 'sleep', sleep)
    monkeypatch.setattr(c, '_read_loop', lambda: None)
    yield c, port, factory, commands, sleep
    c.close()


@pytest.mark.parametrize('fake_uart', ['0', '1'], indirect=True)
def test_startup_probe_reopening_and_close_are_distinct(logs, fake_uart):
    c, port, factory, commands, sleep = fake_uart
    c.start()
    c.start()  # Idempotent: no second handshake/open.
    assert commands == ['reset_session', 'hello']
    assert factory.call_count == 1
    assert port.open.call_count == 0
    sleep.assert_called_once_with(.5)
    inventory = c.recover_uart()
    assert inventory['connected_addresses'] == ['AA:BB']
    assert commands == ['reset_session', 'hello', 'get_status']
    assert factory.call_count == 1 and port.close.call_count == 0
    assert not any(m.startswith('UART_REOPEN ') for m in messages(logs))
    assert any('existing UART responsive; no reopening' in m for m in messages(logs))

    c._handle_transport_failure('injected UART read failure', recover=False)
    c.start()  # Existing automatic recovery path; no BLE reset/hello.
    assert factory.call_count == 2 and port.open.call_count == 1
    assert commands == ['reset_session', 'hello', 'get_status', 'get_status']
    c.started = c.running = False
    c.recover_uart()  # Explicit physical reopen.
    assert factory.call_count == 3 and port.open.call_count == 2
    assert commands == ['reset_session', 'hello', 'get_status', 'get_status', 'get_status']
    sleep.assert_called_once_with(.5)
    port.reset_input_buffer.assert_called_once()
    assert port.flush.call_count == (len(commands) if c._uart_tx_flush else 0)
    c.close()
    text = messages(logs)
    assert len([m for m in text if m.startswith('UART_TX_FLUSH ')]) == 1
    assert any('UART_OPEN origin=initial result=succeeded' in m and 'generation=0' in m and 'recovery_requested=False' in m for m in text)
    assert any('UART_REOPEN origin=automatic result=succeeded' in m and 'generation=2' in m and 'recovery_requested=True' in m for m in text)
    assert any('UART_REOPEN origin=explicit result=succeeded' in m and 'generation=3' in m for m in text)
    assert any('UART_CLOSE ' in m and 'generation=0' in m and 'injected UART read failure' in m for m in text)
    assert any('UART_CLOSE ' in m and 'client close requested' in m for m in text)
    assert len([m for m in text if 'UART_BLE_RESET origin=destructive result=requested' in m]) == 1
    assert all('client=' in m and 'port=' in m for m in text if m.startswith('UART_') and not m.startswith('UART_TX_FLUSH'))


def test_automatic_retry_recovery_origin_and_no_ble_reset(logs, fake_uart):
    c, port, factory, commands, _ = fake_uart
    c.start()
    c.get_connection_inventory = Mock(side_effect=[TimeoutError('probe timed out'), {'connected_addresses': ['AA:BB']}])
    c._recover_transport('normal-policy retry after timeout')
    assert factory.call_count == 2 and port.open.call_count == 1
    assert commands == ['reset_session', 'hello']
    assert c.get_connection_inventory.call_count == 2
    assert any('UART_REOPEN origin=automatic result=succeeded' in m and 'normal-policy retry after timeout' in m for m in messages(logs))


@pytest.mark.parametrize('initial', [True, False])
def test_open_failures_are_logged_without_extra_attempts(logs, fake_uart, initial):
    c, port, factory, commands, _ = fake_uart
    if initial:
        factory.side_effect = serial.SerialException('open failed')
        action = c.start
    else:
        c._has_opened_uart = True
        port.open.side_effect = serial.SerialException('open failed')
        action = c.recover_uart
    with pytest.raises(serial.SerialException, match='open failed'):
        action()
    assert factory.call_count == 1
    assert commands == []
    event = 'OPEN origin=initial' if initial else 'REOPEN origin=explicit'
    assert any(f'UART_{event} result=failed' in m and 'open failed' in m for m in messages(logs))
    operation = 'STARTUP' if initial else 'RECOVERY'
    assert any(f'UART_{operation} ' in m and 'result=failed' in m for m in messages(logs))


def test_recovery_admission_rejection_is_logged_without_io(logs, fake_uart):
    c, port, factory, commands, _ = fake_uart
    c._reset_owner = 123
    with pytest.raises(RuntimeError, match='already in progress'):
        c.recover_uart()
    factory.assert_not_called()
    port.open.assert_not_called()
    assert commands == []
    assert any('UART_RECOVERY origin=explicit result=failed' in m for m in messages(logs))


def test_flush_error_preserves_failure_handling_and_unlocks(logs, monkeypatch):
    monkeypatch.setenv('NEXUS_N3_UART_TX_FLUSH', '1')
    c = client()
    c.started = c.running = True
    c.ser = SimpleNamespace(write=Mock(side_effect=lambda data: len(data)),
                            flush=Mock(side_effect=serial.SerialException('drain failed')))
    monkeypatch.setattr(c, '_handle_transport_failure', Mock())
    with pytest.raises(serial.SerialException, match='drain failed'):
        c.send({'type': 'test'})
    c.ser.write.assert_called_once()
    c.ser.flush.assert_called_once()
    c._handle_transport_failure.assert_called_once()
    assert not c.write_lock.locked()


def test_close_failure_is_observable_without_retry(logs, fake_uart):
    c, port, factory, commands, _ = fake_uart
    c.start()
    port.close.side_effect = OSError('close failed')
    c.close()  # Existing close behaviour swallows the physical close exception.
    port.close.assert_called_once()
    assert factory.call_count == 1 and commands == ['reset_session', 'hello']
    assert any('UART_CLOSE origin=transport result=failed' in m and 'close failed' in m for m in messages(logs))
