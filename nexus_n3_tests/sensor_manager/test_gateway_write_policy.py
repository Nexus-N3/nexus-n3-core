"""Per-operation timeouts must not reset unrelated BLE connections."""
import asyncio
from unittest.mock import Mock

import pytest
import serial

from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.adapters.gateway_ble_client import GatewaySerialClient
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


@pytest.fixture
def client(monkeypatch):
    client = GatewaySerialClient(BLERuntimeConfig(backend="gateway"))
    monkeypatch.setattr(client, "_handle_transport_failure", Mock())
    monkeypatch.setattr(client, "_recover_transport", Mock())
    return client


def reply_success(client, message):
    client._route_json({"type": "write_complete", "request_id": message["request_id"]})


@pytest.mark.parametrize("best_effort", [False, True])
def test_successful_write_cleans_pending_state(client, monkeypatch, best_effort):
    send = Mock(side_effect=lambda msg: reply_success(client, msg))
    monkeypatch.setattr(client, "send", send)
    assert client.write_gatt("CORE", "uuid", "1366", .01, allow_timeout=best_effort)
    assert send.call_count == 1
    assert client.pending_requests == {}
    client._handle_transport_failure.assert_not_called()
    client._recover_transport.assert_not_called()


def test_allowed_timeout_cleans_state_and_next_write_succeeds(client, monkeypatch):
    sent = []
    monkeypatch.setattr(client, "send", sent.append)
    assert client.write_gatt("CORE", "uuid", "1366", .001, allow_timeout=True) is None
    assert len(sent) == 1
    assert client.pending_requests == {}
    client._handle_transport_failure.assert_not_called()
    client._recover_transport.assert_not_called()

    def complete_next(msg):
        # An old reply must not complete the new operation.
        reply_success(client, sent[0])
        assert client.pending_requests[msg["request_id"]].empty()
        reply_success(client, msg)
    monkeypatch.setattr(client, "send", complete_next)
    assert client.write_gatt("CORE", "uuid", "1367", .01, allow_timeout=True)
    assert client.pending_requests == {}


def test_strict_timeout_recovers_and_retries(client, monkeypatch):
    sent = []
    def send(msg):
        sent.append(msg)
        if len(sent) == 2:
            reply_success(client, msg)
    monkeypatch.setattr(client, "send", send)
    assert client.write_gatt("CORE", "uuid", "1366", .001)
    assert len(sent) == 2
    assert sent[0]["request_id"] != sent[1]["request_id"]
    client._handle_transport_failure.assert_not_called()
    client._recover_transport.assert_called_once()
    assert client.pending_requests == {}


def test_strict_retry_timeout_is_propagated(client, monkeypatch):
    send = Mock()
    monkeypatch.setattr(client, "send", send)
    with pytest.raises(TimeoutError):
        client.write_gatt("CORE", "uuid", "1366", .001)
    assert send.call_count == 2
    client._recover_transport.assert_called_once()
    assert client.pending_requests == {}


@pytest.mark.parametrize("error,expected", [
    (TimeoutError("Timed out waiting for gateway response"), True),
    (TimeoutError("Timed out waiting for scan_complete"), True),
    (TimeoutError("Timed out waiting for status snapshot"), True),
    (TimeoutError("Timed out waiting for gatt_read"), True),
    (TimeoutError("sensor operation timeout"), False),
    (OSError("serial I/O failed"), True),
    (serial.SerialException("serial failed"), True),
    (RuntimeError("gatt_write_failed"), False),
])
def test_retry_classifies_timeouts_before_oserror(error, expected):
    assert GatewaySerialClient._is_transport_retryable(error) is expected


def test_best_effort_keeps_actual_serial_failure_recovery(client, monkeypatch):
    def send(msg):
        if send_mock.call_count == 1:
            raise serial.SerialException("serial failed")
        reply_success(client, msg)
    send_mock = Mock(side_effect=send)
    monkeypatch.setattr(client, "send", send_mock)
    assert client.write_gatt("CORE", "uuid", "1366", .01, allow_timeout=True)
    client._recover_transport.assert_called_once()
    assert client.pending_requests == {}


@pytest.mark.parametrize("best_effort", [False, True])
def test_adapter_passes_policy_and_reports_timeout(client, monkeypatch, best_effort):
    adapter = GatewayBLEAdapter()
    adapter.gateway_client = client
    adapter.ble_runtime_config = BLERuntimeConfig(backend="gateway", gateway_write_timeout_s=.001)
    device = adapter.create_transport_client("CORE")
    device.is_connected = True
    other = adapter.create_transport_client("DOT")
    other.is_connected = True
    packets = []
    other.notify_callbacks["measurement"] = lambda uuid, data, timing: packets.append(data)
    disconnect = Mock()
    other.disconnected_callback = disconnect
    client.register_event_handler("notification", adapter._handle_notification)
    sent = Mock(side_effect=lambda msg: client._route_json({
        "type": "notification", "address": "DOT", "characteristic_uuid": "measurement",
        "payload_hex": "01",
    }))
    monkeypatch.setattr(client, "send", sent)
    async def scenario():
        return await adapter.write(device, "uuid", b"\x13\x66", best_effort=best_effort)
    if best_effort:
        assert asyncio.run(scenario()) is False
        assert sent.call_count == 1
        client._recover_transport.assert_not_called()
        client._handle_transport_failure.assert_not_called()
    else:
        with pytest.raises(TimeoutError):
            asyncio.run(scenario())
        assert sent.call_count == 2
        client._recover_transport.assert_called_once()
    assert packets
    assert other.is_connected
    disconnect.assert_not_called()
    assert client.pending_requests == {}
