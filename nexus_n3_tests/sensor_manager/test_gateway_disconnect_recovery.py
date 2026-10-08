"""A missing remote link must not trap Core in stale host connection state."""

from unittest.mock import Mock

import pytest

from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.adapters.gateway_ble_client import GatewayOperationPolicy
from nexus_n3.sensor_manager.discovery_service import _build_disconnect_callback
from nexus_n3.sensor_manager.sensor_handle import SensorBase
from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from types import SimpleNamespace


@pytest.fixture
def adapter(monkeypatch):
    adapter = GatewayBLEAdapter()
    client = adapter.gateway_client
    monkeypatch.setattr(client, "_handle_transport_failure", Mock())
    monkeypatch.setattr(client, "_recover_transport", Mock())
    return adapter


@pytest.mark.parametrize("policy", list(GatewayOperationPolicy))
def test_already_absent_disconnect_retires_stale_client(adapter, monkeypatch, policy):
    device = adapter.create_transport_client("aa:bb:cc:dd:ee:ff")
    device.is_connected = True
    device.sensor_id = 1
    device.operation_policy = policy
    device.notify_callbacks["measurement"] = Mock()
    other = adapter.create_transport_client("OTHER")
    other.is_connected = True
    client = adapter.gateway_client
    send = Mock(side_effect=lambda msg: client._route_json({
        "type": "error", "request_id": msg["request_id"],
        "message": "disconnect_addresses_failed", "error_code": -3,
    }))
    monkeypatch.setattr(client, "send", send)

    assert adapter._disconnect_sync(device) is True
    assert not device.is_connected
    assert device.sensor_id is None
    assert device.notify_callbacks == {}
    assert device.address.upper() not in adapter.transport_clients
    assert client.is_disconnected(device.address)
    assert other.is_connected
    assert send.call_count == 1
    assert not client.pending_requests
    client._recover_transport.assert_not_called()


def test_missing_sensor_error_for_batch_is_not_confirmation_for_every_address(adapter, monkeypatch):
    client = adapter.gateway_client
    monkeypatch.setattr(client, "send", lambda msg: client._route_json({
        "type": "error", "request_id": msg["request_id"], "error_code": -3,
    }))
    with pytest.raises(TimeoutError, match="Failed to disconnect"):
        client.disconnect(["AA", "BB"], .01)
    assert not client.disconnected_addresses
    assert not client.pending_requests


def test_no_reply_is_not_mistaken_for_a_disconnected_sensor(adapter, monkeypatch):
    device = adapter.create_transport_client("AA")
    device.is_connected = True
    device.operation_policy = GatewayOperationPolicy.SENSOR_CLEANUP
    monkeypatch.setattr(adapter.gateway_client, "_wait_for_message", Mock(side_effect=TimeoutError("Timed out waiting for gateway response")))
    monkeypatch.setattr(adapter.gateway_client, "send", Mock())
    with pytest.raises(TimeoutError):
        adapter._disconnect_sync(device)
    assert device.is_connected
    assert "AA" in adapter.transport_clients
    assert not adapter.gateway_client.disconnected_addresses
    assert not adapter.gateway_client.pending_requests
    adapter.gateway_client._recover_transport.assert_not_called()


def test_normal_timeout_recovery_can_retry_against_an_already_empty_gateway(adapter, monkeypatch):
    device = adapter.create_transport_client("AA")
    device.is_connected = True
    client = adapter.gateway_client
    send = Mock(side_effect=lambda msg: client._route_json({
        "type": "error", "request_id": msg["request_id"], "error_code": -3,
    }))
    monkeypatch.setattr(client, "send", send)
    wait = client._wait_for_message
    first = True

    def timeout_once(request_queue, deadline):
        nonlocal first
        if first:
            first = False
            raise TimeoutError("Timed out waiting for gateway response")
        return wait(request_queue, deadline)

    monkeypatch.setattr(client, "_wait_for_message", timeout_once)
    assert adapter._disconnect_sync(device)
    assert send.call_count == 2
    client._recover_transport.assert_called_once()
    assert not client.pending_requests


def test_acknowledged_session_reset_clears_host_links_and_preserves_measurement_cleanup(adapter, monkeypatch):
    client = adapter.gateway_client
    sensor = SensorBase(SimpleNamespace(local_name="DOT"), {
        "sensor": {"adapter": "BLE"}, "events": ["on_disconnected"],
    })
    sensor.address = "AA"
    sensor.set_connection_status(ConnectionStatus.CONNECTED)
    sensor.measurement_cleanup_pending = True
    device = adapter.create_transport_client("AA", disconnected_callback=_build_disconnect_callback(sensor))
    sensor.set_transport_client(device)
    device.is_connected = True
    device.sensor_id = 1
    device.binary_notify_uuid = "measurement"
    device.notify_callbacks["measurement"] = Mock()
    disconnected = Mock()
    sensor.register_listener("on_disconnected", disconnected)
    send = Mock(side_effect=lambda msg: client._route_json({
        "type": "reset_session_complete", "request_id": msg["request_id"], "ok": True,
    }))
    monkeypatch.setattr(client, "send", send)
    client.reset_session(.01)
    assert not device.is_connected
    assert device.sensor_id is None
    assert device.binary_notify_uuid is None
    assert not device.notify_callbacks
    assert sensor.connection_status.name == "DISCONNECTED"
    assert sensor.measurement_cleanup_pending  # Link reset cannot confirm measurement stop.
    disconnected.assert_called_once_with(["AA"])
    assert adapter._disconnect_sync(device)
    assert send.call_count == 1  # No futile disconnect for the old session.
    assert not client.pending_requests


def test_failed_session_reset_does_not_claim_links_are_disconnected(adapter, monkeypatch):
    device = adapter.create_transport_client("AA")
    device.is_connected = True
    device.disconnected_callback = Mock()
    monkeypatch.setattr(adapter.gateway_client, "send", Mock())
    with pytest.raises(TimeoutError):
        adapter.gateway_client.reset_session(.001)
    assert device.is_connected
    assert not adapter.gateway_client.disconnected_addresses
    device.disconnected_callback.assert_not_called()
    assert not adapter.gateway_client.pending_requests
