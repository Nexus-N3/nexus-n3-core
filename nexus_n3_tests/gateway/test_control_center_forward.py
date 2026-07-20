from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.gateway.messaging.message_handler import MessageHandler


class FakeEventBus:
    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(event)


def test_control_center_forward_command_emits_local_control_center_event():
    event_bus = FakeEventBus()
    handler = MessageHandler("tallinn-lab", event_bus)
    forwarded_message = {
        "type": "subject_catalog_update",
        "target": "neia",
        "payload": {"groups": []},
    }

    handler.handle(
        {
            "type": mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE,
            "payload": {"message": forwarded_message},
        }
    )

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert event["type"] == mt.EVT_CONTROL_CENTER_MESSAGE
    assert event["payload"] == forwarded_message


def test_control_center_forward_command_emits_error_for_invalid_payload():
    event_bus = FakeEventBus()
    handler = MessageHandler("tallinn-lab", event_bus)

    handler.handle(
        {
            "type": mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE,
            "payload": {"message": "not-a-dict"},
        }
    )

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert event["type"] == mt.EVT_ERROR
    assert "invalid message payload" in event["payload"]


def test_control_center_forward_command_preserves_remote_operation_updates():
    event_bus = FakeEventBus()
    handler = MessageHandler("tallinn-lab", event_bus)
    forwarded_message = {
        "type": "remote_operation_update",
        "target": "neia",
        "payload": {"active": True, "operator_username": "developer"},
    }

    handler.handle(
        {
            "type": mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE,
            "payload": {"message": forwarded_message},
        }
    )

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert event["type"] == mt.EVT_CONTROL_CENTER_MESSAGE
    assert event["payload"]["type"] == "remote_operation_update"
    assert event["payload"]["payload"]["active"] is True
