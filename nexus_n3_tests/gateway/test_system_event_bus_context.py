from nexus_n3.gateway.event_bus.system_event_bus import SystemEventBus
import time


def _wait_for_event(received: list[dict]) -> dict:
    deadline = time.time() + 1
    while time.time() < deadline:
        if received:
            return received.pop()
        time.sleep(0.01)
    raise AssertionError("expected an event to be delivered")


def test_system_event_bus_enriches_events_with_customer_and_site_context():
    received = []
    event_bus = SystemEventBus(
        deployment_context={
            "customer_id": "customer-dlr",
            "site_id": "site-lunar-facility",
            "site_name": "Lunar Facility",
        }
    )
    event_bus.subscribe(received.append)

    event_bus.emit(
        {
            "type": "evt_example",
            "payload": {"value": 1},
        }
    )

    event = _wait_for_event(received)
    assert event["customer_id"] == "customer-dlr"
    assert event["site_id"] == "site-lunar-facility"
    assert event["site"] == "Lunar Facility"
    assert event["payload"]["customer_id"] == "customer-dlr"
    assert event["payload"]["site_id"] == "site-lunar-facility"
    assert event["payload"]["site"] == "Lunar Facility"


def test_system_event_bus_preserves_explicit_event_context():
    received = []
    event_bus = SystemEventBus(
        deployment_context={
            "customer_id": "customer-dlr",
            "site_id": "site-lunar-facility",
            "site_name": "Lunar Facility",
        }
    )
    event_bus.subscribe(received.append)

    event_bus.emit(
        {
            "type": "evt_example",
            "customer_id": "customer-explicit",
            "site_id": "site-explicit",
            "site": "Explicit Site",
            "payload": {
                "customer_id": "customer-explicit",
                "site_id": "site-explicit",
                "site": "Explicit Site",
            },
        }
    )

    event = _wait_for_event(received)
    assert event["customer_id"] == "customer-explicit"
    assert event["site_id"] == "site-explicit"
    assert event["site"] == "Explicit Site"
    assert event["payload"]["customer_id"] == "customer-explicit"
    assert event["payload"]["site_id"] == "site-explicit"
    assert event["payload"]["site"] == "Explicit Site"
