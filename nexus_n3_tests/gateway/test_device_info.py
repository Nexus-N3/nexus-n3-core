from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.gateway.messaging.message_handler import MessageHandler


class FakeEventBus:
    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(event)


class FakeSystemInterface:
    def get_supported_sensors(self):
        return [
            {
                "name": "Movella DOT",
                "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
                "computations": [
                    {
                        "name": "standard_loading_intensity",
                        "inputs": {"gravity": 9.80665},
                    }
                ],
            }
        ]

    def get_supported_algorithms(self):
        return ["standard_loading_intensity", "gait_asymmetry"]

    def get_supported_gateways(self):
        return ["zeromq_gateway"]

    def get_supported_bridges(self):
        return ["azure_bridge", "lavinmq_bridge"]

    def get_ble_runtime_config(self):
        return {
            "backend": "gateway",
            "backend_label": "nexus_ble_gateway",
            "gateway_serial_port": "/dev/serial/by-id/test-gateway",
            "gateway_baudrate": 1000000,
            "gateway_protocol_version": 1,
        }


def test_get_device_info_emits_control_center_friendly_snapshot():
    event_bus = FakeEventBus()
    handler = MessageHandler("tallinn-lab", event_bus)
    handler.si = FakeSystemInterface()
    handler.is_ready = True
    handler.set_device_info_provider(
        lambda: {
            "display_name": "Tallinn Habitat Lab Edge 01",
            "role": "standalone",
            "status": "online",
            "gateway_name": "zeromq_gateway",
            "active_bridge": "azure_bridge",
            "iot_hub_device_id": "nexus-edge-01",
            "software_version": "nexus-n3-core 0.0.7",
            "server_status": "running",
            "uptime_seconds": 3605,
            "uptime": "1h 0m",
            "remote_control_enabled": True,
            "usb_disk": {"present": True, "path": "/media/usb0"},
            "last_heartbeat_at": None,
            "neia_apps": [
                {
                    "id": "nexus_load",
                    "name": "Nexus Load",
                    "version": "0.2.0",
                    "developer": "Right Step",
                    "app_type": "app",
                    "installed": False,
                }
            ],
            "neia_workflows": [
                {
                    "id": "nexus",
                    "name": "Nexus Session Management",
                    "version": "0.4.0",
                    "developer": "Right Step",
                    "app_type": "workflow",
                    "installed": True,
                }
            ],
        }
    )

    handler.handle({"type": mt.CMD_GET_DEVICE_INFO, "payload": {"correlation_id": "abc"}})

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert event["type"] == mt.EVT_DEVICE_INFO

    payload = event["payload"]
    assert payload["site"] == "tallinn-lab"
    assert payload["correlation_id"] == "abc"
    assert payload["device"]["display_name"] == "Tallinn Habitat Lab Edge 01"
    assert payload["device"]["iot_hub_device_id"] == "nexus-edge-01"
    assert payload["device"]["active_bridge"] == "azure_bridge"
    assert payload["device"]["software_version"] == "nexus-n3-core 0.0.7"
    assert payload["admin_summary"]["server_status"] == "running"
    assert payload["admin_summary"]["remote_control_enabled"] is True
    assert payload["admin_summary"]["usb_storage_mounted"] is True
    assert payload["capabilities"]["supported_algorithms"] == [
        "standard_loading_intensity",
        "gait_asymmetry",
    ]
    assert payload["plugin_inventory"]["summary"] == {
        "sensors": 1,
        "algorithms": 2,
        "apps": 1,
        "workflows": 1,
    }
    assert payload["plugin_inventory"]["sensors"][0]["name"] == "Movella DOT"
    assert payload["plugin_inventory"]["algorithms"][0]["name"] == "standard_loading_intensity"
    assert payload["plugin_inventory"]["apps"][0]["id"] == "nexus_load"
    assert payload["plugin_inventory"]["apps"][0]["installed"] is False
    assert payload["plugin_inventory"]["workflows"][0]["id"] == "nexus"
    assert payload["plugin_inventory"]["workflows"][0]["installed"] is True


def test_server_ready_now_includes_supported_algorithms():
    event_bus = FakeEventBus()
    handler = MessageHandler("tallinn-lab", event_bus)
    archive_service = {
        "available": True,
        "scheme": "http",
        "port": 9000,
        "list_path": "/api/outputs",
        "download_path": "/api/outputs/download",
    }
    handler.set_archive_service(archive_service)
    handler.si = FakeSystemInterface()
    handler.is_ready = True

    handler.handle({"type": mt.CMD_IS_SERVER_READY, "payload": {"correlation_id": "abc"}})

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert event["type"] == mt.EVT_SERVER_READY
    assert event["payload"]["correlation_id"] == "abc"
    assert event["payload"]["archive_service"] == archive_service
    assert event["payload"]["supported_algorithms"] == [
        "standard_loading_intensity",
        "gait_asymmetry",
    ]
