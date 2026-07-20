from nexus_n3.azure_bridge.bridge import AzureBridgeService
from nexus_n3.azure_bridge.config import AzureBridgeConfig
from nexus_n3.gateway.messaging import message_types as mt


class FakeAzureClient:
    def __init__(self):
        self.telemetry = []
        self.reported = []

    def send_telemetry(self, payload):
        self.telemetry.append(payload)

    def patch_reported_properties(self, payload):
        self.reported.append(payload)


def test_server_ready_updates_capabilities_reported_properties():
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()

    service._handle_local_event(
        {
            "type": mt.EVT_SERVER_READY,
            "payload": {
                "msg": "System Server Ready",
                "site": "test-site",
                "supported_sensors": [
                    {
                        "name": "Movella DOT",
                        "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
                        "computations": [
                            {"name": "standard_loading_intensity", "inputs": {"gravity": 9.80665}}
                        ],
                    }
                ],
                "supported_algorithms": ["standard_loading_intensity"],
                "supported_gateways": ["zeromq_gateway"],
                "supported_bridges": ["azure_bridge", "lavinmq_bridge"],
            },
        }
    )

    assert len(service.azure_client.telemetry) == 1
    assert len(service.azure_client.reported) == 1

    reported = service.azure_client.reported[0]
    assert reported["bridge"]["device_id"] == "d1"
    assert reported["capabilities"]["supported_algorithms"] == ["standard_loading_intensity"]
    assert reported["capabilities"]["supported_gateways"] == ["zeromq_gateway"]
    assert reported["capabilities"]["supported_bridges"] == ["azure_bridge", "lavinmq_bridge"]
    assert reported["capabilities"]["supported_sensors"][0]["name"] == "Movella DOT"
    assert (
        reported["capabilities"]["supported_sensors"][0]["computations"][0]["name"]
        == "standard_loading_intensity"
    )
