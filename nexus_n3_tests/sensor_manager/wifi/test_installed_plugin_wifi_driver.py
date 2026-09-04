from __future__ import annotations

import asyncio
from types import SimpleNamespace

from nexus_n3.plugins.runtime.sensor_runtime import _InstalledPluginWifiDriver
from nexus_n3.sensor_manager.adapters.wifi.models import (
    IPv4Configuration,
    WifiDevice,
)


class FakeSensorHostClient:
    def __init__(self) -> None:
        self.connected_device = None
        self.disconnected = False
        self.diagnostics_reset = False

    def wifi_discover_connected(self, network):
        assert network.cidr == "10.42.0.1/24"
        return [
            {
                "address": "6A33CA84",
                "endpoint": "10.42.0.48",
                "metadata": {
                    "udp_send_port": 8048,
                    "udp_receive_port": 9000,
                },
            }
        ]

    def wifi_connect_sensor(self, device):
        self.connected_device = device
        return True

    def wifi_disconnect_sensor(self):
        self.disconnected = True
        return True

    def wifi_identify_candidate(self, network):
        return {
            "address": "6A33CA84",
            "endpoint": network.address,
            "metadata": {},
        }

    def wifi_provision(self, network, target):
        return {
            "device": {
                "address": "6A33CA84",
                "endpoint": network.address,
                "metadata": {},
            },
            "remote_access_point_disappeared": True,
        }

    def get_diagnostics_snapshot(self):
        return {"transport": "fake_plugin", "complete_samples": 123}

    def reset_session_diagnostics(self):
        self.diagnostics_reset = True


def test_installed_plugin_wifi_driver_bridges_lifecycle():
    async def scenario():
        client = FakeSensorHostClient()
        proxy = SimpleNamespace(_ensure_client=lambda: client)
        driver = _InstalledPluginWifiDriver(proxy)

        devices = await driver.discover_connected(
            IPv4Configuration(address="10.42.0.1", prefix=24)
        )

        assert devices == [
            WifiDevice(
                address="6A33CA84",
                endpoint="10.42.0.48",
                metadata={
                    "udp_send_port": 8048,
                    "udp_receive_port": 9000,
                },
            )
        ]
        assert await driver.connect_sensor(None, devices[0], None) is True
        assert client.connected_device == devices[0]
        assert await driver.disconnect_sensor(None) is True
        assert client.disconnected is True

        provisioning_network = IPv4Configuration("192.168.1.2", 24)
        identified = await driver.identify_candidate(provisioning_network)
        assert identified["address"] == "6A33CA84"

        controls = SimpleNamespace(
            disappeared=False,
            remote_access_point_disappeared=lambda: setattr(
                controls, "disappeared", True
            ),
        )
        provisioned = await driver.provision(
            provisioning_network,
            SimpleNamespace(ssid="nexus-n3-sensors"),
            controls,
        )
        assert provisioned["address"] == "6A33CA84"
        assert controls.disappeared is True

        diagnostics = await driver.get_diagnostics_snapshot()
        assert diagnostics == {
            "transport": "fake_plugin",
            "complete_samples": 123,
        }
        await driver.reset_session_diagnostics()
        assert client.diagnostics_reset is True

    asyncio.run(scenario())
