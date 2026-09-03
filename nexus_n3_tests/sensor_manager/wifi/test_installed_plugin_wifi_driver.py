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

    asyncio.run(scenario())
