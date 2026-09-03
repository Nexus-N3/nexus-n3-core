from __future__ import annotations

import asyncio

import pytest

from nexus_n3.sensor_manager.adapters.wifi.backends.linux_networkmanager import (
    LinuxNetworkManagerBackend,
    _CommandResult,
)
from nexus_n3.sensor_manager.adapters.wifi.config import WifiRuntimeConfig
from nexus_n3.sensor_manager.adapters.wifi.errors import WifiBackendUnavailable
from nexus_n3.sensor_manager.adapters.wifi.models import IPv4Configuration


class FakeNmcli:
    def __init__(self, *, active_profile: str = "nexus-n3-sensor-ap") -> None:
        self.active_profile = active_profile
        self.commands: list[tuple[str, ...]] = []

    async def __call__(self, command):
        command = tuple(command)
        self.commands.append(command)

        if command[1:4] == ("connection", "up", "nexus-n3-sensor-ap"):
            self.active_profile = "nexus-n3-sensor-ap"
            return _CommandResult(0, "Connection successfully activated\n", "")

        field = command[2] if len(command) > 2 and command[1] == "-g" else ""
        values = {
            "GENERAL.STATE": "100 (connected)\n",
            "GENERAL.CONNECTION": f"{self.active_profile}\n",
            "802-11-wireless.mode": "ap\n",
            "802-11-wireless.ssid": "nexus-n3-sensors\n",
            "IP4.ADDRESS": "10.42.0.1/24\n",
            "IP4.GATEWAY": "\n",
        }
        if field in values:
            return _CommandResult(0, values[field], "")
        return _CommandResult(1, "", f"unexpected command: {command}")


def _config(**overrides) -> WifiRuntimeConfig:
    values = {
        "enabled": True,
        "backend": "linux-networkmanager",
        "interface_name": "wlx00c0cabaa751",
        "ap_profile": "nexus-n3-sensor-ap",
        "expected_ap_cidr": "10.42.0.1/24",
    }
    values.update(overrides)
    return WifiRuntimeConfig(**values)


def test_reads_active_sensor_ap_without_reconfiguring_it():
    async def scenario():
        nmcli = FakeNmcli()
        backend = LinuxNetworkManagerBackend(_config(), command_runner=nmcli)

        await backend.initialize()
        network = await backend.ensure_ap_active()

        assert network == IPv4Configuration("10.42.0.1", 24)
        assert not any(command[1:3] == ("connection", "up") for command in nmcli.commands)
        assert backend.capabilities.ap_hosting is True
        await backend.shutdown()

    asyncio.run(scenario())


def test_reactivates_saved_ap_profile_when_interface_uses_another_connection():
    async def scenario():
        nmcli = FakeNmcli(active_profile="--")
        backend = LinuxNetworkManagerBackend(_config(), command_runner=nmcli)

        await backend.initialize()
        network = await backend.ensure_ap_active()

        assert network.cidr == "10.42.0.1/24"
        assert (
            "nmcli",
            "connection",
            "up",
            "nexus-n3-sensor-ap",
            "ifname",
            "wlx00c0cabaa751",
        ) in nmcli.commands

    asyncio.run(scenario())


def test_rejects_unexpected_ap_address():
    async def scenario():
        nmcli = FakeNmcli()

        async def wrong_address(command):
            result = await nmcli(command)
            if tuple(command)[2] == "IP4.ADDRESS":
                return _CommandResult(0, "192.168.60.1/24\n", "")
            return result

        backend = LinuxNetworkManagerBackend(_config(), command_runner=wrong_address)
        await backend.initialize()

        with pytest.raises(WifiBackendUnavailable, match="expected 10.42.0.1/24"):
            await backend.ensure_ap_active()

    asyncio.run(scenario())


def test_rejects_non_ap_networkmanager_profile():
    async def scenario():
        nmcli = FakeNmcli()

        async def client_profile(command):
            result = await nmcli(command)
            if tuple(command)[2] == "802-11-wireless.mode":
                return _CommandResult(0, "infrastructure\n", "")
            return result

        backend = LinuxNetworkManagerBackend(_config(), command_runner=client_profile)
        await backend.initialize()

        with pytest.raises(WifiBackendUnavailable, match="is not an AP"):
            await backend.ensure_ap_active()

    asyncio.run(scenario())


def test_rejects_unexpected_ap_ssid():
    async def scenario():
        nmcli = FakeNmcli()

        async def wrong_ssid(command):
            result = await nmcli(command)
            if tuple(command)[2] == "802-11-wireless.ssid":
                return _CommandResult(0, "another-network\n", "")
            return result

        backend = LinuxNetworkManagerBackend(_config(), command_runner=wrong_ssid)
        await backend.initialize()

        with pytest.raises(WifiBackendUnavailable, match="expected 'nexus-n3-sensors'"):
            await backend.ensure_ap_active()

    asyncio.run(scenario())
