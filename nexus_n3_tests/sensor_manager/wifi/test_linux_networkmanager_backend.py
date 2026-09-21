from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

pytest.importorskip("dbus_fast")

from nexus_n3.sensor_manager.adapters.wifi.backends.linux_networkmanager import (
    LinuxNetworkManagerBackend,
    WIFI_RECOVERY_UNIT,
)
from nexus_n3.sensor_manager.adapters.wifi.backends.networkmanager_dbus import (
    _is_access_point_secured,
)
from nexus_n3.sensor_manager.adapters.wifi.config import WifiRuntimeConfig
from nexus_n3.sensor_manager.adapters.wifi.errors import WifiBackendUnavailable
from nexus_n3.sensor_manager.adapters.wifi.models import IPv4Configuration


def _variant(value):
    return SimpleNamespace(value=value)


class FakeNetworkManagerClient:
    def __init__(self, *, active: bool = True) -> None:
        self.active = active
        self.connected = False
        self.closed = False
        self.operations: list[object] = []
        self.network = IPv4Configuration("10.42.0.1", 24)
        self.settings = {
            "connection": {"id": _variant("nexus-n3-sensor-ap")},
            "802-11-wireless": {
                "mode": _variant("ap"),
                "ssid": _variant(b"nexus-n3-sensors"),
            },
            "ipv4": {"method": _variant("shared")},
        }

    async def connect(self):
        self.connected = True
        self.operations.append("connect")

    def close(self):
        self.closed = True

    async def get_device_path(self, interface_name):
        self.operations.append(("get_device_path", interface_name))
        return "/device/1"

    async def find_saved_connection(self, connection_id):
        self.operations.append(("find_saved", connection_id))
        return "/settings/1"

    async def get_connection_settings(self, path):
        self.operations.append(("get_settings", path))
        return self.settings

    async def find_active_connection(self, connection_id):
        self.operations.append(("find_active", connection_id))
        return "/active/1" if self.active else None

    async def activate(self, connection_path, device_path):
        self.operations.append(("activate", connection_path, device_path))
        self.active = True
        return "/active/1"

    async def wait_for_connection_id(self, connection_id, device_path, timeout):
        self.operations.append(("wait_for_connection", connection_id))
        return "/active/1"

    async def get_ipv4(self, active_path):
        self.operations.append(("get_ipv4", active_path))
        return self.network

    async def wait_for_ipv4(self, active_path, timeout):
        return self.network

    async def wait_until_available(self, interface_name, connection_id, timeout):
        self.operations.append(("wait_until_available", interface_name, connection_id))
        return "/device/2", "/settings/2"


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


@pytest.mark.parametrize(
    ("flags", "wpa_flags", "rsn_flags", "expected"),
    [
        (0, 0, 0, False),
        (0x2, 0, 0, False),  # WPS alone does not make an AP secured.
        (0x1, 0, 0, True),
        (0, 0x100, 0, True),
        (0, 0, 0x100, True),
    ],
)
def test_access_point_security_ignores_non_privacy_ap_flags(
    flags, wpa_flags, rsn_flags, expected
):
    assert _is_access_point_secured(flags, wpa_flags, rsn_flags) is expected


def test_reads_active_sensor_ap_without_reconfiguring_it():
    async def scenario():
        client = FakeNetworkManagerClient()
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()

        network = await backend.ensure_ap_active()

        assert network == IPv4Configuration("10.42.0.1", 24)
        assert not any(
            isinstance(operation, tuple) and operation[0] == "activate"
            for operation in client.operations
        )
        assert backend.capabilities.ap_hosting is True
        await backend.shutdown()
        assert client.closed is True

    asyncio.run(scenario())


def test_reactivates_saved_ap_profile_when_inactive():
    async def scenario():
        client = FakeNetworkManagerClient(active=False)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()

        network = await backend.ensure_ap_active()

        assert network.cidr == "10.42.0.1/24"
        assert ("activate", "/settings/1", "/device/1") in client.operations

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("section", "key", "value", "message"),
    [
        ("802-11-wireless", "mode", "infrastructure", "is not an AP"),
        ("802-11-wireless", "ssid", b"another-network", "expected 'nexus-n3-sensors'"),
        ("ipv4", "method", "manual", "expected 'shared'"),
    ],
)
def test_rejects_invalid_saved_ap_settings(section, key, value, message):
    async def scenario():
        client = FakeNetworkManagerClient()
        client.settings[section][key] = _variant(value)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()

        with pytest.raises(WifiBackendUnavailable, match=message):
            await backend.ensure_ap_active()

    asyncio.run(scenario())


def test_rejects_unexpected_ap_address():
    async def scenario():
        client = FakeNetworkManagerClient()
        client.network = IPv4Configuration("192.168.60.1", 24)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()

        with pytest.raises(WifiBackendUnavailable, match="expected 10.42.0.1/24"):
            await backend.ensure_ap_active()

    asyncio.run(scenario())


def test_network_stack_recovery_uses_only_fixed_systemd_unit():
    async def scenario():
        client = FakeNetworkManagerClient()
        recovery_calls = []

        async def recover(unit_name, timeout):
            recovery_calls.append((unit_name, timeout))

        backend = LinuxNetworkManagerBackend(
            _config(network_stack_restart_timeout_s=17),
            client=client,
            recovery_runner=recover,
        )
        await backend.initialize()

        await backend._restart_network_stack()

        assert recovery_calls == [(WIFI_RECOVERY_UNIT, 17)]
        assert ("wait_until_available", "wlx00c0cabaa751", "nexus-n3-sensor-ap") in client.operations

    asyncio.run(scenario())


def test_fixed_recovery_command_is_exact_and_noninteractive(monkeypatch):
    async def scenario():
        calls = []

        class FakeProcess:
            returncode = 0

            async def communicate(self):
                return b"", b""

            def kill(self):
                raise AssertionError("successful recovery must not be killed")

            async def wait(self):
                return 0

        async def create_subprocess_exec(*args, **kwargs):
            calls.append((args, kwargs))
            return FakeProcess()

        monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)

        await LinuxNetworkManagerBackend._run_fixed_recovery_unit(
            WIFI_RECOVERY_UNIT,
            1,
        )

        assert calls[0][0] == (
            "sudo",
            "-n",
            "/usr/bin/systemctl",
            "restart",
            WIFI_RECOVERY_UNIT,
        )

    asyncio.run(scenario())


def test_fixed_recovery_command_rejects_unknown_unit():
    async def scenario():
        with pytest.raises(WifiBackendUnavailable, match="unknown recovery unit"):
            await LinuxNetworkManagerBackend._run_fixed_recovery_unit(
                "another.service",
                1,
            )

    asyncio.run(scenario())
