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
    devices = {
        "wlx00c0cabaa751": "/device/wifi",
        "br-sensor": "/device/bridge",
        "enp0s31f6.20": "/device/vlan",
    }
    saved = {
        "nexus-n3-sensor-ap": "/settings/ap",
        "nexus-n3-sensor-bridge": "/settings/bridge",
        "nexus-n3-sensor-vlan": "/settings/vlan",
    }
    active_paths = {
        "nexus-n3-sensor-ap": "/active/ap",
        "nexus-n3-sensor-bridge": "/active/bridge",
        "nexus-n3-sensor-vlan": "/active/vlan",
    }
    active_devices = {
        "/device/wifi": ["/active/ap"],
        "/device/bridge": ["/active/bridge"],
        "/device/vlan": ["/active/vlan"],
    }

    def __init__(self, *, ap_active: bool = True) -> None:
        self.connected = False
        self.closed = False
        self.operations: list[object] = []
        self.network = IPv4Configuration("10.42.20.250", 24)
        self.active = dict(self.active_paths)
        self.device_active = {
            key: list(value) for key, value in self.active_devices.items()
        }
        if not ap_active:
            self.active.pop("nexus-n3-sensor-ap")
            self.device_active["/device/wifi"] = []
        self.settings = {
            "/settings/ap": {
                "connection": {
                    "id": _variant("nexus-n3-sensor-ap"),
                    "type": _variant("802-11-wireless"),
                    "interface-name": _variant("wlx00c0cabaa751"),
                    "master": _variant("br-sensor"),
                    "slave-type": _variant("bridge"),
                },
                "802-11-wireless": {
                    "mode": _variant("ap"),
                    "ssid": _variant(b"nexus-n3-sensors"),
                },
            },
            "/settings/bridge": {
                "connection": {
                    "id": _variant("nexus-n3-sensor-bridge"),
                    "type": _variant("bridge"),
                    "interface-name": _variant("br-sensor"),
                },
            },
            "/settings/vlan": {
                "connection": {
                    "id": _variant("nexus-n3-sensor-vlan"),
                    "type": _variant("vlan"),
                    "interface-name": _variant("enp0s31f6.20"),
                    "master": _variant("br-sensor"),
                    "slave-type": _variant("bridge"),
                },
                "vlan": {"id": _variant(20)},
            },
        }

    async def connect(self):
        self.connected = True
        self.operations.append("connect")

    def close(self):
        self.closed = True

    async def get_device_path(self, interface_name):
        self.operations.append(("get_device_path", interface_name))
        return self.devices[interface_name]

    async def find_saved_connection(self, connection_id):
        self.operations.append(("find_saved", connection_id))
        return self.saved.get(connection_id)

    async def get_connection_settings(self, path):
        self.operations.append(("get_settings", path))
        return self.settings[path]

    async def find_active_connection(self, connection_id):
        self.operations.append(("find_active", connection_id))
        return self.active.get(connection_id)

    async def active_connections_for_device(self, device_path):
        return self.device_active.get(device_path, [])

    async def activate(self, connection_path, device_path):
        self.operations.append(("activate", connection_path, device_path))
        self.active["nexus-n3-sensor-ap"] = "/active/ap"
        self.device_active["/device/wifi"] = ["/active/ap"]
        return "/active/ap"

    async def wait_for_connection_id(self, connection_id, device_path, timeout):
        self.operations.append(("wait_for_connection", connection_id))
        return self.active[connection_id]

    async def get_ipv4(self, active_path):
        self.operations.append(("get_ipv4", active_path))
        return self.network if active_path == "/active/bridge" else None

    async def wait_for_ipv4(self, active_path, timeout):
        return self.network if active_path == "/active/bridge" else None

    async def quiesce(self, device_path, timeout):
        self.operations.append(("quiesce", device_path))
        self.active.pop("nexus-n3-sensor-ap", None)
        self.device_active["/device/wifi"] = []

    async def wait_until_available(self, interface_name, connection_id, timeout):
        self.operations.append(("wait_until_available", interface_name, connection_id))
        return self.devices[interface_name], self.saved[connection_id]


def _config(**overrides) -> WifiRuntimeConfig:
    values = {
        "enabled": True,
        "backend": "linux-networkmanager",
        "wifi_interface": "wlx00c0cabaa751",
        "wifi_profile": "nexus-n3-sensor-ap",
        "bridge_interface": "br-sensor",
        "bridge_profile": "nexus-n3-sensor-bridge",
        "vlan_interface": "enp0s31f6.20",
        "vlan_profile": "nexus-n3-sensor-vlan",
        "vlan_id": 20,
        "expected_sensor_cidr": "10.42.20.250/24",
    }
    values.update(overrides)
    return WifiRuntimeConfig(**values)


@pytest.mark.parametrize(
    ("flags", "wpa_flags", "rsn_flags", "expected"),
    [
        (0, 0, 0, False),
        (0x2, 0, 0, False),
        (0x1, 0, 0, True),
        (0, 0x100, 0, True),
        (0, 0, 0x100, True),
    ],
)
def test_access_point_security_ignores_non_privacy_ap_flags(
    flags, wpa_flags, rsn_flags, expected
):
    assert _is_access_point_secured(flags, wpa_flags, rsn_flags) is expected


def test_reads_sensor_network_from_active_bridge_not_ap():
    async def scenario():
        client = FakeNetworkManagerClient()
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()
        network = await backend.ensure_ap_active()

        assert network == IPv4Configuration("10.42.20.250", 24)
        assert ("get_ipv4", "/active/bridge") in client.operations
        assert ("get_ipv4", "/active/ap") not in client.operations
        assert not any(
            isinstance(operation, tuple) and operation[0] == "activate"
            for operation in client.operations
        )

    asyncio.run(scenario())


def test_reactivates_only_saved_ap_profile_when_inactive():
    async def scenario():
        client = FakeNetworkManagerClient(ap_active=False)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()
        network = await backend.ensure_ap_active()

        assert network.cidr == "10.42.20.250/24"
        assert ("activate", "/settings/ap", "/device/wifi") in client.operations

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("path", "section", "key", "value", "message"),
    [
        ("/settings/ap", "802-11-wireless", "mode", "infrastructure", "is not an AP"),
        ("/settings/ap", "802-11-wireless", "ssid", b"other", "expected 'nexus-n3-sensors'"),
        ("/settings/ap", "connection", "master", "other", "is not attached"),
        ("/settings/vlan", "vlan", "id", 21, "expected 20"),
        ("/settings/vlan", "connection", "master", "other", "is not attached"),
        ("/settings/bridge", "connection", "type", "ethernet", "expected 'bridge'"),
    ],
)
def test_rejects_invalid_saved_topology(path, section, key, value, message):
    async def scenario():
        client = FakeNetworkManagerClient()
        client.settings[path][section][key] = _variant(value)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()
        with pytest.raises(WifiBackendUnavailable, match=message):
            await backend.ensure_ap_active()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        ("nexus-n3-sensor-bridge", "sensor bridge.*not active"),
        ("nexus-n3-sensor-vlan", "sensor VLAN.*not active"),
    ],
)
def test_rejects_inactive_provisioned_network_components(profile, message):
    async def scenario():
        client = FakeNetworkManagerClient()
        client.active.pop(profile)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()
        with pytest.raises(WifiBackendUnavailable, match=message):
            await backend.ensure_ap_active()

    asyncio.run(scenario())


def test_rejects_unexpected_bridge_address():
    async def scenario():
        client = FakeNetworkManagerClient()
        client.network = IPv4Configuration("192.168.60.1", 24)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()
        with pytest.raises(WifiBackendUnavailable, match="expected 10.42.20.250/24"):
            await backend.ensure_ap_active()

    asyncio.run(scenario())


def test_restore_returns_bridge_network(monkeypatch):
    async def scenario():
        client = FakeNetworkManagerClient(ap_active=False)
        backend = LinuxNetworkManagerBackend(_config(), client=client)
        await backend.initialize()

        async def no_sleep(_seconds):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)
        network = await backend.restore_ap()

        assert network.cidr == "10.42.20.250/24"
        assert ("get_ipv4", "/active/bridge") in client.operations

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
        assert (
            "wait_until_available",
            "wlx00c0cabaa751",
            "nexus-n3-sensor-ap",
        ) in client.operations

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
            WIFI_RECOVERY_UNIT, 1
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
                "another.service", 1
            )

    asyncio.run(scenario())
