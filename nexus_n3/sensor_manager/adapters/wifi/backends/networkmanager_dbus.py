"""Small asynchronous NetworkManager D-Bus client used by the Linux backend."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import ipaddress
from typing import Any

from dbus_fast import Message, Variant
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType, MessageType

from ..models import IPv4Configuration


NM_SERVICE = "org.freedesktop.NetworkManager"
NM_PATH = "/org/freedesktop/NetworkManager"
NM_SETTINGS_PATH = "/org/freedesktop/NetworkManager/Settings"
NM_INTERFACE = "org.freedesktop.NetworkManager"
PROPERTIES_INTERFACE = "org.freedesktop.DBus.Properties"
SETTINGS_INTERFACE = "org.freedesktop.NetworkManager.Settings"
SETTINGS_CONNECTION_INTERFACE = "org.freedesktop.NetworkManager.Settings.Connection"
DEVICE_INTERFACE = "org.freedesktop.NetworkManager.Device"
WIRELESS_INTERFACE = "org.freedesktop.NetworkManager.Device.Wireless"
ACCESS_POINT_INTERFACE = "org.freedesktop.NetworkManager.AccessPoint"
ACTIVE_CONNECTION_INTERFACE = "org.freedesktop.NetworkManager.Connection.Active"
IP4_CONFIG_INTERFACE = "org.freedesktop.NetworkManager.IP4Config"

DEVICE_DISCONNECTED = 30
DEVICE_ACTIVATED = 100
DEVICE_FAILED = 120
ACTIVE_ACTIVATED = 2
ACTIVE_DEACTIVATED = 4
AP_FLAGS_PRIVACY = 0x1


def _is_access_point_secured(flags: int, wpa_flags: int, rsn_flags: int) -> bool:
    """Return whether an AP requires credentials.

    NetworkManager's general AP flags also contain WPS capability bits.  Only
    the privacy bit indicates legacy protection; WPA and RSN advertise their
    key-management capabilities separately.
    """

    return bool((flags & AP_FLAGS_PRIVACY) or wpa_flags or rsn_flags)


class NetworkManagerDBusError(RuntimeError):
    """A sanitized NetworkManager D-Bus operation failure."""


@dataclass(frozen=True)
class NetworkManagerAccessPoint:
    path: str
    ssid: str
    bssid: str
    strength: int
    frequency_mhz: int
    secured: bool


class NetworkManagerClient:
    """NetworkManager operations required by the sensor Wi-Fi backend."""

    def __init__(self, bus: MessageBus | None = None) -> None:
        self._bus = bus
        self._owns_bus = bus is None

    async def connect(self) -> None:
        if self._bus is None:
            self._bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            self._owns_bus = True

    def close(self) -> None:
        if self._bus is not None and self._owns_bus:
            self._bus.disconnect()
        self._bus = None

    async def call(
        self,
        *,
        path: str,
        interface: str,
        member: str,
        signature: str = "",
        body: list[Any] | None = None,
    ) -> list[Any]:
        if self._bus is None:
            raise NetworkManagerDBusError("NetworkManager D-Bus is not connected")
        reply = await self._bus.call(
            Message(
                destination=NM_SERVICE,
                path=path,
                interface=interface,
                member=member,
                signature=signature,
                body=body or [],
            )
        )
        if reply.message_type == MessageType.ERROR:
            detail = str(reply.body[0]) if reply.body else "no error detail"
            raise NetworkManagerDBusError(
                f"{interface}.{member} failed: {reply.error_name}: {detail}"
            )
        return reply.body

    async def get_property(self, path: str, interface: str, name: str) -> Any:
        body = await self.call(
            path=path,
            interface=PROPERTIES_INTERFACE,
            member="Get",
            signature="ss",
            body=[interface, name],
        )
        if not body:
            raise NetworkManagerDBusError(f"Property {interface}.{name} was empty")
        return body[0].value

    async def get_device_path(self, interface_name: str) -> str:
        body = await self.call(
            path=NM_PATH,
            interface=NM_INTERFACE,
            member="GetDeviceByIpIface",
            signature="s",
            body=[interface_name],
        )
        if not body:
            raise NetworkManagerDBusError(
                f"No NetworkManager device found for {interface_name!r}"
            )
        return str(body[0])

    async def list_saved_connections(self) -> list[str]:
        body = await self.call(
            path=NM_SETTINGS_PATH,
            interface=SETTINGS_INTERFACE,
            member="ListConnections",
        )
        return [str(path) for path in (body[0] if body else [])]

    async def get_connection_settings(self, path: str) -> dict[str, Any]:
        body = await self.call(
            path=path,
            interface=SETTINGS_CONNECTION_INTERFACE,
            member="GetSettings",
        )
        return body[0] if body else {}

    async def find_saved_connection(self, connection_id: str) -> str | None:
        for path in await self.list_saved_connections():
            try:
                settings = await self.get_connection_settings(path)
            except NetworkManagerDBusError:
                continue
            value = settings.get("connection", {}).get("id")
            if value is not None and value.value == connection_id:
                return path
        return None

    async def find_active_connection(self, connection_id: str) -> str | None:
        paths = await self.get_property(NM_PATH, NM_INTERFACE, "ActiveConnections")
        for path in paths:
            try:
                active_id = await self.get_property(
                    str(path), ACTIVE_CONNECTION_INTERFACE, "Id"
                )
            except NetworkManagerDBusError:
                continue
            if active_id == connection_id:
                return str(path)
        return None

    async def active_connections_for_device(self, device_path: str) -> list[str]:
        paths = await self.get_property(NM_PATH, NM_INTERFACE, "ActiveConnections")
        matching = []
        for path in paths:
            try:
                devices = await self.get_property(
                    str(path), ACTIVE_CONNECTION_INTERFACE, "Devices"
                )
            except NetworkManagerDBusError:
                continue
            if device_path in devices:
                matching.append(str(path))
        return matching

    async def activate(self, connection_path: str, device_path: str) -> str:
        body = await self.call(
            path=NM_PATH,
            interface=NM_INTERFACE,
            member="ActivateConnection",
            signature="ooo",
            body=[connection_path, device_path, "/"],
        )
        if not body:
            raise NetworkManagerDBusError("ActivateConnection returned no path")
        return str(body[0])

    async def deactivate(self, active_path: str) -> None:
        await self.call(
            path=NM_PATH,
            interface=NM_INTERFACE,
            member="DeactivateConnection",
            signature="o",
            body=[active_path],
        )

    async def disconnect_device(self, device_path: str) -> None:
        await self.call(
            path=device_path,
            interface=DEVICE_INTERFACE,
            member="Disconnect",
        )

    async def delete_connection(self, connection_path: str) -> None:
        await self.call(
            path=connection_path,
            interface=SETTINGS_CONNECTION_INTERFACE,
            member="Delete",
        )

    async def wait_for_device_state(
        self, device_path: str, expected: int, timeout: float
    ) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        last_state = None
        while asyncio.get_running_loop().time() < deadline:
            last_state = int(
                await self.get_property(device_path, DEVICE_INTERFACE, "State")
            )
            if last_state == expected:
                return
            if last_state == DEVICE_FAILED:
                reason = await self.get_property(
                    device_path, DEVICE_INTERFACE, "StateReason"
                )
                raise NetworkManagerDBusError(
                    f"Wi-Fi device entered failed state: {reason}"
                )
            await asyncio.sleep(0.25)
        raise TimeoutError(
            f"Wi-Fi device did not reach state {expected}; last state={last_state}"
        )

    async def wait_for_active(self, active_path: str, timeout: float) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        last_state = None
        while asyncio.get_running_loop().time() < deadline:
            last_state = int(
                await self.get_property(
                    active_path, ACTIVE_CONNECTION_INTERFACE, "State"
                )
            )
            if last_state == ACTIVE_ACTIVATED:
                return
            if last_state == ACTIVE_DEACTIVATED:
                raise NetworkManagerDBusError(
                    "Connection deactivated before becoming active"
                )
            await asyncio.sleep(0.25)
        raise TimeoutError(
            f"Connection did not activate; last state={last_state}"
        )

    async def wait_for_connection_id(
        self,
        connection_id: str,
        device_path: str,
        timeout: float,
    ) -> str:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            active_path = await self.find_active_connection(connection_id)
            if active_path is not None:
                try:
                    await self.wait_for_active(
                        active_path,
                        max(0.25, deadline - asyncio.get_running_loop().time()),
                    )
                    await self.wait_for_device_state(
                        device_path,
                        DEVICE_ACTIVATED,
                        max(0.25, deadline - asyncio.get_running_loop().time()),
                    )
                    return active_path
                except NetworkManagerDBusError:
                    pass
            await asyncio.sleep(0.25)
        raise TimeoutError(f"Connection {connection_id!r} did not activate")

    async def quiesce(self, device_path: str, timeout: float) -> None:
        for active_path in await self.active_connections_for_device(device_path):
            try:
                await self.deactivate(active_path)
            except NetworkManagerDBusError:
                pass
        try:
            await self.disconnect_device(device_path)
        except NetworkManagerDBusError:
            pass
        await self.wait_for_device_state(device_path, DEVICE_DISCONNECTED, timeout)

    async def scan(self, device_path: str, timeout: float) -> list[NetworkManagerAccessPoint]:
        previous = int(
            await self.get_property(device_path, WIRELESS_INTERFACE, "LastScan")
        )
        await self.call(
            path=device_path,
            interface=WIRELESS_INTERFACE,
            member="RequestScan",
            signature="a{sv}",
            body=[{}],
        )
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            current = int(
                await self.get_property(device_path, WIRELESS_INTERFACE, "LastScan")
            )
            if current != previous:
                return await self.get_access_points(device_path)
            await asyncio.sleep(0.25)
        raise TimeoutError("NetworkManager did not complete a fresh Wi-Fi scan")

    async def get_access_points(
        self, device_path: str
    ) -> list[NetworkManagerAccessPoint]:
        body = await self.call(
            path=device_path,
            interface=WIRELESS_INTERFACE,
            member="GetAllAccessPoints",
        )
        results = []
        for raw_path in (body[0] if body else []):
            path = str(raw_path)
            try:
                raw_ssid = await self.get_property(path, ACCESS_POINT_INTERFACE, "Ssid")
                bssid = await self.get_property(
                    path, ACCESS_POINT_INTERFACE, "HwAddress"
                )
                strength = await self.get_property(
                    path, ACCESS_POINT_INTERFACE, "Strength"
                )
                frequency = await self.get_property(
                    path, ACCESS_POINT_INTERFACE, "Frequency"
                )
                flags = int(
                    await self.get_property(path, ACCESS_POINT_INTERFACE, "Flags")
                )
                wpa_flags = int(
                    await self.get_property(path, ACCESS_POINT_INTERFACE, "WpaFlags")
                )
                rsn_flags = int(
                    await self.get_property(path, ACCESS_POINT_INTERFACE, "RsnFlags")
                )
            except NetworkManagerDBusError:
                continue
            results.append(
                NetworkManagerAccessPoint(
                    path=path,
                    ssid=bytes(raw_ssid).decode("utf-8", errors="replace"),
                    bssid=str(bssid),
                    strength=int(strength),
                    frequency_mhz=int(frequency),
                    secured=_is_access_point_secured(
                        flags,
                        wpa_flags,
                        rsn_flags,
                    ),
                )
            )
        return results

    async def add_and_activate_open_wifi(
        self,
        device_path: str,
        access_point: NetworkManagerAccessPoint,
        interface_name: str,
        connection_id: str,
    ) -> tuple[str, str]:
        settings = {
            "connection": {
                "id": Variant("s", connection_id),
                "type": Variant("s", "802-11-wireless"),
                "interface-name": Variant("s", interface_name),
                "autoconnect": Variant("b", False),
            },
            "802-11-wireless": {
                "ssid": Variant("ay", access_point.ssid.encode()),
                "mode": Variant("s", "infrastructure"),
            },
            "ipv4": {
                "method": Variant("s", "auto"),
                "never-default": Variant("b", True),
            },
            "ipv6": {"method": Variant("s", "disabled")},
        }
        options = {
            "persist": Variant("s", "volatile"),
            "bind-activation": Variant("s", "dbus-client"),
        }
        body = await self.call(
            path=NM_PATH,
            interface=NM_INTERFACE,
            member="AddAndActivateConnection2",
            signature="a{sa{sv}}ooa{sv}",
            body=[settings, device_path, access_point.path, options],
        )
        if len(body) < 2:
            raise NetworkManagerDBusError(
                "AddAndActivateConnection2 returned incomplete output"
            )
        return str(body[0]), str(body[1])

    async def get_ipv4(self, active_path: str) -> IPv4Configuration | None:
        path = str(
            await self.get_property(
                active_path, ACTIVE_CONNECTION_INTERFACE, "Ip4Config"
            )
        )
        if path == "/":
            return None
        address_data = await self.get_property(path, IP4_CONFIG_INTERFACE, "AddressData")
        gateway = str(await self.get_property(path, IP4_CONFIG_INTERFACE, "Gateway"))
        for entry in address_data:
            address_value = entry.get("address")
            prefix_value = entry.get("prefix")
            if address_value is None or prefix_value is None:
                continue
            address = ipaddress.ip_address(str(address_value.value))
            if not isinstance(address, ipaddress.IPv4Address):
                continue
            if address.is_loopback or address.is_link_local:
                continue
            return IPv4Configuration(
                address=str(address),
                prefix=int(prefix_value.value),
                gateway=gateway,
            )
        return None

    async def wait_for_ipv4(
        self, active_path: str, timeout: float
    ) -> IPv4Configuration:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            configuration = await self.get_ipv4(active_path)
            if configuration is not None:
                return configuration
            await asyncio.sleep(0.25)
        raise TimeoutError("No usable IPv4 address was assigned")

    async def wait_until_available(
        self, interface_name: str, connection_id: str, timeout: float
    ) -> tuple[str, str]:
        deadline = asyncio.get_running_loop().time() + timeout
        last_error = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                device = await self.get_device_path(interface_name)
                connection = await self.find_saved_connection(connection_id)
                if connection is not None:
                    return device, connection
            except Exception as exc:
                last_error = exc
            await asyncio.sleep(0.5)
        raise TimeoutError("NetworkManager did not return after recovery") from last_error


def unwrap_setting(settings: dict[str, Any], section: str, key: str) -> Any:
    """Return one D-Bus setting value without exposing Variant to callers."""

    value = settings.get(section, {}).get(key)
    return None if value is None else value.value


__all__ = [
    "NetworkManagerAccessPoint",
    "NetworkManagerClient",
    "NetworkManagerDBusError",
    "unwrap_setting",
]
