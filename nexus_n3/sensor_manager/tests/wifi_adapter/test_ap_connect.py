"""Connect to an x-IMU3 access point using the sensor Wi-Fi interface.

This live test:

1. Confirms the Nexus sensor AP is active.
2. Stops the Nexus sensor AP.
3. Scans for an x-IMU3 access point.
4. Connects to the first matching x-IMU3 AP.
5. Confirms that a usable IPv4 address was assigned.
6. Disconnects from the x-IMU3.
7. Restores the Nexus sensor AP.

The temporary x-IMU3 NetworkManager profile is volatile and should disappear
when disconnected.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import ipaddress
import os
from typing import Any

import pytest
from dbus_fast import Message, Variant
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType, MessageType


NETWORK_MANAGER_SERVICE = "org.freedesktop.NetworkManager"
NETWORK_MANAGER_PATH = "/org/freedesktop/NetworkManager"

NETWORK_MANAGER_INTERFACE = "org.freedesktop.NetworkManager"
DBUS_PROPERTIES_INTERFACE = "org.freedesktop.DBus.Properties"

DEVICE_INTERFACE = "org.freedesktop.NetworkManager.Device"
WIRELESS_DEVICE_INTERFACE = "org.freedesktop.NetworkManager.Device.Wireless"
ACCESS_POINT_INTERFACE = "org.freedesktop.NetworkManager.AccessPoint"
ACTIVE_CONNECTION_INTERFACE = (
    "org.freedesktop.NetworkManager.Connection.Active"
)
IP4_CONFIG_INTERFACE = "org.freedesktop.NetworkManager.IP4Config"

SENSOR_INTERFACE = os.getenv(
    "NEXUS_SENSOR_INTERFACE",
    "wlx00c0cabaa751",
)

SENSOR_CONNECTION = os.getenv(
    "NEXUS_SENSOR_CONNECTION",
    "nexus-n3-sensor-ap",
)

PROVISION_CONNECTION = os.getenv(
    "XIMU3_PROVISION_CONNECTION",
    "ximu3-provision",
)

XIMU3_SSID_PREFIX = os.getenv(
    "XIMU3_SSID_PREFIX",
    "x-IMU3",
)

SCAN_TIMEOUT_SECONDS = float(
    os.getenv("XIMU3_SCAN_TIMEOUT_SECONDS", "20")
)

CONNECT_TIMEOUT_SECONDS = float(
    os.getenv("XIMU3_CONNECT_TIMEOUT_SECONDS", "30")
)

RESTORE_TIMEOUT_SECONDS = float(
    os.getenv("NEXUS_AP_RESTORE_TIMEOUT_SECONDS", "60")
)

DEVICE_STATE_DISCONNECTED = 30
DEVICE_STATE_ACTIVATED = 100
DEVICE_STATE_FAILED = 120

ACTIVE_CONNECTION_STATE_ACTIVATING = 1
ACTIVE_CONNECTION_STATE_ACTIVATED = 2
ACTIVE_CONNECTION_STATE_DEACTIVATING = 3
ACTIVE_CONNECTION_STATE_DEACTIVATED = 4


pytestmark = pytest.mark.asyncio


class NetworkManagerDBusError(RuntimeError):
    """Raised when NetworkManager returns a D-Bus error."""


@dataclass(frozen=True)
class WifiAccessPoint:
    object_path: str
    ssid: str
    bssid: str
    strength: int
    frequency_mhz: int


@dataclass(frozen=True)
class IPv4Configuration:
    address: str
    prefix: int
    gateway: str

    @property
    def cidr(self) -> str:
        return f"{self.address}/{self.prefix}"


class NetworkManagerClient:
    """Small NetworkManager D-Bus client used by this hardware test."""

    def __init__(self, bus: MessageBus):
        self.bus = bus

    async def call(
        self,
        *,
        path: str,
        interface: str,
        member: str,
        signature: str = "",
        body: list[Any] | None = None,
    ) -> list[Any]:
        reply = await self.bus.call(
            Message(
                destination=NETWORK_MANAGER_SERVICE,
                path=path,
                interface=interface,
                member=member,
                signature=signature,
                body=body or [],
            )
        )

        if reply.message_type == MessageType.ERROR:
            detail = str(reply.body[0]) if reply.body else "No error detail"
            raise NetworkManagerDBusError(
                f"{interface}.{member} failed: "
                f"{reply.error_name}: {detail}"
            )

        return reply.body

    async def get_property(
        self,
        *,
        path: str,
        interface: str,
        property_name: str,
    ) -> Any:
        body = await self.call(
            path=path,
            interface=DBUS_PROPERTIES_INTERFACE,
            member="Get",
            signature="ss",
            body=[interface, property_name],
        )

        if not body:
            raise NetworkManagerDBusError(
                f"Property {interface}.{property_name} returned no value"
            )

        return body[0].value

    async def get_device_path(self, interface_name: str) -> str:
        body = await self.call(
            path=NETWORK_MANAGER_PATH,
            interface=NETWORK_MANAGER_INTERFACE,
            member="GetDeviceByIpIface",
            signature="s",
            body=[interface_name],
        )

        if not body:
            raise NetworkManagerDBusError(
                f"No NetworkManager device found for {interface_name!r}"
            )

        return str(body[0])

    async def find_active_connection(
        self,
        connection_id: str,
    ) -> str | None:
        active_connections = await self.get_property(
            path=NETWORK_MANAGER_PATH,
            interface=NETWORK_MANAGER_INTERFACE,
            property_name="ActiveConnections",
        )

        for active_path in active_connections:
            try:
                active_id = await self.get_property(
                    path=active_path,
                    interface=ACTIVE_CONNECTION_INTERFACE,
                    property_name="Id",
                )
            except NetworkManagerDBusError:
                continue

            if active_id == connection_id:
                return str(active_path)

        return None

    async def deactivate_connection(
        self,
        active_connection_path: str,
    ) -> None:
        await self.call(
            path=NETWORK_MANAGER_PATH,
            interface=NETWORK_MANAGER_INTERFACE,
            member="DeactivateConnection",
            signature="o",
            body=[active_connection_path],
        )

    async def disconnect_device(self, device_path: str) -> None:
        """Force the device into a clean disconnected state."""
        await self.call(
            path=device_path,
            interface=DEVICE_INTERFACE,
            member="Disconnect",
        )

    async def get_active_connections_for_device(
        self,
        device_path: str,
    ) -> list[str]:
        active_connections = await self.get_property(
            path=NETWORK_MANAGER_PATH,
            interface=NETWORK_MANAGER_INTERFACE,
            property_name="ActiveConnections",
        )

        matching: list[str] = []

        for active_path in active_connections:
            try:
                devices = await self.get_property(
                    path=active_path,
                    interface=ACTIVE_CONNECTION_INTERFACE,
                    property_name="Devices",
                )
            except NetworkManagerDBusError:
                continue

            if device_path in devices:
                matching.append(str(active_path))

        return matching

    async def activate_saved_connection(
        self,
        *,
        connection_path: str,
        device_path: str,
    ) -> str:
        body = await self.call(
            path=NETWORK_MANAGER_PATH,
            interface=NETWORK_MANAGER_INTERFACE,
            member="ActivateConnection",
            signature="ooo",
            body=[
                connection_path,
                device_path,
                "/",
            ],
        )

        if not body:
            raise NetworkManagerDBusError(
                "ActivateConnection returned no active connection path"
            )

        return str(body[0])

    async def request_scan(self, device_path: str) -> None:
        await self.call(
            path=device_path,
            interface=WIRELESS_DEVICE_INTERFACE,
            member="RequestScan",
            signature="a{sv}",
            body=[{}],
        )

    async def get_access_points(
        self,
        device_path: str,
    ) -> list[WifiAccessPoint]:
        body = await self.call(
            path=device_path,
            interface=WIRELESS_DEVICE_INTERFACE,
            member="GetAllAccessPoints",
        )

        access_point_paths = body[0] if body else []
        access_points: list[WifiAccessPoint] = []

        for access_point_path in access_point_paths:
            try:
                raw_ssid = await self.get_property(
                    path=access_point_path,
                    interface=ACCESS_POINT_INTERFACE,
                    property_name="Ssid",
                )
                bssid = await self.get_property(
                    path=access_point_path,
                    interface=ACCESS_POINT_INTERFACE,
                    property_name="HwAddress",
                )
                strength = await self.get_property(
                    path=access_point_path,
                    interface=ACCESS_POINT_INTERFACE,
                    property_name="Strength",
                )
                frequency = await self.get_property(
                    path=access_point_path,
                    interface=ACCESS_POINT_INTERFACE,
                    property_name="Frequency",
                )
            except NetworkManagerDBusError:
                # The AP may disappear while its properties are being read.
                continue

            access_points.append(
                WifiAccessPoint(
                    object_path=str(access_point_path),
                    ssid=bytes(raw_ssid).decode(
                        "utf-8",
                        errors="replace",
                    ),
                    bssid=str(bssid),
                    strength=int(strength),
                    frequency_mhz=int(frequency),
                )
            )

        return access_points

    async def scan_and_wait(
        self,
        *,
        device_path: str,
        timeout: float,
    ) -> list[WifiAccessPoint]:
        previous_scan = int(
            await self.get_property(
                path=device_path,
                interface=WIRELESS_DEVICE_INTERFACE,
                property_name="LastScan",
            )
        )

        await self.request_scan(device_path)

        deadline = asyncio.get_running_loop().time() + timeout

        while asyncio.get_running_loop().time() < deadline:
            current_scan = int(
                await self.get_property(
                    path=device_path,
                    interface=WIRELESS_DEVICE_INTERFACE,
                    property_name="LastScan",
                )
            )

            if current_scan != previous_scan:
                return await self.get_access_points(device_path)

            await asyncio.sleep(0.25)

        raise TimeoutError(
            "NetworkManager did not complete a fresh Wi-Fi scan "
            f"within {timeout:.1f} seconds"
        )

    async def add_and_activate_open_wifi(
        self,
        *,
        device_path: str,
        access_point: WifiAccessPoint,
    ) -> tuple[str, str]:
        """Create and activate a volatile connection to an open Wi-Fi AP."""

        settings = {
            "connection": {
                "id": Variant("s", PROVISION_CONNECTION),
                "type": Variant("s", "802-11-wireless"),
                "interface-name": Variant("s", SENSOR_INTERFACE),
                "autoconnect": Variant("b", False),
            },
            "802-11-wireless": {
                "ssid": Variant("ay", access_point.ssid.encode("utf-8")),
                "mode": Variant("s", "infrastructure"),
            },
            "ipv4": {
                "method": Variant("s", "auto"),
                "never-default": Variant("b", True),
            },
            "ipv6": {
                "method": Variant("s", "disabled"),
            },
        }

        options = {
            "persist": Variant("s", "volatile"),
            "bind-activation": Variant("s", "dbus-client"),
        }

        body = await self.call(
            path=NETWORK_MANAGER_PATH,
            interface=NETWORK_MANAGER_INTERFACE,
            member="AddAndActivateConnection2",
            signature="a{sa{sv}}ooa{sv}",
            body=[
                settings,
                device_path,
                access_point.object_path,
                options,
            ],
        )

        if len(body) < 2:
            raise NetworkManagerDBusError(
                "AddAndActivateConnection2 returned incomplete output"
            )

        connection_path = str(body[0])
        active_connection_path = str(body[1])

        return connection_path, active_connection_path

    async def wait_for_device_state(
        self,
        *,
        device_path: str,
        expected_state: int,
        timeout: float,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        last_state: int | None = None

        while asyncio.get_running_loop().time() < deadline:
            last_state = int(
                await self.get_property(
                    path=device_path,
                    interface=DEVICE_INTERFACE,
                    property_name="State",
                )
            )

            if last_state == expected_state:
                return

            if last_state == DEVICE_STATE_FAILED:
                reason = await self.get_property(
                    path=device_path,
                    interface=DEVICE_INTERFACE,
                    property_name="StateReason",
                )
                raise NetworkManagerDBusError(
                    f"Wi-Fi device entered failed state: {reason}"
                )

            await asyncio.sleep(0.25)

        raise TimeoutError(
            f"Device did not reach state {expected_state}; "
            f"last state was {last_state}"
        )

    async def wait_for_active_connection(
        self,
        *,
        active_connection_path: str,
        timeout: float,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        last_state: int | None = None

        while asyncio.get_running_loop().time() < deadline:
            try:
                last_state = int(
                    await self.get_property(
                        path=active_connection_path,
                        interface=ACTIVE_CONNECTION_INTERFACE,
                        property_name="State",
                    )
                )
            except NetworkManagerDBusError as exc:
                raise NetworkManagerDBusError(
                    "The temporary connection disappeared before activation"
                ) from exc

            if last_state == ACTIVE_CONNECTION_STATE_ACTIVATED:
                return

            if last_state == ACTIVE_CONNECTION_STATE_DEACTIVATED:
                raise NetworkManagerDBusError(
                    "The temporary connection was deactivated "
                    "before becoming active"
                )

            await asyncio.sleep(0.25)

        raise TimeoutError(
            "Connection did not become active; "
            f"last active-connection state was {last_state}"
        )

    async def wait_for_active_connection_to_disappear(
        self,
        *,
        active_connection_path: str,
        timeout: float,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + timeout

        while asyncio.get_running_loop().time() < deadline:
            active_connections = await self.get_property(
                path=NETWORK_MANAGER_PATH,
                interface=NETWORK_MANAGER_INTERFACE,
                property_name="ActiveConnections",
            )

            if active_connection_path not in active_connections:
                return

            await asyncio.sleep(0.25)

        raise TimeoutError(
            "Temporary connection remained active after deactivation"
        )

    async def get_ipv4_configuration(
        self,
        active_connection_path: str,
    ) -> IPv4Configuration | None:
        ip4_config_path = str(
            await self.get_property(
                path=active_connection_path,
                interface=ACTIVE_CONNECTION_INTERFACE,
                property_name="Ip4Config",
            )
        )

        if ip4_config_path == "/":
            return None

        address_data = await self.get_property(
            path=ip4_config_path,
            interface=IP4_CONFIG_INTERFACE,
            property_name="AddressData",
        )

        gateway = str(
            await self.get_property(
                path=ip4_config_path,
                interface=IP4_CONFIG_INTERFACE,
                property_name="Gateway",
            )
        )

        for entry in address_data:
            address_variant = entry.get("address")
            prefix_variant = entry.get("prefix")

            if address_variant is None or prefix_variant is None:
                continue

            address = str(address_variant.value)
            prefix = int(prefix_variant.value)

            parsed = ipaddress.ip_address(address)

            if not isinstance(parsed, ipaddress.IPv4Address):
                continue

            # A link-local address would indicate DHCP did not succeed.
            if parsed.is_link_local or parsed.is_loopback:
                continue

            return IPv4Configuration(
                address=address,
                prefix=prefix,
                gateway=gateway,
            )

        return None

    async def wait_for_ipv4_configuration(
        self,
        *,
        active_connection_path: str,
        timeout: float,
    ) -> IPv4Configuration:
        deadline = asyncio.get_running_loop().time() + timeout

        while asyncio.get_running_loop().time() < deadline:
            configuration = await self.get_ipv4_configuration(
                active_connection_path
            )

            if configuration is not None:
                return configuration

            await asyncio.sleep(0.25)

        raise TimeoutError(
            "No usable IPv4 address was assigned by the x-IMU3 AP"
        )

    async def wait_for_connection_id_activated(
        self,
        *,
        connection_id: str,
        device_path: str,
        timeout: float,
    ) -> str:
        """Wait for a saved connection to become active by stable ID."""

        deadline = asyncio.get_running_loop().time() + timeout
        last_active_path: str | None = None
        last_active_state: int | None = None
        last_device_state: int | None = None
        last_device_reason: Any = None

        while asyncio.get_running_loop().time() < deadline:
            active_path = await self.find_active_connection(connection_id)

            if active_path is not None:
                last_active_path = active_path

                try:
                    last_active_state = int(
                        await self.get_property(
                            path=active_path,
                            interface=ACTIVE_CONNECTION_INTERFACE,
                            property_name="State",
                        )
                    )
                except NetworkManagerDBusError:
                    await asyncio.sleep(0.25)
                    continue

                if last_active_state == ACTIVE_CONNECTION_STATE_ACTIVATED:
                    await self.wait_for_device_state(
                        device_path=device_path,
                        expected_state=DEVICE_STATE_ACTIVATED,
                        timeout=max(
                            1.0,
                            deadline - asyncio.get_running_loop().time(),
                        ),
                    )
                    return active_path

            last_device_state = int(
                await self.get_property(
                    path=device_path,
                    interface=DEVICE_INTERFACE,
                    property_name="State",
                )
            )

            last_device_reason = await self.get_property(
                path=device_path,
                interface=DEVICE_INTERFACE,
                property_name="StateReason",
            )

            if last_device_state == DEVICE_STATE_FAILED:
                raise NetworkManagerDBusError(
                    f"Wi-Fi device failed while restoring "
                    f"{connection_id!r}: {last_device_reason}"
                )

            await asyncio.sleep(0.25)

        raise TimeoutError(
            f"Connection {connection_id!r} did not become active; "
            f"active_path={last_active_path!r}, "
            f"active_state={last_active_state}, "
            f"device_state={last_device_state}, "
            f"device_reason={last_device_reason}"
        )

    async def wait_for_device_disconnected(
        self,
        *,
        device_path: str,
        timeout: float,
    ) -> None:
        await self.wait_for_device_state(
            device_path=device_path,
            expected_state=DEVICE_STATE_DISCONNECTED,
            timeout=timeout,
        )

    async def quiesce_device(
        self,
        *,
        device_path: str,
        timeout: float,
    ) -> None:
        """Cancel active or pending connections and leave Wi-Fi idle."""

        active_paths = await self.get_active_connections_for_device(
            device_path
        )

        for active_path in active_paths:
            try:
                await self.deactivate_connection(active_path)
            except NetworkManagerDBusError:
                pass

        try:
            await self.disconnect_device(device_path)
        except NetworkManagerDBusError:
            # The device may already be disconnected.
            pass

        await self.wait_for_device_disconnected(
            device_path=device_path,
            timeout=timeout,
        )


async def restore_sensor_ap(
    network_manager: NetworkManagerClient,
    *,
    connection_path: str,
    device_path: str,
) -> None:
    """Restore the saved Nexus sensor AP with one controlled retry."""

    last_error: Exception | None = None

    for attempt in range(1, 3):
        print(
            f"Preparing {SENSOR_INTERFACE!r} to restore "
            f"{SENSOR_CONNECTION!r} (attempt {attempt}/2)"
        )

        try:
            await network_manager.quiesce_device(
                device_path=device_path,
                timeout=20,
            )

            # Allow the driver and wpa_supplicant to finish changing from
            # station mode back to AP mode.
            await asyncio.sleep(2.0)

            existing_active_path = (
                await network_manager.find_active_connection(
                    SENSOR_CONNECTION
                )
            )

            if existing_active_path is not None:
                try:
                    await network_manager.deactivate_connection(
                        existing_active_path
                    )
                    await network_manager.wait_for_active_connection_to_disappear(
                        active_connection_path=existing_active_path,
                        timeout=15,
                    )
                except NetworkManagerDBusError:
                    pass

            print(
                f"Restoring sensor AP connection "
                f"{SENSOR_CONNECTION!r}"
            )

            await network_manager.activate_saved_connection(
                connection_path=connection_path,
                device_path=device_path,
            )

            await network_manager.wait_for_connection_id_activated(
                connection_id=SENSOR_CONNECTION,
                device_path=device_path,
                timeout=RESTORE_TIMEOUT_SECONDS,
            )

            print(
                f"Sensor AP connection "
                f"{SENSOR_CONNECTION!r} restored"
            )
            return

        except Exception as exc:
            last_error = exc

            try:
                device_state = await network_manager.get_property(
                    path=device_path,
                    interface=DEVICE_INTERFACE,
                    property_name="State",
                )
                state_reason = await network_manager.get_property(
                    path=device_path,
                    interface=DEVICE_INTERFACE,
                    property_name="StateReason",
                )
            except Exception:
                device_state = "unknown"
                state_reason = "unknown"

            print(
                f"Restore attempt {attempt} failed: {exc}; "
                f"device_state={device_state}, "
                f"state_reason={state_reason}"
            )

            if attempt < 2:
                await asyncio.sleep(3.0)

    raise RuntimeError(
        f"Failed to restore {SENSOR_CONNECTION!r} after two attempts"
    ) from last_error


async def test_ap_connects_to_ximu3_and_restores_sensor_ap() -> None:
    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    network_manager = NetworkManagerClient(bus)

    device_path: str | None = None
    sensor_connection_path: str | None = None
    temporary_active_path: str | None = None
    sensor_ap_was_deactivated = False

    try:
        device_path = await network_manager.get_device_path(
            SENSOR_INTERFACE
        )

        sensor_active_path = (
            await network_manager.find_active_connection(
                SENSOR_CONNECTION
            )
        )

        assert sensor_active_path is not None, (
            f"Precondition failed: {SENSOR_CONNECTION!r} is not active"
        )

        sensor_connection_path = str(
            await network_manager.get_property(
                path=sensor_active_path,
                interface=ACTIVE_CONNECTION_INTERFACE,
                property_name="Connection",
            )
        )

        print(
            f"Stopping sensor AP {SENSOR_CONNECTION!r} "
            f"on {SENSOR_INTERFACE!r}"
        )

        sensor_ap_was_deactivated = True

        print(
            f"Disconnecting {SENSOR_INTERFACE!r} and suppressing "
            "automatic reconnection during provisioning"
        )

        await network_manager.disconnect_device(device_path)

        await network_manager.wait_for_device_disconnected(
            device_path=device_path,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )

        print(
            f"Scanning on {SENSOR_INTERFACE!r} "
            f"for SSIDs beginning with {XIMU3_SSID_PREFIX!r}"
        )

        access_points = await network_manager.scan_and_wait(
            device_path=device_path,
            timeout=SCAN_TIMEOUT_SECONDS,
        )

        ximu3_access_points = sorted(
            (
                access_point
                for access_point in access_points
                if access_point.ssid.casefold().startswith(
                    XIMU3_SSID_PREFIX.casefold()
                )
            ),
            key=lambda access_point: access_point.strength,
            reverse=True,
        )

        assert ximu3_access_points, (
            f"No access point beginning with "
            f"{XIMU3_SSID_PREFIX!r} was discovered"
        )

        selected = ximu3_access_points[0]

        print(
            "Selected x-IMU3 AP: "
            f"ssid={selected.ssid!r}, "
            f"bssid={selected.bssid}, "
            f"signal={selected.strength}%, "
            f"frequency={selected.frequency_mhz} MHz"
        )

        print(f"Connecting to {selected.ssid!r}")

        _, temporary_active_path = (
            await network_manager.add_and_activate_open_wifi(
                device_path=device_path,
                access_point=selected,
            )
        )

        await network_manager.wait_for_active_connection(
            active_connection_path=temporary_active_path,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )

        await network_manager.wait_for_device_state(
            device_path=device_path,
            expected_state=DEVICE_STATE_ACTIVATED,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )

        ipv4 = await network_manager.wait_for_ipv4_configuration(
            active_connection_path=temporary_active_path,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )

        print(
            f"Connected to {selected.ssid!r}: "
            f"address={ipv4.cidr}, gateway={ipv4.gateway!r}"
        )

        assert ipv4.address
        assert ipv4.prefix > 0

    finally:
        try:
            if (
                temporary_active_path is not None
                and device_path is not None
            ):
                print("Disconnecting temporary x-IMU3 connection")

                try:
                    await network_manager.deactivate_connection(
                        temporary_active_path
                    )
                except NetworkManagerDBusError as exc:
                    print(
                        "Temporary connection was already unavailable: "
                        f"{exc}"
                    )

                # Cancel any automatic activation that started while the
                # temporary connection was being removed.
                await network_manager.quiesce_device(
                    device_path=device_path,
                    timeout=20,
                )

        finally:
            try:
                if (
                    sensor_ap_was_deactivated
                    and sensor_connection_path is not None
                    and device_path is not None
                ):
                    await restore_sensor_ap(
                        network_manager,
                        connection_path=sensor_connection_path,
                        device_path=device_path,
                    )
            finally:
                bus.disconnect()