"""Scan for an x-IMU3 AP and always restore the Nexus sensor AP.

This live hardware test accepts an initially inactive saved AP profile. It
activates the profile, stops it, scans for x-IMU3, and restores it through the
same two-attempt NetworkManager path used by the provisioning test.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import ipaddress
import json
import os
import time
from typing import Any

import pytest

ximu3 = pytest.importorskip(
    "ximu3",
    reason="The ximu3 package is required for this live hardware test",
)

from dbus_fast import Message, Variant
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType, MessageType


TEST_VERSION = "2026-08-05-scan-v2"

NETWORK_MANAGER_SERVICE = "org.freedesktop.NetworkManager"
NETWORK_MANAGER_PATH = "/org/freedesktop/NetworkManager"
NETWORK_MANAGER_SETTINGS_PATH = "/org/freedesktop/NetworkManager/Settings"

NETWORK_MANAGER_INTERFACE = "org.freedesktop.NetworkManager"
DBUS_PROPERTIES_INTERFACE = "org.freedesktop.DBus.Properties"
NETWORK_MANAGER_SETTINGS_INTERFACE = "org.freedesktop.NetworkManager.Settings"
SETTINGS_CONNECTION_INTERFACE = (
    "org.freedesktop.NetworkManager.Settings.Connection"
)

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

NEXUS_SENSOR_AP_SSID = os.getenv(
    "NEXUS_SENSOR_AP_SSID",
    "nexus-n3-sensors",
)

NEXUS_SENSOR_AP_PASSWORD = os.getenv(
    "NEXUS_SENSOR_AP_PASSWORD",
    "",
)

NEXUS_SENSOR_AP_CHANNEL = int(
    os.getenv("NEXUS_SENSOR_AP_CHANNEL", "36")
)

SENSOR_JOIN_TIMEOUT_SECONDS = float(
    os.getenv("XIMU3_SENSOR_JOIN_TIMEOUT_SECONDS", "90")
)

SCAN_TIMEOUT_SECONDS = float(
    os.getenv("XIMU3_SCAN_TIMEOUT_SECONDS", "20")
)

CONNECT_TIMEOUT_SECONDS = float(
    os.getenv("XIMU3_CONNECT_TIMEOUT_SECONDS", "30")
)

RESTORE_TIMEOUT_SECONDS = float(
    os.getenv("NEXUS_AP_RESTORE_TIMEOUT_SECONDS", "45")
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


@dataclass(frozen=True)
class Ximu3UdpResult:
    device_name: str
    serial_number: str
    interface: str
    ip_address: str
    tcp_port: int
    udp_send_port: int
    udp_receive_port: int


@dataclass(frozen=True)
class Ximu3ProvisioningResult:
    serial_number: str
    ap_ip_address: str


def _discover_and_ping_ximu3_udp_sync(
    local_ipv4: IPv4Configuration,
) -> Ximu3UdpResult:
    """Discover the connected x-IMU3 and verify its UDP connection.

    The vendor API is used only to validate the expected external behaviour:
    network announcement discovery, conversion to UDP configuration, opening
    the UDP connection, and receiving a ping response.
    """

    local_network = ipaddress.ip_interface(local_ipv4.cidr).network
    announcements = (
        ximu3.NetworkAnnouncement()
        .get_messages_after_short_delay()
    )

    matching = []

    for announcement in announcements:
        device_name = str(announcement.device_name)

        try:
            announced_ip = ipaddress.ip_address(
                str(announcement.ip_address)
            )
        except ValueError:
            continue

        if not device_name.casefold().startswith(
            XIMU3_SSID_PREFIX.casefold()
        ):
            continue

        if announced_ip not in local_network:
            continue

        matching.append(announcement)

    if not matching:
        visible = [
            {
                "name": str(message.device_name),
                "serial": str(message.serial_number),
                "ip": str(message.ip_address),
                "tcp": int(message.tcp_port),
                "udp_send": int(message.udp_send),
                "udp_receive": int(message.udp_receive),
            }
            for message in announcements
        ]

        raise AssertionError(
            "No x-IMU3 announcement was found on "
            f"{local_network}. Announcements received: {visible}"
        )

    announcement = matching[0]

    print(
        "Received x-IMU3 announcement: "
        f"name={announcement.device_name!r}, "
        f"serial={announcement.serial_number!r}, "
        f"ip={announcement.ip_address}, "
        f"tcp={announcement.tcp_port}, "
        f"udp_send={announcement.udp_send}, "
        f"udp_receive={announcement.udp_receive}"
    )

    config = announcement.to_udp_connection_config()
    connection = ximu3.Connection(config).open()

    try:
        response = connection.ping()

        if not response:
            raise AssertionError(
                "The x-IMU3 did not respond to a UDP ping"
            )

        print(
            "UDP ping response: "
            f"interface={response.interface!r}, "
            f"name={response.device_name!r}, "
            f"serial={response.serial_number!r}"
        )

        return Ximu3UdpResult(
            device_name=str(response.device_name),
            serial_number=str(response.serial_number),
            interface=str(response.interface),
            ip_address=str(announcement.ip_address),
            tcp_port=int(announcement.tcp_port),
            udp_send_port=int(announcement.udp_send),
            udp_receive_port=int(announcement.udp_receive),
        )

    finally:
        connection.close()


async def discover_and_ping_ximu3_udp(
    local_ipv4: IPv4Configuration,
    *,
    timeout: float = 20.0,
) -> Ximu3UdpResult:
    """Run the blocking vendor API without blocking the asyncio loop."""

    return await asyncio.wait_for(
        asyncio.to_thread(
            _discover_and_ping_ximu3_udp_sync,
            local_ipv4,
        ),
        timeout=timeout,
    )



def _normalise_command_key(value: str) -> str:
    return "".join(character for character in value.casefold() if character.isalnum())


def _assert_command_responses(
    commands: list[str],
    responses: list[Any],
) -> None:
    if len(responses) != len(commands):
        raise AssertionError(
            f"Expected {len(commands)} command responses, got {len(responses)}"
        )

    for command, response in zip(commands, responses):
        expected_key = str(next(iter(json.loads(command))))

        if response is None:
            raise AssertionError(
                f"No response received for x-IMU3 command {expected_key!r}"
            )

        error = getattr(response, "error", None)
        if error:
            raise AssertionError(
                f"x-IMU3 command {expected_key!r} failed: {error}"
            )

        actual_key = str(getattr(response, "key", ""))
        if _normalise_command_key(actual_key) != _normalise_command_key(expected_key):
            raise AssertionError(
                f"Unexpected response key for {expected_key!r}: {actual_key!r}"
            )


def _find_ximu3_announcement_on_network(
    *,
    local_ipv4: IPv4Configuration,
    serial_number: str | None = None,
) -> Any:
    local_network = ipaddress.ip_interface(local_ipv4.cidr).network
    announcements = ximu3.NetworkAnnouncement().get_messages_after_short_delay()

    for announcement in announcements:
        device_name = str(announcement.device_name)

        if not device_name.casefold().startswith(XIMU3_SSID_PREFIX.casefold()):
            continue

        try:
            announced_ip = ipaddress.ip_address(str(announcement.ip_address))
        except ValueError:
            continue

        if announced_ip not in local_network:
            continue

        if (
            serial_number is not None
            and str(announcement.serial_number) != serial_number
        ):
            continue

        return announcement

    visible = [
        {
            "name": str(message.device_name),
            "serial": str(message.serial_number),
            "ip": str(message.ip_address),
        }
        for message in announcements
    ]

    raise LookupError(
        f"No matching x-IMU3 announcement on {local_network}; "
        f"announcements={visible}"
    )


def _provision_ximu3_udp_sync(
    local_ipv4: IPv4Configuration,
    *,
    ssid: str,
    password: str,
    channel: int,
) -> Ximu3ProvisioningResult:
    """Write persistent Wi-Fi client settings over the AP-mode UDP link."""

    announcement = _find_ximu3_announcement_on_network(local_ipv4=local_ipv4)
    serial_number = str(announcement.serial_number)

    print(
        "Provisioning x-IMU3: "
        f"serial={serial_number!r}, "
        f"ap_ip={announcement.ip_address}, "
        f"target_ssid={ssid!r}, "
        f"target_channel={channel}"
    )

    connection = ximu3.Connection(
        announcement.to_udp_connection_config()
    ).open()

    try:
        ping_response = connection.ping()
        if not ping_response:
            raise AssertionError(
                "The x-IMU3 did not respond before provisioning"
            )

        if str(ping_response.serial_number) != serial_number:
            raise AssertionError(
                "The UDP ping serial number did not match the announcement"
            )

        commands = [
            json.dumps(
                {"wi_fi_client_ssid": ssid},
                separators=(",", ":"),
            ),
            json.dumps(
                {"wi_fi_client_key": password},
                separators=(",", ":"),
            ),
            json.dumps(
                {"wi_fi_client_channel": channel},
                separators=(",", ":"),
            ),
            json.dumps(
                {"wi_fi_client_dhcp_enabled": True},
                separators=(",", ":"),
            ),
            json.dumps(
                {"wireless_mode": 1},
                separators=(",", ":"),
            ),
            '{"save":null}',
        ]

        print(
            "Writing x-IMU3 Wi-Fi client settings "
            "(the password is intentionally not logged)"
        )

        responses = connection.send_commands(commands)
        _assert_command_responses(commands, responses)

        print("Settings acknowledged and saved; applying Wi-Fi client mode")

        try:
            response = connection.send_command('{"apply":null}')

            if response is not None:
                error = getattr(response, "error", None)
                if error:
                    raise AssertionError(
                        f"x-IMU3 apply command failed: {error}"
                    )
        except Exception as exc:
            # Applying Wi-Fi client mode removes the AP and can tear down the
            # UDP route before the host API sees the acknowledgement. The
            # final same-serial announcement on the Nexus network is the
            # authoritative proof that apply succeeded.
            print(
                "The AP-mode UDP connection ended while applying settings: "
                f"{type(exc).__name__}: {exc}"
            )

        return Ximu3ProvisioningResult(
            serial_number=serial_number,
            ap_ip_address=str(announcement.ip_address),
        )

    finally:
        connection.close()


async def provision_ximu3_udp(
    local_ipv4: IPv4Configuration,
    *,
    ssid: str,
    password: str,
    channel: int,
    timeout: float = 25.0,
) -> Ximu3ProvisioningResult:
    return await asyncio.wait_for(
        asyncio.to_thread(
            _provision_ximu3_udp_sync,
            local_ipv4,
            ssid=ssid,
            password=password,
            channel=channel,
        ),
        timeout=timeout,
    )


def _wait_for_ximu3_on_nexus_network_sync(
    local_ipv4: IPv4Configuration,
    *,
    serial_number: str,
    timeout: float,
) -> Ximu3UdpResult:
    """Wait for the provisioned serial number and verify UDP communication."""

    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            announcement = _find_ximu3_announcement_on_network(
                local_ipv4=local_ipv4,
                serial_number=serial_number,
            )

            print(
                "Received provisioned x-IMU3 announcement: "
                f"name={announcement.device_name!r}, "
                f"serial={announcement.serial_number!r}, "
                f"ip={announcement.ip_address}, "
                f"tcp={announcement.tcp_port}, "
                f"udp_send={announcement.udp_send}, "
                f"udp_receive={announcement.udp_receive}"
            )

            connection = ximu3.Connection(
                announcement.to_udp_connection_config()
            ).open()

            try:
                response = connection.ping()

                if not response:
                    raise AssertionError(
                        "The provisioned x-IMU3 did not respond to UDP ping"
                    )

                if str(response.serial_number) != serial_number:
                    raise AssertionError(
                        "The post-provisioning UDP ping returned the wrong serial"
                    )

                return Ximu3UdpResult(
                    device_name=str(response.device_name),
                    serial_number=str(response.serial_number),
                    interface=str(response.interface),
                    ip_address=str(announcement.ip_address),
                    tcp_port=int(announcement.tcp_port),
                    udp_send_port=int(announcement.udp_send),
                    udp_receive_port=int(announcement.udp_receive),
                )
            finally:
                connection.close()

        except Exception as exc:
            last_error = exc
            time.sleep(1.0)

    raise TimeoutError(
        f"x-IMU3 serial {serial_number!r} did not join and respond on "
        f"{local_ipv4.cidr} within {timeout:.1f} seconds"
    ) from last_error


async def wait_for_ximu3_on_nexus_network(
    local_ipv4: IPv4Configuration,
    *,
    serial_number: str,
    timeout: float,
) -> Ximu3UdpResult:
    return await asyncio.wait_for(
        asyncio.to_thread(
            _wait_for_ximu3_on_nexus_network_sync,
            local_ipv4,
            serial_number=serial_number,
            timeout=timeout,
        ),
        timeout=timeout + 5.0,
    )


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

    async def find_saved_connection(
        self,
        connection_id: str,
    ) -> str | None:
        body = await self.call(
            path=NETWORK_MANAGER_SETTINGS_PATH,
            interface=NETWORK_MANAGER_SETTINGS_INTERFACE,
            member="ListConnections",
        )

        connection_paths = body[0] if body else []

        for connection_path in connection_paths:
            try:
                settings_body = await self.call(
                    path=str(connection_path),
                    interface=SETTINGS_CONNECTION_INTERFACE,
                    member="GetSettings",
                )
            except NetworkManagerDBusError:
                continue

            settings = settings_body[0] if settings_body else {}
            connection_settings = settings.get("connection", {})
            id_variant = connection_settings.get("id")

            if id_variant is not None and id_variant.value == connection_id:
                return str(connection_path)

        return None

    async def wait_for_network_manager(
        self,
        *,
        interface_name: str,
        connection_id: str,
        timeout: float,
    ) -> tuple[str, str]:
        """Wait for NetworkManager to return after a service restart."""

        deadline = asyncio.get_running_loop().time() + timeout
        last_error: Exception | None = None

        while asyncio.get_running_loop().time() < deadline:
            try:
                device_path = await self.get_device_path(interface_name)
                connection_path = await self.find_saved_connection(
                    connection_id
                )

                if connection_path is not None:
                    return device_path, connection_path
            except Exception as exc:
                last_error = exc

            await asyncio.sleep(0.5)

        raise TimeoutError(
            f"NetworkManager did not restore device {interface_name!r} "
            f"and connection {connection_id!r}"
        ) from last_error

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
) -> str:
    """Restore the saved Nexus sensor AP using the proven UDP-test path."""

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

            # Allow mt76x2u and wpa_supplicant to finish changing from
            # station mode back to AP mode. This is the same sequence used
            # by the passing UDP connection test.
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

            active_path = (
                await network_manager.wait_for_connection_id_activated(
                    connection_id=SENSOR_CONNECTION,
                    device_path=device_path,
                    timeout=RESTORE_TIMEOUT_SECONDS,
                )
            )

            print(
                f"Sensor AP connection "
                f"{SENSOR_CONNECTION!r} restored"
            )
            return active_path

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


async def ensure_sensor_ap_active(
    network_manager: NetworkManagerClient,
    *,
    device_path: str,
) -> tuple[str, str]:
    """Return the active and saved paths, activating the AP if needed."""

    connection_path = await network_manager.find_saved_connection(
        SENSOR_CONNECTION
    )

    assert connection_path is not None, (
        f"Precondition failed: saved NetworkManager connection "
        f"{SENSOR_CONNECTION!r} does not exist"
    )

    active_path = await network_manager.find_active_connection(
        SENSOR_CONNECTION
    )

    if active_path is None:
        print(
            f"Precondition: {SENSOR_CONNECTION!r} is saved but inactive; "
            "activating it before the provisioning test"
        )
        active_path = await restore_sensor_ap(
            network_manager,
            connection_path=connection_path,
            device_path=device_path,
        )

    await network_manager.wait_for_connection_id_activated(
        connection_id=SENSOR_CONNECTION,
        device_path=device_path,
        timeout=RESTORE_TIMEOUT_SECONDS,
    )

    return active_path, connection_path



async def test_ap_scan_finds_ximu3_and_restores_sensor_ap() -> None:
    print(f"test_ap_scan version: {TEST_VERSION}")

    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    network_manager = NetworkManagerClient(bus)

    device_path: str | None = None
    connection_path: str | None = None
    ap_was_deactivated = False
    ap_restored = False

    try:
        device_path = await network_manager.get_device_path(
            SENSOR_INTERFACE
        )

        active_path, connection_path = await ensure_sensor_ap_active(
            network_manager,
            device_path=device_path,
        )

        print(
            f"Stopping sensor AP {SENSOR_CONNECTION!r} "
            f"on {SENSOR_INTERFACE!r}"
        )

        await network_manager.deactivate_connection(active_path)
        ap_was_deactivated = True

        await network_manager.wait_for_active_connection_to_disappear(
            active_connection_path=active_path,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )

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

        visible_access_points = sorted(
            access_points,
            key=lambda access_point: access_point.strength,
            reverse=True,
        )

        print(f"NetworkManager returned {len(visible_access_points)} APs")
        for access_point in visible_access_points:
            print(
                "  "
                f"ssid={access_point.ssid!r} "
                f"bssid={access_point.bssid} "
                f"signal={access_point.strength}% "
                f"frequency={access_point.frequency_mhz} MHz"
            )

        ximu3_access_points = [
            access_point
            for access_point in visible_access_points
            if access_point.ssid.casefold().startswith(
                XIMU3_SSID_PREFIX.casefold()
            )
        ]

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

    finally:
        try:
            if (
                ap_was_deactivated
                and not ap_restored
                and connection_path is not None
                and device_path is not None
            ):
                await restore_sensor_ap(
                    network_manager,
                    connection_path=connection_path,
                    device_path=device_path,
                )
                ap_restored = True
        finally:
            bus.disconnect()