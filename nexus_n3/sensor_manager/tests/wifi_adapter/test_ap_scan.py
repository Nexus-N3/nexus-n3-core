"""Live test for discovering an x-IMU3 access point.

This test deliberately takes down the Nexus sensor AP, scans for an x-IMU3
operating in AP mode, and restores the Nexus sensor AP before exiting.

Run this test only from a local terminal, Ethernet connection, or another
management interface. Do not run it through the sensor AP being stopped.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from typing import Any

#!-- imports
# python -m pip install pytest pytest-asyncio dbus-fast

import pytest
from dbus_fast import Message
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


SENSOR_INTERFACE = os.getenv(
    "NEXUS_SENSOR_INTERFACE",
    "wlx00c0cabaa751",
)

SENSOR_CONNECTION = os.getenv(
    "NEXUS_SENSOR_CONNECTION",
    "nexus-n3-sensor-ap",
)

XIMU3_SSID_PREFIX = os.getenv(
    "XIMU3_SSID_PREFIX",
    "x-IMU3",
)

SCAN_TIMEOUT_SECONDS = float(
    os.getenv("XIMU3_SCAN_TIMEOUT_SECONDS", "20")
)

DEVICE_STATE_DISCONNECTED = 30
DEVICE_STATE_ACTIVATED = 100

ACTIVE_CONNECTION_STATE_ACTIVATED = 2


pytestmark = [
    pytest.mark.asyncio,
]

class NetworkManagerDBusError(RuntimeError):
    """Raised when NetworkManager returns a D-Bus error."""


@dataclass(frozen=True)
class WifiAccessPoint:
    """Structured access-point information read from NetworkManager."""

    object_path: str
    ssid: str
    bssid: str
    strength: int
    frequency_mhz: int

    @property
    def channel_description(self) -> str:
        if self.frequency_mhz:
            return f"{self.frequency_mhz} MHz"
        return "unknown frequency"


class NetworkManagerClient:
    """Minimal asynchronous NetworkManager D-Bus client for this test."""

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
                f"NetworkManager did not return a device for {interface_name}"
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
            active_id = await self.get_property(
                path=active_path,
                interface=ACTIVE_CONNECTION_INTERFACE,
                property_name="Id",
            )

            if active_id == connection_id:
                return active_path

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

    async def activate_connection(
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
                # An AP may disappear between GetAllAccessPoints and reading
                # its properties. Ignore that individual result.
                continue

            ssid = bytes(raw_ssid).decode("utf-8", errors="replace")

            access_points.append(
                WifiAccessPoint(
                    object_path=str(access_point_path),
                    ssid=ssid,
                    bssid=str(bssid),
                    strength=int(strength),
                    frequency_mhz=int(frequency),
                )
            )

        return access_points

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
            last_state = int(
                await self.get_property(
                    path=active_connection_path,
                    interface=ACTIVE_CONNECTION_INTERFACE,
                    property_name="State",
                )
            )

            if last_state == ACTIVE_CONNECTION_STATE_ACTIVATED:
                return

            await asyncio.sleep(0.25)

        raise TimeoutError(
            "Connection did not become active; "
            f"last active-connection state was {last_state}"
        )

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
        current_scan = previous_scan

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
            "NetworkManager did not complete a fresh Wi-Fi scan within "
            f"{timeout:.1f} seconds; LastScan remained {current_scan}"
        )


async def restore_sensor_ap(
    network_manager: NetworkManagerClient,
    *,
    connection_path: str,
    device_path: str,
) -> None:
    """Restore the saved Nexus sensor AP connection."""

    existing_active_path = await network_manager.find_active_connection(
        SENSOR_CONNECTION
    )

    if existing_active_path is not None:
        await network_manager.wait_for_active_connection(
            active_connection_path=existing_active_path,
            timeout=20,
        )
        return

    print(f"Restoring sensor AP connection {SENSOR_CONNECTION!r}")

    restored_active_path = await network_manager.activate_connection(
        connection_path=connection_path,
        device_path=device_path,
    )

    await network_manager.wait_for_active_connection(
        active_connection_path=restored_active_path,
        timeout=20,
    )

    await network_manager.wait_for_device_state(
        device_path=device_path,
        expected_state=DEVICE_STATE_ACTIVATED,
        timeout=20,
    )

    print(f"Sensor AP connection {SENSOR_CONNECTION!r} restored")


async def test_ap_scan_finds_ximu3_and_restores_sensor_ap() -> None:
    """Stop the Nexus AP, scan for x-IMU3, and restore the Nexus AP."""

    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    network_manager = NetworkManagerClient(bus)

    device_path: str | None = None
    saved_connection_path: str | None = None
    sensor_ap_was_deactivated = False

    try:
        device_path = await network_manager.get_device_path(
            SENSOR_INTERFACE
        )

        active_connection_path = (
            await network_manager.find_active_connection(
                SENSOR_CONNECTION
            )
        )

        assert active_connection_path is not None, (
            f"Precondition failed: {SENSOR_CONNECTION!r} is not active"
        )

        active_devices = await network_manager.get_property(
            path=active_connection_path,
            interface=ACTIVE_CONNECTION_INTERFACE,
            property_name="Devices",
        )

        assert device_path in active_devices, (
            f"{SENSOR_CONNECTION!r} is active, but not on "
            f"{SENSOR_INTERFACE!r}"
        )

        saved_connection_path = str(
            await network_manager.get_property(
                path=active_connection_path,
                interface=ACTIVE_CONNECTION_INTERFACE,
                property_name="Connection",
            )
        )

        print(
            f"Stopping sensor AP {SENSOR_CONNECTION!r} "
            f"on {SENSOR_INTERFACE!r}"
        )

        await network_manager.deactivate_connection(
            active_connection_path
        )
        sensor_ap_was_deactivated = True

        await network_manager.wait_for_device_state(
            device_path=device_path,
            expected_state=DEVICE_STATE_DISCONNECTED,
            timeout=20,
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
                f"frequency={access_point.channel_description}"
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
                sensor_ap_was_deactivated
                and saved_connection_path is not None
                and device_path is not None
            ):
                await restore_sensor_ap(
                    network_manager,
                    connection_path=saved_connection_path,
                    device_path=device_path,
                )
        finally:
            bus.disconnect()