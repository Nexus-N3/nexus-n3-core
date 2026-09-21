#!/usr/bin/env python3
"""Exercise the production X-IMU3 Wi-Fi provisioning lifecycle.

The sensor must begin in Wi-Fi AP mode.  This diagnostic uses the public
WifiAdapter API to discover the sensor AP, configure the sensor for the Nexus
network, rediscover it, verify a UDP connection, and disconnect it.
"""

from __future__ import annotations

import asyncio
import sys

from nexus_n3.sensor_manager.adapters.wifi.config import WifiRuntimeConfig
from nexus_n3.sensor_manager.adapters.wifi_adapter import WifiAdapter

try:
    from nexus_n3_sensor_x_imu3.sensor import XImu3Sensor
except ImportError:
    print(
        "The X-IMU3 plugin package is not importable.\n"
        "Run this script with its src directory and the plugin SDK on "
        "PYTHONPATH; see x-imu3.md.",
        file=sys.stderr,
    )
    raise SystemExit(2)


async def exercise_lifecycle() -> int:
    config = WifiRuntimeConfig.from_env()
    adapter = WifiAdapter(config=config)
    sensor = XImu3Sensor()
    initialized = False

    try:
        print("Initializing the production Wi-Fi adapter")
        await adapter.initialize()
        initialized = True
        print(f"Nexus sensor bridge active: {adapter.network.cidr}")

        print("Discovering or provisioning the X-IMU3")
        devices = await adapter.discover_devices([sensor])
        if not devices:
            raise RuntimeError("No X-IMU3 was discovered after provisioning")

        print("Discovered devices:")
        for address, (device, _advertisement) in devices.items():
            print(f"  address={address!r} endpoint={device.endpoint}")

        address = next(iter(devices))
        sensor.address = address
        sensor.set_transport_client(adapter.create_transport_client(address))

        print(f"Connecting to X-IMU3 {address!r} over UDP")
        connected = await adapter.connect_to_device(
            sensor,
            adapter,
            timeout=config.connect_timeout_s,
        )
        if not connected:
            raise RuntimeError(f"X-IMU3 {address!r} did not connect")
        print(f"Connected: {address}")

        disconnected = await adapter.disconnect_sensor(sensor)
        if not disconnected:
            raise RuntimeError(f"X-IMU3 {address!r} did not disconnect cleanly")
        print(f"Disconnected: {address}")
        return 0
    finally:
        if initialized:
            await adapter.shutdown()


def main() -> int:
    return asyncio.run(exercise_lifecycle())


if __name__ == "__main__":
    raise SystemExit(main())
