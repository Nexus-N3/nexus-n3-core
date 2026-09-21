#!/usr/bin/env python3
"""Discover and classify X-IMU3 provisioning access points.

This diagnostic exercises the production NetworkManager backend and the
X-IMU3 plugin classifier without changing any sensor settings.  The saved
Nexus sensor AP is restored before the program exits.
"""

from __future__ import annotations

import asyncio
import sys

from nexus_n3.sensor_manager.adapters.wifi.backends.linux_networkmanager import (
    LinuxNetworkManagerBackend,
)
from nexus_n3.sensor_manager.adapters.wifi.config import WifiRuntimeConfig

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


async def discover() -> int:
    config = WifiRuntimeConfig.from_env()
    backend = LinuxNetworkManagerBackend(config)
    sensor = XImu3Sensor()
    restore_required = False

    await backend.initialize()
    network = await backend.ensure_ap_active()
    print(f"Nexus sensor bridge active: {network.cidr}")

    try:
        # From this point onward, attempt restoration even if scanning or
        # plugin classification raises an exception.
        restore_required = True
        access_points = await backend.begin_provisioning()

        print("Visible APs:")
        for access_point in access_points:
            print(
                f"  ssid={access_point.ssid!r} "
                f"strength={access_point.strength} "
                f"frequency={access_point.frequency_mhz} "
                f"secured={access_point.secured}"
            )

        candidates = await sensor.classify_access_points(access_points)
        print("X-IMU3 candidates:")
        for candidate in candidates:
            access_point = candidate["access_point"]
            print(
                f"  ssid={access_point['ssid']!r} "
                f"confidence={candidate['confidence']}"
            )

        if not candidates:
            raise RuntimeError("No X-IMU3 provisioning access point was found")
        return 0
    finally:
        if restore_required:
            restored = await backend.restore_ap()
            print(f"Nexus AP restored to bridge: {restored.cidr}")
        await backend.shutdown()


def main() -> int:
    return asyncio.run(discover())


if __name__ == "__main__":
    raise SystemExit(main())
