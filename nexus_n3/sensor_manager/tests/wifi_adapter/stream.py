#!/usr/bin/env python3
"""Exercise X-IMU3 discovery, connection, and vendor UDP streaming."""

from __future__ import annotations

import argparse
import asyncio
from queue import Empty, Queue
import sys
import time

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


async def exercise_stream(seconds: float) -> int:
    config = WifiRuntimeConfig.from_env()
    adapter = WifiAdapter(config=config)
    sensor = XImu3Sensor()
    samples: Queue = Queue()
    initialized = False
    connected = False
    streaming = False

    sensor.register_listener("on_data", samples.put)

    try:
        print("Initializing the production Wi-Fi adapter")
        await adapter.initialize()
        initialized = True
        print(f"Nexus AP active: {adapter.network.cidr}")

        print("Discovering or provisioning the X-IMU3")
        devices = await adapter.discover_devices([sensor])
        if not devices:
            raise RuntimeError("No X-IMU3 was discovered after provisioning")

        address = next(iter(devices))
        device, _advertisement = devices[address]
        sensor.address = address
        sensor.location = "DEFAULT"
        sensor.set_transport_client(adapter.create_transport_client(address))

        print(f"Connecting to X-IMU3 {address!r} at {device.endpoint}")
        connected = await adapter.connect_to_device(
            sensor,
            adapter,
            timeout=config.connect_timeout_s,
        )
        if not connected:
            raise RuntimeError(f"X-IMU3 {address!r} did not connect")

        await sensor.setup(adapter)
        rate = int(sensor.attributes["SAMPLING_RATE"])
        print(f"Starting {rate} Hz IMU stream for {seconds:g} seconds")
        await sensor.start_stream(adapter)
        streaming = True

        started = time.monotonic()
        received = 0
        last_timestamp = None
        while time.monotonic() - started < seconds:
            await asyncio.sleep(0.05)
            while True:
                try:
                    sample = samples.get_nowait()
                except Empty:
                    break
                received += 1
                last_timestamp = sample.timestamp
                if received <= 5:
                    print(
                        f"  sample={received} timestamp_us={sample.timestamp} "
                        f"quat={sample.quat} accel_m_s2={sample.accel} "
                        f"gyro_deg_s={sample.gyro}"
                    )

        await sensor.stop_stream(adapter)
        streaming = False
        elapsed = time.monotonic() - started
        observed_rate = received / elapsed if elapsed else 0.0
        print(
            f"Received {received} complete IMU samples in {elapsed:.2f}s "
            f"({observed_rate:.1f} Hz); last timestamp={last_timestamp} us"
        )
        if received == 0:
            raise RuntimeError("The X-IMU3 stream produced no complete IMU samples")
        return 0
    finally:
        if streaming:
            try:
                await sensor.stop_stream(adapter)
            except Exception as exc:
                print(f"Warning: stream cleanup failed: {exc}", file=sys.stderr)
        if connected:
            try:
                await adapter.disconnect_sensor(sensor)
            except Exception as exc:
                print(f"Warning: disconnect cleanup failed: {exc}", file=sys.stderr)
        if initialized:
            await adapter.shutdown()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--seconds",
        type=float,
        default=10.0,
        help="stream duration in seconds (default: 10)",
    )
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("--seconds must be greater than zero")
    return args


def main() -> int:
    args = parse_args()
    return asyncio.run(exercise_stream(args.seconds))


if __name__ == "__main__":
    raise SystemExit(main())
