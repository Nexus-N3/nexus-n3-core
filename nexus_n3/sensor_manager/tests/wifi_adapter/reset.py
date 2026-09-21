#!/usr/bin/env python3
"""Wait for an x-IMU3 on the Nexus network, then switch it to Wi-Fi AP mode."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

try:
    import ximu3
except ImportError:
    print(
        "The ximu3 package is required.\n"
        "Install it with:\n"
        "  python -m pip install ximu3",
        file=sys.stderr,
    )
    raise SystemExit(2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Wait for a specific x-IMU3 network announcement, verify it with "
            "UDP ping, and switch the device back to Wi-Fi AP mode."
        )
    )
    parser.add_argument(
        "--serial",
        default="6A33CA84",
        help="Target x-IMU3 serial number (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="Maximum seconds to wait for the device (default: %(default)s)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Delay between announcement attempts (default: %(default)s)",
    )
    return parser.parse_args()


def get_attr(value: Any, name: str, default: Any = None) -> Any:
    return getattr(value, name, default)


def check_responses(responses: Any, *, operation: str) -> None:
    if responses is None:
        raise RuntimeError(f"{operation}: no responses returned")

    for response in responses:
        if response is None:
            raise RuntimeError(f"{operation}: received an empty response")

        error = get_attr(response, "error")
        if error:
            raise RuntimeError(f"{operation}: {error}")

        key = get_attr(response, "key", "<unknown>")
        value = get_attr(response, "value", None)
        print(f"{operation}: {key}={value!r}")


def find_announcement(serial: str) -> Any | None:
    announcements = (
        ximu3.NetworkAnnouncement()
        .get_messages_after_short_delay()
    )

    for announcement in announcements:
        if str(get_attr(announcement, "serial_number", "")) == serial:
            return announcement

    return None


def main() -> int:
    args = parse_args()
    deadline = time.monotonic() + args.timeout
    attempt = 0

    print(
        f"Waiting up to {args.timeout:g} seconds for x-IMU3 "
        f"serial {args.serial!r}"
    )
    print(
        "The script may be started while the sensor is still flashing cyan."
    )

    announcement = None

    while time.monotonic() < deadline:
        attempt += 1

        try:
            announcement = find_announcement(args.serial)
        except Exception as exc:
            print(f"Announcement attempt {attempt} failed: {exc}")
            announcement = None

        if announcement is not None:
            break

        remaining = max(0.0, deadline - time.monotonic())
        print(
            f"Device not announced yet; "
            f"{remaining:.0f} seconds remaining"
        )
        time.sleep(args.poll_interval)

    if announcement is None:
        print(
            f"Timed out waiting for x-IMU3 serial {args.serial!r}. "
            "Confirm the Nexus AP is active and the sensor can join it.",
            file=sys.stderr,
        )
        return 1

    device_name = str(get_attr(announcement, "device_name", "x-IMU3"))
    ip_address = str(get_attr(announcement, "ip_address", "<unknown>"))

    print(
        f"Found {device_name!r}, serial={args.serial!r}, "
        f"ip={ip_address}"
    )

    connection = ximu3.Connection(
        announcement.to_udp_connection_config()
    ).open()

    try:
        ping = connection.ping()
        if ping is None:
            raise RuntimeError("UDP ping returned no response")

        print(
            "UDP ping succeeded: "
            f"interface={get_attr(ping, 'interface', '<unknown>')!r}, "
            f"name={get_attr(ping, 'device_name', '<unknown>')!r}, "
            f"serial={get_attr(ping, 'serial_number', '<unknown>')!r}"
        )

        check_responses(
            connection.send_commands(
                [
                    '{"wireless_mode":2}',
                    '{"save":null}',
                ]
            ),
            operation="configure",
        )

        print(
            "Applying Wi-Fi AP mode. Loss of the current UDP connection "
            "is expected."
        )

        try:
            responses = connection.send_commands(
                ['{"apply":null}']
            )
            if responses is not None:
                for response in responses:
                    if response is None:
                        continue
                    error = get_attr(response, "error")
                    if error:
                        print(f"apply response: {error}")
                    else:
                        print(
                            "apply response: "
                            f"{get_attr(response, 'key', '<unknown>')}="
                            f"{get_attr(response, 'value', None)!r}"
                        )
        except Exception as exc:
            print(
                "UDP connection ended while applying AP mode, "
                f"as expected: {exc}"
            )

    finally:
        connection.close()

    print("AP-mode command sent successfully.")
    print("Expected LED transition: cyan -> flashing magenta -> solid magenta.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())