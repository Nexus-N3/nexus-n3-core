"""Discover the host serial interface exposed by a Nexus BLE gateway."""

from __future__ import annotations

from pathlib import Path

from serial.tools import list_ports


AUTO_GATEWAY_SERIAL_PORT = "auto"
LINUX_GATEWAY_BY_ID_DIR = Path("/dev/serial/by-id")
LINUX_GATEWAY_INTERFACE_GLOB = "usb-ZEPHYR_IFMCU_CMSIS-DAP_*-if01*"


class GatewaySerialPortError(RuntimeError):
    """Raised when automatic gateway serial-port selection cannot proceed."""


def resolve_gateway_serial_port(configured_port: str | None) -> str:
    """Resolve ``auto`` to the sole attached Nexus BLE gateway interface.

    A non-empty explicit port remains authoritative. Automatic selection first
    uses Linux's stable ``/dev/serial/by-id`` names, then falls back to pyserial
    device metadata for other host platforms.
    """

    configured = str(configured_port or AUTO_GATEWAY_SERIAL_PORT).strip()
    if configured.casefold() != AUTO_GATEWAY_SERIAL_PORT:
        return str(Path(configured).expanduser())

    candidates = discover_gateway_serial_ports()
    if not candidates:
        raise GatewaySerialPortError(
            "No Nexus BLE gateway serial interface was found. Attach a Zephyr "
            "IFMCU CMSIS-DAP gateway or set GATEWAY_SERIAL_PORT explicitly."
        )
    if len(candidates) > 1:
        joined = ", ".join(candidates)
        raise GatewaySerialPortError(
            "Multiple Nexus BLE gateway serial interfaces were found: "
            f"{joined}. Set GATEWAY_SERIAL_PORT to the interface to use."
        )
    return candidates[0]


def discover_gateway_serial_ports(
    by_id_dir: Path = LINUX_GATEWAY_BY_ID_DIR,
) -> list[str]:
    """Return deterministic gateway data interfaces visible to this host."""

    by_id_candidates = _linux_by_id_candidates(by_id_dir)
    if by_id_candidates:
        return by_id_candidates

    candidates = []
    for port in list_ports.comports():
        identity = " ".join(
            str(value or "")
            for value in (
                getattr(port, "description", None),
                getattr(port, "hwid", None),
                getattr(port, "manufacturer", None),
                getattr(port, "product", None),
                getattr(port, "interface", None),
            )
        ).upper()
        if "ZEPHYR" not in identity or "CMSIS-DAP" not in identity:
            continue
        # The composite gateway exposes more than one serial interface. MI_01
        # is the gateway data protocol; MI_03 is not.
        if "MI_03" in identity:
            continue
        if "MI_" in identity and "MI_01" not in identity:
            continue
        device = str(getattr(port, "device", "") or "").strip()
        if device:
            candidates.append(device)
    return sorted(set(candidates))


def _linux_by_id_candidates(by_id_dir: Path) -> list[str]:
    try:
        candidates = [
            str(path)
            for path in by_id_dir.glob(LINUX_GATEWAY_INTERFACE_GLOB)
            if path.exists()
        ]
    except OSError:
        return []
    return sorted(set(candidates))
