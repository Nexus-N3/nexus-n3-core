from pathlib import Path
from types import SimpleNamespace

import pytest

from nexus_n3.sensor_manager import gateway_serial


def test_linux_discovery_selects_gateway_data_interface(tmp_path: Path):
    data_interface = (
        tmp_path / "usb-ZEPHYR_IFMCU_CMSIS-DAP_820D9A5F0C92248E796CF-if01"
    )
    other_interface = (
        tmp_path / "usb-ZEPHYR_IFMCU_CMSIS-DAP_820D9A5F0C92248E796CF-if03"
    )
    data_interface.touch()
    other_interface.touch()

    assert gateway_serial.discover_gateway_serial_ports(tmp_path) == [
        str(data_interface)
    ]


def test_auto_resolves_the_only_attached_gateway(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        gateway_serial,
        "discover_gateway_serial_ports",
        lambda: ["/dev/serial/by-id/current-gateway-if01"],
    )

    assert (
        gateway_serial.resolve_gateway_serial_port("auto")
        == "/dev/serial/by-id/current-gateway-if01"
    )
    assert (
        gateway_serial.resolve_gateway_serial_port(None)
        == "/dev/serial/by-id/current-gateway-if01"
    )


def test_auto_rejects_ambiguous_gateways(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        gateway_serial,
        "discover_gateway_serial_ports",
        lambda: ["/dev/gateway-a", "/dev/gateway-b"],
    )

    with pytest.raises(gateway_serial.GatewaySerialPortError, match="Multiple"):
        gateway_serial.resolve_gateway_serial_port("auto")


def test_auto_reports_when_no_gateway_is_attached(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        gateway_serial, "discover_gateway_serial_ports", lambda: []
    )

    with pytest.raises(gateway_serial.GatewaySerialPortError, match="No Nexus"):
        gateway_serial.resolve_gateway_serial_port("auto")


def test_explicit_port_remains_authoritative(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        gateway_serial,
        "discover_gateway_serial_ports",
        lambda: pytest.fail("explicit ports must not trigger discovery"),
    )

    assert gateway_serial.resolve_gateway_serial_port("/dev/ttyACM9") == "/dev/ttyACM9"


def test_pyserial_fallback_uses_composite_interface_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        gateway_serial.list_ports,
        "comports",
        lambda: [
            SimpleNamespace(
                device="COM7",
                description="ZEPHYR IFMCU CMSIS-DAP",
                hwid="USB VID:PID=2FE3:0100 MI_01",
                manufacturer=None,
                product=None,
                interface=None,
            ),
            SimpleNamespace(
                device="COM8",
                description="ZEPHYR IFMCU CMSIS-DAP",
                hwid="USB VID:PID=2FE3:0100 MI_03",
                manufacturer=None,
                product=None,
                interface=None,
            ),
        ],
    )

    assert gateway_serial.discover_gateway_serial_ports(tmp_path / "missing") == [
        "COM7"
    ]
