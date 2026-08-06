from __future__ import annotations

from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nexus_n3.sensor_manager.adapter_registry import (
    ADAPTER_REGISTRY,
    resolve_adapter_class,
)
from nexus_n3.sensor_manager.adapters.ble_adapter import BLEAdapter
from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.adapters.usb_camera_adapter import USBCameraAdapter
from nexus_n3.sensor_manager.adapters.wifi_adapter import WiFiAdapter, WifiAdapter
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


@pytest.mark.parametrize("adapter_type", ["WIFI", "wifi", "WiFi"])
def test_wifi_resolution_is_case_insensitive(adapter_type):
    assert resolve_adapter_class(adapter_type) is WifiAdapter


def test_registry_uses_canonical_wifi_class_and_preserves_alias():
    assert ADAPTER_REGISTRY["WIFI"] is WifiAdapter
    assert WiFiAdapter is WifiAdapter


def test_resolving_wifi_does_not_construct_adapter(monkeypatch):
    def fail_if_constructed(*args, **kwargs):
        raise AssertionError("adapter resolution must not initialize a backend")

    monkeypatch.setattr(WifiAdapter, "__init__", fail_if_constructed)

    assert resolve_adapter_class("WIFI") is WifiAdapter


def test_existing_adapter_resolution_is_unchanged():
    assert resolve_adapter_class("USB_CAMERA") is USBCameraAdapter
    assert resolve_adapter_class(
        "BLE", BLERuntimeConfig(backend="bleak")
    ) is BLEAdapter
    assert resolve_adapter_class(
        "ble", BLERuntimeConfig(backend="gateway")
    ) is GatewayBLEAdapter


def test_unknown_adapter_type_is_rejected():
    with pytest.raises(ValueError, match="Unsupported adapter type: UNKNOWN"):
        resolve_adapter_class("unknown")
