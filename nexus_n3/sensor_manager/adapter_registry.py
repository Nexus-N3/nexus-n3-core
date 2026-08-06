"""Registry of available sensor adapters."""

from nexus_n3.sensor_manager.adapters.ble_adapter import BLEAdapter
from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.adapters.usb_camera_adapter import USBCameraAdapter
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.sensor_manager.adapters.wifi_adapter import WiFiAdapter

ADAPTER_REGISTRY = {
    "USB_CAMERA": USBCameraAdapter,
    "WIFI": WiFiAdapter,
}

BLE_BACKEND_REGISTRY = {
    "bleak": BLEAdapter,
    "gateway": GatewayBLEAdapter,
}

# WIFI_BACKEND_REGISTRY = {}

def resolve_adapter_class(
    adapter_type: str,
    ble_runtime_config: BLERuntimeConfig | None = None,
):
    """Resolve an adapter class, allowing the BLE backend to vary at runtime."""
    key = str(adapter_type).upper()
    if key == "BLE":
        config = ble_runtime_config or BLERuntimeConfig.from_env()
        try:
            return BLE_BACKEND_REGISTRY[config.backend]
        except KeyError as exc:
            raise ValueError(f"Unsupported BLE backend: {config.backend}") from exc
    try:
        return ADAPTER_REGISTRY[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported adapter type: {key}") from exc
