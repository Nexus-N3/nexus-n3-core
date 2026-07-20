"""USB camera adapter using V4L2 device discovery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

from nexus_n3.logger.logger import get_module_logger
from nexus_n3.sensor_manager.types.connections import ConnectionStatus

logger = get_module_logger("USB Camera Adapter")


@dataclass(frozen=True)
class CameraAdvertisement:
    """Minimal advertisement data compatible with match_devices()."""
    local_name: str
    by_id: str | None = None
    by_path: str | None = None


@dataclass(frozen=True)
class USBCameraDevice:
    """Lightweight descriptor for a V4L2 camera device."""
    address: str
    name: str
    by_id: str | None = None
    by_path: str | None = None


class USBCameraClient:
    """Placeholder transport client for USB camera devices."""
    def __init__(self, device_path: str):
        self.device_path = device_path
        self._handle = None

    def open(self):
        if self._handle is None:
            self._handle = open(self.device_path, "rb", buffering=0)

    def close(self):
        if self._handle:
            self._handle.close()
            self._handle = None


class USBCameraAdapter:
    """
    Adapter for USB cameras discovered via V4L2 sysfs entries.

    Discovery returns devices keyed by address with a minimal advertisement
    payload that provides a local_name for matching.
    """
    adapter_type = "USB_CAMERA"

    @staticmethod
    async def connect(client: USBCameraClient) -> bool:
        """Open the camera device path to confirm availability."""
        try:
            client.open()
            return True
        except Exception as exc:
            logger.error("USB camera connect failed: %s", exc)
            return False

    @staticmethod
    async def disconnect(client: USBCameraClient) -> bool:
        """Close the camera device path."""
        try:
            client.close()
            return True
        except Exception as exc:
            logger.error("USB camera disconnect failed: %s", exc)
            return False

    @staticmethod
    async def connect_all(devices, adapter) -> bool:
        """Connect all USB camera devices by opening their device paths."""
        ok = True
        for device in devices:
            connected = await adapter.connect(device.transport_client)
            if connected:
                device.set_connection_status(ConnectionStatus.CONNECTED)
            ok = ok and connected
        return ok

    @staticmethod
    def _sysfs_name(video_node: Path) -> str | None:
        name_path = Path("/sys/class/video4linux") / video_node.name / "name"
        if not name_path.exists():
            return None
        try:
            return name_path.read_text(encoding="utf-8").strip()
        except Exception:
            return None

    @staticmethod
    def _by_id_map() -> Dict[str, str]:
        by_id_dir = Path("/dev/v4l/by-id")
        if not by_id_dir.exists():
            return {}
        mapping: Dict[str, str] = {}
        for entry in by_id_dir.iterdir():
            try:
                target = entry.resolve()
            except Exception:
                continue
            mapping[str(target)] = entry.name
        return mapping

    @staticmethod
    def _by_path_map() -> Dict[str, str]:
        by_path_dir = Path("/dev/v4l/by-path")
        if not by_path_dir.exists():
            return {}
        mapping: Dict[str, str] = {}
        for entry in by_path_dir.iterdir():
            try:
                target = entry.resolve()
            except Exception:
                continue
            mapping[str(target)] = entry.name
        return mapping

    @staticmethod
    async def discover_devices(names: list[str]) -> Dict[str, Tuple[USBCameraDevice, CameraAdvertisement]]:
        """
        Discover V4L2 cameras and return devices with advertisement data.

        Matches devices by name or by-id value when provided in `names`.
        """
        devices: Dict[str, Tuple[USBCameraDevice, CameraAdvertisement]] = {}
        by_id = USBCameraAdapter._by_id_map()
        by_path = USBCameraAdapter._by_path_map()
        matched_name_assigned = False
        for node in sorted(Path("/dev").glob("video*")):
            device_name = USBCameraAdapter._sysfs_name(node) or node.name
            node_path = str(node)
            device_by_id = by_id.get(node_path)
            device_by_path = by_path.get(node_path)

            if device_by_id and device_by_id in names:
                local_name = device_by_id
            elif device_name in names:
                if len(names) == 1 and matched_name_assigned:
                    local_name = f"{device_name} ({node.name})"
                else:
                    local_name = device_name
                    matched_name_assigned = True
            elif len(names) == 1 and not matched_name_assigned:
                local_name = names[0]
                matched_name_assigned = True
            else:
                local_name = device_name

            device = USBCameraDevice(
                address=node_path,
                name=device_name,
                by_id=device_by_id,
                by_path=device_by_path,
            )
            adv = CameraAdvertisement(
                local_name=local_name,
                by_id=device_by_id,
                by_path=device_by_path,
            )
            devices[node_path] = (device, adv)

        logger.info("Discovered %d USB camera device(s)", len(devices))
        return devices

    @staticmethod
    def create_transport_client(address: str, loop=None) -> USBCameraClient:
        """Create a transport client for a camera device address."""
        return USBCameraClient(address)
