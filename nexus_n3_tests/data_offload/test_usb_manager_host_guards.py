from pathlib import Path

from nexus_n3.data_file_offload.sinks.usb import USBDiskManager


def test_usb_manager_disables_hotdisk_on_non_linux_hosts(tmp_path):
    fallback_dir = tmp_path / "outputs"

    manager = USBDiskManager(fallback_dir=fallback_dir, host_platform="Windows")

    try:
        assert manager.supports_hotdisk is False
        assert manager.usb_path is None
        assert manager.network_path is None
        assert manager.local_path == fallback_dir.resolve()
        assert manager.refresh() == fallback_dir.resolve()
        assert manager._thread.is_alive() is False
    finally:
        manager.stop()
