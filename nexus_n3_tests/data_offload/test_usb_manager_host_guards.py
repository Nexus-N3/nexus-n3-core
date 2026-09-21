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


def test_usb_path_invalidates_cached_path_after_mount_disappears(tmp_path, monkeypatch):
    manager = USBDiskManager(
        fallback_dir=tmp_path / "fallback",
        host_platform="Windows",
    )
    cached_output = tmp_path / "usb" / manager.USB_OUTPUT_DIRNAME
    cached_output.mkdir(parents=True)

    try:
        # Use a non-monitoring manager while exercising Linux mount semantics.
        manager.supports_hotdisk = True
        manager._usb_output_path = cached_output.resolve()
        monkeypatch.setattr(manager, "_detect_usb_mount", lambda: None)

        assert manager.usb_path is None
        assert manager.local_path == manager.fallback_dir
        assert manager.network_path is None
        assert manager._usb_output_path is None
    finally:
        manager.stop()
