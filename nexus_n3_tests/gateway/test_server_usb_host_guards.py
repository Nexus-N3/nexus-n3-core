import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from nexus_n3.data_file_offload.sinks.usb import USBDiskManager
from nexus_n3.gateway.server import Server


class FakeGateway:
    site = "test-site"

    def publish_event(self, event):
        self.last_event = event

    def start(self, _on_message):
        return None

    def stop(self):
        return None


def test_server_does_not_expose_linux_usb_controls_on_non_linux_hosts(tmp_path):
    manager = USBDiskManager(fallback_dir=tmp_path / "outputs", host_platform="Windows")
    server = Server(FakeGateway(), usb_disk_manager=manager)

    try:
        assert server._usb_hotdisk_enabled is False
        assert server.handler._usb_mount_handler is None
        assert server.handler._usb_unmount_handler is None
        assert server.handler._usb_status_provider is None
        assert server.handler._before_stream_start is None
        assert server.handler._after_stream_stop is None
        assert server.mount_usb_disk() is False
        assert server.safe_unmount_usb_disk() is False
    finally:
        manager.stop()
