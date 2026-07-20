"""USB disk detection and path switching for data output."""

import errno
import os
import platform
import time
from pathlib import Path
from threading import Thread, Event

class USBDiskManager:
    """
    Auto-detects a USB disk mounted at /media/<user>/<label>, /mnt/<label>, or /run/media/<user>/<label>.
    Creates nexus_n3_outputs on the USB if writable.
    Fires callbacks only on insert/removal events.
    Does NOT perform any mounting.
    """

    USB_OUTPUT_DIRNAME = "nexus_n3_outputs"

    def __init__(self, fallback_dir="nexus_n3_outputs", poll_interval=2, host_platform: str | None = None):
        """
        Initialize the USB disk manager.

        Args:
            fallback_dir: Local fallback directory when no USB is present.
            poll_interval: Seconds between detection checks.
        """
        self.fallback_dir = Path(fallback_dir).absolute()
        self.poll_interval = poll_interval
        self.host_platform = host_platform or platform.system()
        self.supports_hotdisk = self.host_platform == "Linux"

        self._usb_output_path: Path | None = None
        self._stop_event = Event()
        self._thread = Thread(target=self._monitor_usb, daemon=True)
        self._callbacks = {"inserted": [], "removed": []}

        # Ensure local fallback exists
        self.fallback_dir.mkdir(parents=True, exist_ok=True)

        # Detect USB at startup but don't fire callbacks.
        # On non-Linux hosts the runtime stays on the local fallback path.
        self._initial_usb_detection()

        if self.supports_hotdisk:
            self._thread.start()

    @property
    def local_path(self) -> Path:
        """Return the local output path (USB if present, else fallback)."""
        return self._usb_output_path or self.fallback_dir

    @property
    def usb_path(self) -> Path | None:
        """Return the detected USB output path, if present."""
        return self._usb_output_path
    
    @property
    def network_path(self) -> Path | None:
        """
        Returns the path that workers should use to access USB data (NFS export).
        Does NOT mount anything; just returns the standard export path.
        """
        # Only report the export path if a USB is detected
        if self._usb_output_path:
            return Path("/exports/nexus_n3_data/nexus_n3_outputs")
        return None


    def register_callback(self, event, callback):
        """
        Register a callback for USB insert/remove events.

        Args:
            event: \"inserted\" or \"removed\".
            callback: Callable to invoke on event.
        """
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    def stop(self):
        """Stop the background monitor thread."""
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join()

    def refresh(self) -> Path:
        """
        Refresh the detected USB state immediately and return the active output path.

        This is used by explicit mount/unmount actions so the application does not
        need to wait for the background poller to notice the filesystem change.
        """
        if not self.supports_hotdisk:
            self._usb_output_path = None
            return self.local_path
        mount = self._detect_usb_mount()
        if mount:
            out_dir = mount / self.USB_OUTPUT_DIRNAME
            if self._prepare_output_dir(out_dir):
                self._usb_output_path = out_dir.resolve()
                return self.local_path
        self._usb_output_path = None
        return self.local_path

    # -----------------------
    # Internal methods
    # -----------------------
    def _initial_usb_detection(self):
        """Detect USB at startup and test write, but do not mount or bind."""
        if not self.supports_hotdisk:
            print(
                f"[USB] Hot-disk workflow disabled on host platform {self.host_platform}; "
                f"using local output path {self.fallback_dir}"
            )
            return
        mount = self._detect_usb_mount()
        if mount:
            out_dir = mount / self.USB_OUTPUT_DIRNAME
            if self._prepare_output_dir(out_dir):
                self._usb_output_path = out_dir.resolve()
                print(f"[USB] USB ready at {self._usb_output_path} (startup)")
            else:
                print(f"[USB] USB at {out_dir} not writable at startup")
                self._usb_output_path = None

    def _monitor_usb(self):
        """Monitor USB mount availability and emit callbacks on changes."""
        if not self.supports_hotdisk:
            return
        last_path = self._usb_output_path.parent if self._usb_output_path else None

        while not self._stop_event.is_set():
            usb_path = self._detect_usb_mount()
            if usb_path != last_path:
                if usb_path:
                    # USB inserted
                    out_dir = usb_path / self.USB_OUTPUT_DIRNAME
                    if self._prepare_output_dir(out_dir):
                        self._usb_output_path = out_dir.resolve()
                        print(f"[USB] USB inserted at {self._usb_output_path}")
                        for cb in self._callbacks["inserted"]:
                            cb(self._usb_output_path)
                        last_path = usb_path
                    else:
                        print(f"[USB] USB at {out_dir} not writable")
                        self._usb_output_path = None
                        last_path = None
                else:
                    # USB removed
                    print("[USB] USB removed")
                    self._usb_output_path = None
                    for cb in self._callbacks["removed"]:
                        cb()
                    last_path = None
            time.sleep(self.poll_interval)

    def _detect_usb_mount(self) -> Path | None:
        """Return the USB mountpoint if present and mounted."""
        if not self.supports_hotdisk:
            return None
        hotplug_path = Path("/exports/nexus_n3_data")
        try:
            if (
                hotplug_path.exists()
                and hotplug_path.is_dir()
                and os.path.ismount(hotplug_path)
                and self._path_accessible(hotplug_path)
            ):
                return hotplug_path
        except OSError as exc:
            print(f"[USB] Mount check failed for {hotplug_path}: {exc}")
        return None

    def _prepare_output_dir(self, path: Path) -> bool:
        """Ensure the USB output directory exists and is writable."""
        try:
            if not self._path_accessible(path.parent):
                return False
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            print(f"[USB] Failed to prepare output dir {path}: {exc}")
            return False
        return self._test_write(path)

    def _path_accessible(self, path: Path) -> bool:
        """Return True when the mount path can be traversed without device errors."""
        try:
            path.stat()
            return True
        except OSError as exc:
            if exc.errno == errno.ENODEV:
                print(f"[USB] Ignoring inaccessible mount path {path}: {exc}")
            else:
                print(f"[USB] Mount path check failed for {path}: {exc}")
            return False


    def _test_write(self, path: Path) -> bool:
        """Test if we can write to the USB output directory."""
        try:
            test_file = path / "rs_write_test.txt"
            print(f"[USB] Testing write access on {test_file}")
            with open(test_file, "w") as f:
                f.write("test")
            #test_file.unlink()  # optionally remove test file
            return True
        except Exception as e:
            print(f"[USB] Write test failed at {path}: {e}")
            return False
