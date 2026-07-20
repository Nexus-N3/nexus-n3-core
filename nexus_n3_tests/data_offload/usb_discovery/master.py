from zeroconf import Zeroconf, ServiceInfo
import socket
import zmq
import time
import os
from pathlib import Path
from threading import Thread, Event

from nexus_n3_tests.distributed.auto_discovery.shared_utils import get_local_ip
from nexus_n3_tests.distributed.auto_discovery.node_registry import NodeRegistry


# =====================================================
# USB Disk Manager (MASTER ONLY)
# =====================================================
class USBDiskManager:
    """
    Detects a mounted USB drive under /media/<user>/<label> or /mnt/<label>.
    Creates nexus_n3_outputs on the USB if writable.
    Falls back to local storage otherwise.
    """

    def __init__(self, fallback_dir="nexus_n3_outputs", poll_interval=2):
        self.fallback_dir = Path(fallback_dir).absolute()
        self.poll_interval = poll_interval

        self._usb_output_path: Path | None = None
        self._stop_event = Event()
        self._thread = Thread(target=self._monitor_usb, daemon=True)

        self._callbacks = {"inserted": [], "removed": []}

        self.fallback_dir.mkdir(parents=True, exist_ok=True)
        self._thread.start()

    @property
    def local_path(self) -> Path:
        """Where the master actually writes data."""
        return self._usb_output_path or self.fallback_dir

    @property
    def network_path(self) -> Path | None:
        """
        Network-exported path for workers (NFS).
        Only valid if USB is present.
        """
        if self._usb_output_path:
            return Path("/exports/nexus_n3_outputs")
        return None

    def register_callback(self, event, callback):
        """Register callbacks for 'inserted' or 'removed' events."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    def stop(self):
        self._stop_event.set()
        self._thread.join()

    # -------------------------------------------------

    def _monitor_usb(self):
        last_state: Path | None = None
        while not self._stop_event.is_set():
            usb_path = self._detect_usb_mount()
            if usb_path != last_state:
                # USB inserted
                if usb_path and os.access(usb_path, os.W_OK):
                    out = usb_path / "nexus_n3_outputs"
                    try:
                        out.mkdir(exist_ok=True)
                        self._usb_output_path = out.resolve()
                        print(f"[USB] Using USB disk at {self._usb_output_path}")
                    except PermissionError:
                        print("[USB] Permission denied, falling back to local storage")
                        self._usb_output_path = None
                        usb_path = None
                else:
                    self._usb_output_path = None

                # Callbacks
                if usb_path and self._callbacks["inserted"]:
                    for cb in self._callbacks["inserted"]:
                        cb(self._usb_output_path)
                elif not usb_path and self._callbacks["removed"]:
                    for cb in self._callbacks["removed"]:
                        cb()

                last_state = usb_path

            time.sleep(self.poll_interval)


    def _detect_usb_mount(self) -> Path | None:
        """
        Detect a writable USB mount.
        Expected layouts:
          /media/<user>/<label>
          /mnt/<label>
        """
        candidates = []

        media = Path("/media")
        if media.exists():
            for user_dir in media.iterdir():
                if user_dir.is_dir():
                    candidates.extend(p for p in user_dir.iterdir() if p.is_dir())

        mnt = Path("/mnt")
        if mnt.exists():
            candidates.extend(p for p in mnt.iterdir() if p.is_dir())

        for path in candidates:
            if os.path.ismount(path) and os.access(path, os.W_OK):
                return path.resolve()

        return None


# =====================================================
# mDNS Advertisement
# =====================================================
def advertise_master(port: int, network_usb_path: Path | None):
    ip = socket.inet_aton(get_local_ip())

    properties = {}
    if network_usb_path:
        properties["usb_path"] = str(network_usb_path)

    info = ServiceInfo(
        "_nexusn3._tcp.local.",
        "nexus-n3-master._nexusn3._tcp.local.",
        addresses=[ip],
        port=port,
        properties=properties,
    )

    zc = Zeroconf()
    zc.register_service(info)
    return zc


# =====================================================
# Main
# =====================================================
PORT = 5555

print("[MASTER] Starting...")
print(f"[MASTER] Local IP: {get_local_ip()}")

usb_manager = USBDiskManager()

# ZeroMQ context
ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.bind(f"tcp://*:{PORT}")
rep.setsockopt(zmq.RCVTIMEO, 1000)

registry = NodeRegistry()
print("[MASTER] Waiting for workers...")

# ---------------- USB Callbacks ----------------

def notify_workers_usb_inserted(path):
    """Notify all registered workers that USB is now available."""
    print(f"[MASTER] USB inserted at {path}, notifying {len(registry.get_nodes())} workers")
    for node_id, ip in registry.get_nodes().items():
        try:
            sock = ctx.socket(zmq.REQ)
            sock.connect(f"tcp://{ip}:5556")  # assuming worker has a listener on 5556
            sock.send_json({"type": "USB_INSERTED", "path": str(path)})
            sock.recv_json()
            sock.close()
        except Exception as e:
            print(f"[MASTER] Failed to notify {node_id}: {e}")

def notify_workers_usb_removed():
    """Notify all registered workers that USB is now gone."""
    print(f"[MASTER] USB removed, notifying {len(registry.get_nodes())} workers")
    for node_id, ip in registry.get_nodes().items():
        try:
            sock = ctx.socket(zmq.REQ)
            sock.connect(f"tcp://{ip}:5556")
            sock.send_json({"type": "USB_REMOVED"})
            sock.recv_json()
            sock.close()
        except Exception as e:
            print(f"[MASTER] Failed to notify {node_id}: {e}")

# Register USB callbacks
usb_manager.register_callback("inserted", notify_workers_usb_inserted)
usb_manager.register_callback("removed", notify_workers_usb_removed)

# Allow initial detection
time.sleep(2)
print(f"[MASTER] Local write path: {usb_manager.local_path}")
if usb_manager.network_path:
    print(f"[MASTER] Exported network path: {usb_manager.network_path}")
else:
    print("[MASTER] No USB detected — workers will use local storage")

# Advertise master via mDNS
zeroconf = advertise_master(PORT, usb_manager.network_path)
print("[MASTER] mDNS advertised")

# ---------------- Main Loop ----------------
try:
    while True:
        try:
            msg = rep.recv_json()
            if msg.get("type") == "REGISTER":
                registry.register(msg["node_id"], msg["ip"])
                print(f"[MASTER] Registered {msg['node_id']} @ {msg['ip']}")

                rep.send_json({
                    "status": "OK",
                    "usb_path": str(usb_manager.network_path) if usb_manager.network_path else None
                })
        except zmq.Again:
            pass

except KeyboardInterrupt:
    print("[MASTER] Shutting down...")
finally:
    usb_manager.stop()
    zeroconf.close()
    ctx.term()

