from zeroconf import Zeroconf, ServiceBrowser
import socket
import time
import zmq
import os
import subprocess
import weakref
from pathlib import Path
import argparse

from nexus_n3_tests.distributed.auto_discovery.shared_utils import get_local_ip


# =====================================================
# mDNS Discovery
# =====================================================
class MasterListener:
    def __init__(self):
        self.master_ip = None
        self.master_port = None

    def add_service(self, zc, type_, name):
        info = zc.get_service_info(type_, name)
        if info:
            self.master_ip = socket.inet_ntoa(info.addresses[0])
            self.master_port = info.port

    def update_service(self, zc, type_, name):
        pass  # required for future zeroconf versions

    def remove_service(self, zc, type_, name):
        pass


def discover_master(timeout=5):
    print("[WORKER] Discovering master via mDNS...")
    zc = Zeroconf()
    listener = MasterListener()
    ServiceBrowser(zc, "_nexusn3._tcp.local.", listener)

    t0 = time.time()
    while time.time() - t0 < timeout:
        if listener.master_ip:
            return listener.master_ip, listener.master_port
        time.sleep(0.1)

    raise RuntimeError("Master not found via mDNS")


# =====================================================
# NFS Mount Helper
# =====================================================
def mount_network_path(master_ip: str, export_path: str, node_id: str) -> Path:
    mount_root = Path.home() / "nexusn3_mounts"
    mount_point = mount_root / f"usb_{node_id}"

    mount_root.mkdir(parents=True, exist_ok=True)
    mount_point.mkdir(parents=True, exist_ok=True)

    if os.path.ismount(mount_point):
        print(f"[WORKER {node_id}] Already mounted at {mount_point}")
        return mount_point

    nfs_target = f"{master_ip}:{export_path}"
    print(f"[WORKER {node_id}] Mounting NFS {nfs_target} -> {mount_point}")

    try:
        subprocess.run(
            ["sudo", "mount", "-t", "nfs", nfs_target, str(mount_point)],
            check=True,
        )
        print(f"[WORKER {node_id}] Mount successful")
        return mount_point

    except subprocess.CalledProcessError as e:
        print(f"[WORKER {node_id}] Mount failed: {e}")
        fallback = Path(f"/tmp/nexus_n3_outputs_{node_id}")
        fallback.mkdir(parents=True, exist_ok=True)
        print(f"[WORKER {node_id}] Falling back to {fallback}")
        return fallback


# =====================================================
# Worker Startup
# =====================================================
parser = argparse.ArgumentParser()
parser.add_argument("--node_id", required=True)
args = parser.parse_args()

NODE_ID = args.node_id

print(f"[WORKER {NODE_ID}] Starting...")
print(f"[WORKER {NODE_ID}] Local IP: {get_local_ip()}")

# Discover master
master_ip, master_port = discover_master()
print(f"[WORKER {NODE_ID}] Found master at {master_ip}:{master_port}")

# ZeroMQ registration
ctx = zmq.Context()
req = ctx.socket(zmq.REQ)
req.connect(f"tcp://{master_ip}:{master_port}")

req.send_json({
    "type": "REGISTER",
    "node_id": NODE_ID,
    "ip": get_local_ip(),
})

reply = req.recv_json()
print(f"[WORKER {NODE_ID}] Reply: {reply}")

# =====================================================
# Resolve storage path
# =====================================================
export_path = reply.get("usb_path")

if export_path:
    print(f"[WORKER {NODE_ID}] Master exported USB path: {export_path}")
    usb_path = mount_network_path(master_ip, export_path, NODE_ID)
else:
    print(f"[WORKER {NODE_ID}] No USB export — using local storage")
    usb_path = Path(f"/tmp/nexus_n3_outputs_{NODE_ID}")
    usb_path.mkdir(parents=True, exist_ok=True)

print(f"[WORKER {NODE_ID}] Using write path: {usb_path}")
