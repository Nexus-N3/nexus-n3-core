# mdns_master.py
from zeroconf import Zeroconf, ServiceInfo
import socket
import zmq
import time
from nexus_n3_tests.distributed.auto_discovery.shared_utils import get_local_ip
from nexus_n3_tests.distributed.auto_discovery.node_registry import NodeRegistry

def advertise_master(port):
    ip = socket.inet_aton(get_local_ip())

    info = ServiceInfo(
        "_nexusn3._tcp.local.",
        "nexus-n3-master._nexusn3._tcp.local.",
        addresses=[ip],
        port=port,
    )

    zc = Zeroconf()
    zc.register_service(info)
    return zc


PORT = 5555

print("[MASTER] Starting...")
print(f"[MASTER] Local IP: {get_local_ip()}")

# Advertise via mDNS
zeroconf = advertise_master(PORT)
print("[MASTER] mDNS advertised")

try:
    while True:
        time.sleep(0.5)
except KeyboardInterrupt:
    zeroconf.close()


# ZeroMQ
ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.bind(f"tcp://*:{PORT}")

registry = NodeRegistry()

print("[MASTER] Waiting for workers...")

rep.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second

try:
    while True:
        try:
            msg = rep.recv_json()

            if msg["type"] == "REGISTER":
                registry.register(msg["node_id"], msg["ip"])
                print(f"[MASTER] Registered {msg['node_id']} @ {msg['ip']}")
                print(f"[MASTER] Registry: {registry.get_nodes()}")

                rep.send_json({"status": "OK"})

        except zmq.Again:
            pass   # timeout, loop again

except KeyboardInterrupt:
    print("[MASTER] Stopped")