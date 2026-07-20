# mdns_worker.py
from zeroconf import Zeroconf, ServiceBrowser
import socket
import time
import zmq
import sys
from nexus_n3_tests.distributed.auto_discovery.shared_utils import get_local_ip

import weakref
import gc

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
    print("discovering master")
    zc = Zeroconf()
    listener = MasterListener()
    browser = ServiceBrowser(zc, "_nexusn3._tcp.local.", listener)
    wr = weakref.ref(browser)
    t0 = time.time()
    while time.time() - t0 < timeout:
        if wr() is None:
            print("ServiceBrowser GC'D")
        if listener.master_ip:
            return listener.master_ip, listener.master_port
        time.sleep(0.1)

    raise RuntimeError("Master not found via mDNS")

# worker.py

NODE_ID = sys.argv[1]

print(f"[WORKER {NODE_ID}] Starting...")
my_ip = get_local_ip()
print(f"[WORKER {NODE_ID}] Local IP: {my_ip}")

master_ip, master_port = discover_master()
print(f"[WORKER {NODE_ID}] Found master at {master_ip}:{master_port}")

ctx = zmq.Context()
req = ctx.socket(zmq.REQ)
req.connect(f"tcp://{master_ip}:{master_port}")

req.send_json({
    "type": "REGISTER",
    "node_id": NODE_ID,
    "ip": my_ip
})



reply = req.recv_json()
print(f"[WORKER {NODE_ID}] Reply: {reply}")
