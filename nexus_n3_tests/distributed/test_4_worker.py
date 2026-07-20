# worker.py
import zmq
import sys

import socket

def get_local_ip(target_ip="8.8.8.8"):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # We never actually send data
        s.connect((target_ip, 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

MASTER_IP = sys.argv[1]
NODE_ID = sys.argv[2]

context = zmq.Context()
req = context.socket(zmq.REQ)
req.connect(f"tcp://{MASTER_IP}:5555")

my_ip = get_local_ip()
print(f"[WORKER {NODE_ID}] IP = {my_ip}")
print(f"[WORKER {NODE_ID}] Registering with master {MASTER_IP}")

req.send_json({
    "type": "REGISTER",
    "node_id": NODE_ID,
    "ip": my_ip
})

reply = req.recv_json()
print(f"[WORKER {NODE_ID}] Reply: {reply}")
