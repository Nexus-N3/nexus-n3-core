# master.py
import zmq
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

context = zmq.Context()
rep = context.socket(zmq.REP)
rep.bind("tcp://*:5555")

my_ip = get_local_ip()
print(f"[MASTER] IP = {my_ip}")
print("[MASTER] Waiting for workers...")

workers = {}

while True:
    msg = rep.recv_json()

    if msg["type"] == "REGISTER":
        node_id = msg["node_id"]
        ip = msg["ip"]

        workers[node_id] = ip

        print(f"[MASTER] Registered {node_id} @ {ip}")
        print(f"[MASTER] Workers = {workers}")

        rep.send_json({"status": "OK"})
