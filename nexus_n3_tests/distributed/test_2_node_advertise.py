from zeroconf import Zeroconf, ServiceInfo
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

ip = socket.inet_aton(get_local_ip())

info = ServiceInfo(
    "_nexusn3._tcp.local.",
    "nexus-n3-master._nexusn3._tcp.local.",
    addresses=[ip],
    port=5555,
)

zc = Zeroconf()
zc.register_service(info)

