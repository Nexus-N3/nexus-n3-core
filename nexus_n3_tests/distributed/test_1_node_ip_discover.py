# this should be shared code for both masters and workers
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

if __name__ == "__main__":
    ip = get_local_ip()
    print(ip)