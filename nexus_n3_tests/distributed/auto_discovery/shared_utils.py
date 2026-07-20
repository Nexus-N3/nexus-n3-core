import socket

# get a nodes own ip
def get_local_ip(target_ip="8.8.8.8"):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # We never actually send data
        s.connect((target_ip, 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip