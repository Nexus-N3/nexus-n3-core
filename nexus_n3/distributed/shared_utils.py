"""Shared utilities for distributed nodes."""

import socket


def get_local_ip(target_ip: str = "8.8.8.8") -> str:
    """Return the local IP used to reach the target address."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect((target_ip, 80))
        ip = sock.getsockname()[0]
    finally:
        sock.close()
    return ip
