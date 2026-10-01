"""Shared utilities for distributed nodes."""

import os
import socket


def get_local_ip(target_ip: str = "8.8.8.8") -> str:
    """Return the configured control address, or infer one from routing."""
    control_address = os.getenv("NEXUS_N3_CONTROL_ADDRESS", "").strip()
    if control_address:
        return control_address

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect((target_ip, 80))
        ip = sock.getsockname()[0]
    finally:
        sock.close()
    return ip
