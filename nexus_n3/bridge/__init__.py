"""Bridge registry and shared bridge clients for Nexus N3 Core."""

from .bridge_registry import create_bridge, discover_bridges
from .local_gateway_client import LocalGatewayClient

__all__ = [
    "LocalGatewayClient",
    "create_bridge",
    "discover_bridges",
]
