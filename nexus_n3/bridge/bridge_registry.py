"""Bridge discovery and creation helpers."""

from __future__ import annotations


def discover_bridges() -> dict[str, dict]:
    """Return bridge metadata keyed by bridge name."""
    return {
        "azure_bridge": {
            "scope": "remote",
            "display_name": "Azure IoT Hub",
            "supports_runtime_control": True,
        },
    }


def create_bridge(
    bridge_name: str,
    *,
    site: str,
    customer_id: str | None = None,
    site_id: str | None = None,
    site_name: str | None = None,
    remote_control_enabled: bool = False,
):
    """Instantiate a configured bridge service."""
    normalized = bridge_name.lower()
    if normalized == "azure_bridge":
        from nexus_n3.azure_bridge import AzureBridgeConfig, AzureBridgeService

        import os

        os.environ.setdefault("AZURE_IOT_SITE", site)
        if customer_id:
            os.environ.setdefault("AZURE_IOT_CUSTOMER_ID", customer_id)
        if site_id or site:
            os.environ.setdefault("AZURE_IOT_SITE_ID", site_id or site)
        if site_name or site:
            os.environ.setdefault("AZURE_IOT_SITE_NAME", site_name or site)
        config = AzureBridgeConfig.from_env()
        config.remote_control_enabled = remote_control_enabled or config.remote_control_enabled
        return AzureBridgeService(config)


    available = ", ".join(sorted(discover_bridges().keys()))
    raise ValueError(f"Unknown bridge '{bridge_name}'. Available: {available}")
