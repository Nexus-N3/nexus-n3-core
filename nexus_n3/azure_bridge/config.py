"""Configuration helpers for the Azure bridge."""

from __future__ import annotations

from dataclasses import dataclass
import os

from nexus_n3.core.runtime_env import load_runtime_env


@dataclass(slots=True)
class AzureBridgeConfig:
    """Runtime configuration for the Azure bridge."""

    connection_string: str
    device_id: str
    customer_id: str | None = None
    site_id: str | None = None
    site_name: str | None = None
    site: str | None = None
    local_cmd_pub_addr: str = "tcp://127.0.0.1:5555"
    local_evt_sub_addr: str = "tcp://127.0.0.1:5556"
    remote_control_enabled: bool = False
    state_file: str | None = None
    websockets: bool = False
    keep_alive: int = 60
    connection_retry_interval: int = 10
    upload_retry_interval: int = 5

    @classmethod
    def from_env(cls) -> "AzureBridgeConfig":
        """Build bridge config from environment variables."""
        load_runtime_env()

        connection_string = os.environ.get("AZURE_IOT_CONNECTION_STRING", "").strip()
        if not connection_string:
            raise ValueError("AZURE_IOT_CONNECTION_STRING is required")

        device_id = os.environ.get("AZURE_IOT_DEVICE_ID", "").strip()
        if not device_id:
            raise ValueError("AZURE_IOT_DEVICE_ID is required")

        return cls(
            connection_string=connection_string,
            device_id=device_id,
            customer_id=os.environ.get("AZURE_IOT_CUSTOMER_ID"),
            site_id=os.environ.get("AZURE_IOT_SITE_ID") or os.environ.get("AZURE_IOT_SITE"),
            site_name=os.environ.get("AZURE_IOT_SITE_NAME") or os.environ.get("AZURE_IOT_SITE"),
            site=os.environ.get("AZURE_IOT_SITE"),
            local_cmd_pub_addr=os.environ.get("AZURE_BRIDGE_LOCAL_CMD_ADDR", "tcp://127.0.0.1:5555"),
            local_evt_sub_addr=os.environ.get("AZURE_BRIDGE_LOCAL_EVT_ADDR", "tcp://127.0.0.1:5556"),
            remote_control_enabled=os.environ.get("AZURE_BRIDGE_REMOTE_CONTROL_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"},
            state_file=os.environ.get("AZURE_BRIDGE_STATE_FILE") or None,
            websockets=os.environ.get("AZURE_BRIDGE_USE_WEBSOCKETS", "").strip().lower() in {"1", "true", "yes", "on"},
            keep_alive=int(os.environ.get("AZURE_BRIDGE_KEEP_ALIVE", "60")),
            connection_retry_interval=int(os.environ.get("AZURE_BRIDGE_CONNECTION_RETRY_INTERVAL", "10")),
            upload_retry_interval=int(os.environ.get("AZURE_BRIDGE_UPLOAD_RETRY_INTERVAL", "5")),
        )
