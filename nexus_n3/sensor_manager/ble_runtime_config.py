"""Runtime configuration for BLE backend selection.

`nexus-n3-core` keeps BLE sensors under the single `BLE` adapter family, but the
concrete backend is selected at runtime:

- `bleak`: direct host BLE access using the existing Bleak adapter
- `gateway` / `nexus_ble_gateway`: USB serial transport to the Nexus BLE gateway

The runtime config object centralizes that selection plus any gateway-specific
transport settings so higher layers only need to resolve the `BLE` adapter once
at startup.
"""

from __future__ import annotations

from dataclasses import dataclass
import os

from nexus_n3.core.runtime_env import load_runtime_env


def _normalize_ble_backend(value: str | None) -> str:
    normalized = (value or "bleak").strip().lower()
    if normalized in {"bleak", "host", "local"}:
        print(f"[BLE_BACKEND] Using Internal Bleak BLE Backend")
        return "bleak"
    if normalized in {"gateway", "nexus_ble_gateway", "ble_gateway"}:
        print(f"[BLE_BACKEND] Using Nexus BLE Gateway BLE Backend")
        return "gateway"
    raise ValueError(
        "Unsupported BLE_BACKEND value: "
        f"{value!r}. Expected one of: bleak, gateway, nexus_ble_gateway."
    )


@dataclass(frozen=True)
class BLERuntimeConfig:
    """Process-level BLE backend and gateway transport settings.

    `backend` is always one of the normalized internal values:

    - `bleak`
    - `gateway`

    The remaining fields are only relevant when `backend == "gateway"`.
    """

    backend: str = "bleak"
    gateway_serial_port: str | None = None
    gateway_baudrate: int = 1_000_000
    gateway_protocol_version: int = 1
    gateway_connect_timeout_s: float = 15.0
    gateway_subscribe_timeout_s: float = 5.0
    gateway_write_timeout_s: float = 5.0
    gateway_read_timeout_s: float = 5.0

    @classmethod
    def from_env(cls) -> "BLERuntimeConfig":
        """Build runtime BLE settings from the shared runtime environment."""
        load_runtime_env()
        return cls(
            backend=_normalize_ble_backend(os.environ.get("BLE_BACKEND")),
            gateway_serial_port=os.environ.get("GATEWAY_SERIAL_PORT") or None,
            gateway_baudrate=int(os.environ.get("GATEWAY_BAUDRATE", "1000000")),
            gateway_protocol_version=int(os.environ.get("GATEWAY_PROTOCOL_VERSION", "1")),
            gateway_connect_timeout_s=float(os.environ.get("GATEWAY_CONNECT_TIMEOUT_S", "15.0")),
            gateway_subscribe_timeout_s=float(os.environ.get("GATEWAY_SUBSCRIBE_TIMEOUT_S", "5.0")),
            gateway_write_timeout_s=float(os.environ.get("GATEWAY_WRITE_TIMEOUT_S", "5.0")),
            gateway_read_timeout_s=float(os.environ.get("GATEWAY_READ_TIMEOUT_S", "5.0")),
        )

    @property
    def backend_label(self) -> str:
        """Return the public backend label used in CLI/admin surfaces."""
        if self.backend == "gateway":
            return "nexus_ble_gateway"
        return "bleak"

    def as_public_dict(self) -> dict:
        """Return a sanitized view suitable for events/admin status."""
        return {
            "backend": self.backend,
            "backend_label": self.backend_label,
            "gateway_serial_port": self.gateway_serial_port,
            "gateway_baudrate": self.gateway_baudrate if self.backend == "gateway" else None,
            "gateway_protocol_version": self.gateway_protocol_version if self.backend == "gateway" else None,
        }
