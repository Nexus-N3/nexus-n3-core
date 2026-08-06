"""Runtime configuration for the SensorManager Wi-Fi adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import ipaddress
import os

from nexus_n3.core.runtime_env import load_runtime_env


class ApAddressMode(str, Enum):
    """Supported ownership models for the Nexus AP address and DHCP."""

    NETWORKMANAGER_SHARED = "networkmanager-shared"
    STATIC_EXTERNAL_DHCP = "static-external-dhcp"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value, got {raw!r}")


@dataclass(frozen=True)
class WifiRuntimeConfig:
    """Process-level Wi-Fi backend and Nexus AP settings."""

    enabled: bool = False
    backend: str = "fake"
    interface_name: str | None = None
    ap_profile: str = "nexus-n3-sensor-ap"
    ap_ssid: str = "nexus-n3-sensors"
    ap_password: str | None = field(default=None, repr=False)
    ap_channel: int | None = None
    ap_address_mode: ApAddressMode = ApAddressMode.NETWORKMANAGER_SHARED
    expected_ap_cidr: str | None = None
    discovery_timeout_s: float = 20.0
    connect_timeout_s: float = 30.0

    def __post_init__(self) -> None:
        backend = self.backend.strip().lower()
        if backend not in {"fake", "linux-networkmanager", "windows-native"}:
            raise ValueError(f"Unsupported Wi-Fi backend: {self.backend!r}")
        object.__setattr__(self, "backend", backend)

        if not isinstance(self.ap_address_mode, ApAddressMode):
            try:
                address_mode = ApAddressMode(
                    str(self.ap_address_mode).strip().lower()
                )
            except ValueError as exc:
                raise ValueError(
                    f"Unsupported Wi-Fi AP address mode: {self.ap_address_mode!r}"
                ) from exc
            object.__setattr__(self, "ap_address_mode", address_mode)

        if not self.ap_profile.strip():
            raise ValueError("Wi-Fi AP profile must not be empty")
        if not self.ap_ssid:
            raise ValueError("Wi-Fi AP SSID must not be empty")
        if backend != "fake" and not (self.interface_name or "").strip():
            raise ValueError(
                "A Wi-Fi interface name is required for a platform backend"
            )
        if self.ap_channel is not None and self.ap_channel <= 0:
            raise ValueError("Wi-Fi AP channel must be positive")
        if self.discovery_timeout_s <= 0 or self.connect_timeout_s <= 0:
            raise ValueError("Wi-Fi timeouts must be positive")
        if self.expected_ap_cidr is not None:
            try:
                expected_interface = ipaddress.ip_interface(self.expected_ap_cidr)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid expected Wi-Fi AP CIDR: {self.expected_ap_cidr!r}"
                ) from exc
            if not isinstance(expected_interface, ipaddress.IPv4Interface):
                raise ValueError("Expected Wi-Fi AP CIDR must be IPv4")

    @classmethod
    def from_env(cls) -> "WifiRuntimeConfig":
        """Load Wi-Fi settings from the shared runtime environment."""

        load_runtime_env()
        raw_channel = os.environ.get("NEXUS_SENSOR_AP_CHANNEL")
        raw_mode = os.environ.get(
            "NEXUS_SENSOR_AP_ADDRESS_MODE",
            ApAddressMode.NETWORKMANAGER_SHARED.value,
        )
        return cls(
            enabled=_env_bool("NEXUS_SENSOR_NETWORK_ENABLED", False),
            backend=os.environ.get("NEXUS_WIFI_BACKEND", "fake"),
            interface_name=os.environ.get("NEXUS_SENSOR_INTERFACE") or None,
            ap_profile=os.environ.get(
                "NEXUS_SENSOR_CONNECTION",
                "nexus-n3-sensor-ap",
            ),
            ap_ssid=os.environ.get(
                "NEXUS_SENSOR_AP_SSID",
                "nexus-n3-sensors",
            ),
            ap_password=os.environ.get("NEXUS_SENSOR_AP_PASSWORD") or None,
            ap_channel=int(raw_channel) if raw_channel else None,
            ap_address_mode=ApAddressMode(raw_mode.strip().lower()),
            expected_ap_cidr=(
                os.environ.get("NEXUS_SENSOR_AP_EXPECTED_CIDR") or None
            ),
            discovery_timeout_s=float(
                os.environ.get("NEXUS_WIFI_DISCOVERY_TIMEOUT_S", "20")
            ),
            connect_timeout_s=float(
                os.environ.get("NEXUS_WIFI_CONNECT_TIMEOUT_S", "30")
            ),
        )
