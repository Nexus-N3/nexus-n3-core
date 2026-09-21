"""Runtime configuration for the SensorManager Wi-Fi adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
import os

from nexus_n3.core.runtime_env import load_runtime_env


def _env_bool(name: str, default: bool) -> bool:
    """Parse a strict boolean environment variable."""
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
    wifi_interface: str | None = None
    wifi_profile: str = "nexus-n3-sensor-ap"
    bridge_interface: str = "br-sensor"
    bridge_profile: str = "nexus-n3-sensor-bridge"
    vlan_interface: str | None = None
    vlan_profile: str = "nexus-n3-sensor-vlan"
    vlan_id: int = 20
    ap_ssid: str = "nexus-n3-sensors"
    ap_password: str | None = field(default=None, repr=False)
    ap_channel: int | None = None
    provisioning_profile: str = "nexus-n3-sensor-provision"
    expected_sensor_cidr: str | None = None
    discovery_timeout_s: float = 20.0
    connect_timeout_s: float = 30.0
    provisioning_join_timeout_s: float = 90.0
    allow_network_stack_restart: bool = False
    network_stack_restart_timeout_s: float = 30.0
    regulatory_domain: str = "EE"

    def __post_init__(self) -> None:
        """Normalize and validate all Wi-Fi runtime settings."""
        backend = self.backend.strip().lower()
        if backend not in {"fake", "linux-networkmanager", "windows-native"}:
            raise ValueError(f"Unsupported Wi-Fi backend: {self.backend!r}")
        object.__setattr__(self, "backend", backend)

        if not self.wifi_profile.strip():
            raise ValueError("Wi-Fi AP profile must not be empty")
        if not self.ap_ssid:
            raise ValueError("Wi-Fi AP SSID must not be empty")
        if backend != "fake":
            required = {
                "Wi-Fi interface": self.wifi_interface,
                "sensor bridge interface": self.bridge_interface,
                "sensor bridge profile": self.bridge_profile,
                "sensor VLAN interface": self.vlan_interface,
                "sensor VLAN profile": self.vlan_profile,
            }
            for label, value in required.items():
                if not (value or "").strip():
                    raise ValueError(
                        f"A {label} is required for a platform backend"
                    )
        if not 1 <= self.vlan_id <= 4094:
            raise ValueError("Sensor VLAN ID must be between 1 and 4094")
        if self.ap_channel is not None and self.ap_channel <= 0:
            raise ValueError("Wi-Fi AP channel must be positive")
        if not self.provisioning_profile.strip():
            raise ValueError("Wi-Fi provisioning profile must not be empty")
        if (
            self.discovery_timeout_s <= 0
            or self.connect_timeout_s <= 0
            or self.provisioning_join_timeout_s <= 0
            or self.network_stack_restart_timeout_s <= 0
        ):
            raise ValueError("Wi-Fi timeouts must be positive")
        regulatory_domain = self.regulatory_domain.strip().upper()
        if len(regulatory_domain) != 2 or not regulatory_domain.isalpha():
            raise ValueError("Wi-Fi regulatory domain must be a two-letter code")
        object.__setattr__(self, "regulatory_domain", regulatory_domain)
        if self.expected_sensor_cidr is not None:
            try:
                expected_interface = ipaddress.ip_interface(
                    self.expected_sensor_cidr
                )
            except ValueError as exc:
                raise ValueError(
                    "Invalid expected sensor-network CIDR: "
                    f"{self.expected_sensor_cidr!r}"
                ) from exc
            if not isinstance(expected_interface, ipaddress.IPv4Interface):
                raise ValueError("Expected sensor-network CIDR must be IPv4")

    @classmethod
    def from_env(cls) -> "WifiRuntimeConfig":
        """Load Wi-Fi settings from the shared runtime environment."""

        load_runtime_env()
        raw_channel = os.environ.get("NEXUS_SENSOR_AP_CHANNEL")
        return cls(
            enabled=_env_bool("NEXUS_SENSOR_NETWORK_ENABLED", False),
            backend=os.environ.get("NEXUS_WIFI_BACKEND", "fake"),
            wifi_interface=(
                os.environ.get("NEXUS_SENSOR_WIFI_INTERFACE") or None
            ),
            wifi_profile=os.environ.get(
                "NEXUS_SENSOR_WIFI_PROFILE",
                "nexus-n3-sensor-ap",
            ),
            bridge_interface=os.environ.get(
                "NEXUS_SENSOR_BRIDGE_INTERFACE",
                "br-sensor",
            ),
            bridge_profile=os.environ.get(
                "NEXUS_SENSOR_BRIDGE_PROFILE",
                "nexus-n3-sensor-bridge",
            ),
            vlan_interface=(
                os.environ.get("NEXUS_SENSOR_VLAN_INTERFACE") or None
            ),
            vlan_profile=os.environ.get(
                "NEXUS_SENSOR_VLAN_PROFILE",
                "nexus-n3-sensor-vlan",
            ),
            vlan_id=int(os.environ.get("NEXUS_SENSOR_VLAN_ID", "20")),
            ap_ssid=os.environ.get(
                "NEXUS_SENSOR_AP_SSID",
                "nexus-n3-sensors",
            ),
            ap_password=os.environ.get("NEXUS_SENSOR_AP_PASSWORD") or None,
            ap_channel=int(raw_channel) if raw_channel else None,
            provisioning_profile=os.environ.get(
                "NEXUS_WIFI_PROVISIONING_CONNECTION",
                "nexus-n3-sensor-provision",
            ),
            expected_sensor_cidr=(
                os.environ.get("NEXUS_SENSOR_EXPECTED_CIDR") or None
            ),
            discovery_timeout_s=float(
                os.environ.get("NEXUS_WIFI_DISCOVERY_TIMEOUT_S", "20")
            ),
            connect_timeout_s=float(
                os.environ.get("NEXUS_WIFI_CONNECT_TIMEOUT_S", "30")
            ),
            provisioning_join_timeout_s=float(
                os.environ.get(
                    "NEXUS_WIFI_PROVISIONING_JOIN_TIMEOUT_S",
                    "90",
                )
            ),
            allow_network_stack_restart=_env_bool(
                "NEXUS_WIFI_ALLOW_NETWORK_STACK_RESTART",
                False,
            ),
            network_stack_restart_timeout_s=float(
                os.environ.get(
                    "NEXUS_NETWORK_STACK_RESTART_TIMEOUT_SECONDS",
                    "30",
                )
            ),
            regulatory_domain=os.environ.get(
                "NEXUS_WIFI_REGULATORY_DOMAIN",
                "EE",
            ),
        )
