"""Deterministic fake backend for Wi-Fi adapter contract tests."""

from __future__ import annotations

from collections.abc import Iterable

from ..models import IPv4Configuration, WifiAccessPoint, WifiCapabilities


class FakeWifiBackend:
    """Record backend lifecycle operations without touching host networking."""

    def __init__(
        self,
        *,
        ipv4: IPv4Configuration | None = None,
        capabilities: WifiCapabilities | None = None,
        fail_on: Iterable[str] = (),
        access_points: Iterable[WifiAccessPoint] = (),
    ) -> None:
        self.operations: list[str] = []
        self.ipv4 = ipv4 or IPv4Configuration(
            address="10.42.20.250",
            prefix=24,
        )
        self._capabilities = capabilities or WifiCapabilities(
            ap_hosting=True,
            scan_while_hosting=False,
            temporary_profiles=True,
            associated_client_reporting=False,
            backend_recovery=True,
        )
        self.fail_on = set(fail_on)
        self.access_points = list(access_points)
        self.initialized = False
        self.closed = False

    @property
    def capabilities(self) -> WifiCapabilities:
        """Return configured fake capabilities."""
        return self._capabilities

    def _record(self, operation: str) -> None:
        """Record an operation and raise when configured to fail there."""
        self.operations.append(operation)
        if operation in self.fail_on:
            raise RuntimeError(f"Fake Wi-Fi backend failed during {operation}")

    async def initialize(self) -> None:
        """Record fake backend initialization."""
        self._record("initialize")
        self.initialized = True
        self.closed = False

    async def ensure_ap_active(self) -> IPv4Configuration:
        """Return the configured fake Nexus AP network."""
        self._record("ensure_ap_active")
        if not self.initialized:
            raise RuntimeError("Fake Wi-Fi backend is not initialized")
        return self.ipv4

    async def shutdown(self) -> None:
        """Record shutdown and clear initialized state."""
        self._record("shutdown")
        self.closed = True
        self.initialized = False

    async def begin_provisioning(self) -> list[WifiAccessPoint]:
        """Return deterministic provisioning scan results."""
        self._record("begin_provisioning")
        return list(self.access_points)

    async def connect_temporary(
        self,
        access_point: WifiAccessPoint,
    ) -> IPv4Configuration:
        """Record a volatile connection and return a fake client network."""
        self._record(f"connect_temporary:{access_point.ssid}")
        return IPv4Configuration(address="192.168.1.2", prefix=24)

    async def restore_ap(
        self,
        *,
        remote_access_point_disappeared: bool = False,
    ) -> IPv4Configuration:
        """Record AP restoration and return the configured fake AP network."""
        self._record("restore_ap")
        if remote_access_point_disappeared:
            self.operations.append("restore_ap:remote_disappeared")
        return self.ipv4

    def reset_session_diagnostics(self) -> None:
        """Clear recorded operations at a simulated session boundary."""
        self.operations.clear()

    def get_diagnostics_snapshot(self) -> dict:
        """Return deterministic fake-backend state for contract tests."""
        return {
            "implementation": "fake",
            "initialized": self.initialized,
            "closed": self.closed,
            "operations": list(self.operations),
        }
