"""Private platform backend contract for the Wi-Fi adapter."""

from __future__ import annotations

from typing import Protocol

from ..models import IPv4Configuration, WifiAccessPoint, WifiCapabilities


class WifiBackend(Protocol):
    """Small Phase 1 backend contract, extended by later implementation."""

    @property
    def capabilities(self) -> WifiCapabilities:
        """Return stable platform capability flags."""
        ...

    async def initialize(self) -> None:
        """Initialize platform resources."""
        ...

    async def ensure_ap_active(self) -> IPv4Configuration:
        """Validate or activate the Nexus sensor AP."""
        ...

    async def begin_provisioning(self) -> list[WifiAccessPoint]:
        """Release AP hosting and return a fresh provisioning scan."""
        ...

    async def connect_temporary(
        self,
        access_point: WifiAccessPoint,
    ) -> IPv4Configuration:
        """Connect temporarily to a selected provisioning AP."""
        ...

    async def restore_ap(
        self,
        *,
        remote_access_point_disappeared: bool = False,
    ) -> IPv4Configuration:
        """Remove temporary state and restore the Nexus sensor AP."""
        ...

    async def shutdown(self) -> None:
        """Release platform resources."""
        ...

    def reset_session_diagnostics(self) -> None:
        """Reset backend-owned session counters."""
        ...

    def get_diagnostics_snapshot(self) -> dict:
        """Return JSON-safe backend diagnostics."""
        ...
