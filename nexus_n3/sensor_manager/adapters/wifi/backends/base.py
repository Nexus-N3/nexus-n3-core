"""Private platform backend contract for the Wi-Fi adapter."""

from __future__ import annotations

from typing import Protocol

from ..models import IPv4Configuration, WifiAccessPoint, WifiCapabilities


class WifiBackend(Protocol):
    """Small Phase 1 backend contract, extended by later implementation."""

    @property
    def capabilities(self) -> WifiCapabilities: ...

    async def initialize(self) -> None: ...

    async def ensure_ap_active(self) -> IPv4Configuration: ...

    async def begin_provisioning(self) -> list[WifiAccessPoint]: ...

    async def connect_temporary(
        self,
        access_point: WifiAccessPoint,
    ) -> IPv4Configuration: ...

    async def restore_ap(
        self,
        *,
        remote_access_point_disappeared: bool = False,
    ) -> IPv4Configuration: ...

    async def shutdown(self) -> None: ...

    def reset_session_diagnostics(self) -> None: ...

    def get_diagnostics_snapshot(self) -> dict: ...
