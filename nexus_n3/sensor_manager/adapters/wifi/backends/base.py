"""Private platform backend contract for the Wi-Fi adapter."""

from __future__ import annotations

from typing import Protocol

from ..models import IPv4Configuration, WifiCapabilities


class WifiBackend(Protocol):
    """Small Phase 1 backend contract, extended by later implementation."""

    @property
    def capabilities(self) -> WifiCapabilities: ...

    async def initialize(self) -> None: ...

    async def ensure_ap_active(self) -> IPv4Configuration: ...

    async def shutdown(self) -> None: ...
