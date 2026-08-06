"""Safe control objects for future exclusive Wi-Fi provisioning sessions."""

from __future__ import annotations

import asyncio


class ProvisioningControls:
    """Platform-neutral hints a device provisioner may give the backend."""

    def __init__(self) -> None:
        self._remote_access_point_disappeared = False

    def remote_access_point_disappeared(self) -> None:
        """Record the expected disappearance of the temporary remote AP."""

        self._remote_access_point_disappeared = True

    @property
    def did_remote_access_point_disappear(self) -> bool:
        return self._remote_access_point_disappeared


class ExclusiveClientSession:
    """Minimal Phase 1 exclusive boundary; network operations arrive later."""

    def __init__(self, lock: asyncio.Lock):
        self._lock = lock
        self.controls = ProvisioningControls()

    async def __aenter__(self) -> "ExclusiveClientSession":
        await self._lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self._lock.release()
