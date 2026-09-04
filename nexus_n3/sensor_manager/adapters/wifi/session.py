"""Safe control objects for future exclusive Wi-Fi provisioning sessions."""

from __future__ import annotations

import asyncio

from .backends.base import WifiBackend
from .errors import WifiProvisioningCleanupError
from .models import IPv4Configuration, WifiAccessPoint


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
    """Own one disruptive scan/client/restore operation on the shared radio."""

    def __init__(self, lock: asyncio.Lock, backend: WifiBackend):
        self._lock = lock
        self._backend = backend
        self.controls = ProvisioningControls()
        self.access_points: list[WifiAccessPoint] = []
        self.restored_network: IPv4Configuration | None = None

    async def __aenter__(self) -> "ExclusiveClientSession":
        await self._lock.acquire()
        try:
            self.access_points = await self._backend.begin_provisioning()
            return self
        except BaseException:
            try:
                await self._restore_shielded()
            finally:
                self._lock.release()
            raise

    async def connect_temporary(
        self,
        access_point: WifiAccessPoint,
    ) -> IPv4Configuration:
        return await self._backend.connect_temporary(access_point)

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        cleanup_error = None
        cancelled = False
        try:
            await self._restore_shielded()
        except asyncio.CancelledError:
            cancelled = True
        except BaseException as restore_error:
            cleanup_error = restore_error
        finally:
            self._lock.release()

        if cleanup_error is not None:
            raise WifiProvisioningCleanupError(
                "Failed to restore the Nexus sensor AP after provisioning"
            ) from cleanup_error
        if cancelled:
            raise asyncio.CancelledError

    async def _restore_shielded(self) -> None:
        restore_task = asyncio.create_task(
            self._backend.restore_ap(
                remote_access_point_disappeared=(
                    self.controls.did_remote_access_point_disappear
                )
            )
        )
        try:
            self.restored_network = await asyncio.shield(restore_task)
        except asyncio.CancelledError:
            # Do not release the shared radio lock until restoration finishes.
            self.restored_network = await restore_task
            raise
