"""Linux Wi-Fi backend implemented through NetworkManager's D-Bus API."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable
import ipaddress

from ..config import ApAddressMode, WifiRuntimeConfig
from ..errors import WifiBackendUnavailable
from ..models import IPv4Configuration, WifiAccessPoint, WifiCapabilities
from .networkmanager_dbus import (
    NetworkManagerAccessPoint,
    NetworkManagerClient,
    NetworkManagerDBusError,
    unwrap_setting,
)
WIFI_RECOVERY_UNIT = "nexus-n3-wifi-recovery.service"
RecoveryRunner = Callable[[str, float], Awaitable[None]]


class LinuxNetworkManagerBackend:
    """Own the shared Linux radio and saved Nexus sensor AP profile."""

    def __init__(
        self,
        config: WifiRuntimeConfig,
        *,
        client: NetworkManagerClient | None = None,
        recovery_runner: RecoveryRunner | None = None,
    ) -> None:
        self.config = config
        self._client = client or NetworkManagerClient()
        self._run_recovery = recovery_runner or self._run_fixed_recovery_unit
        self._initialized = False
        self._device_path: str | None = None
        self._ap_connection_path: str | None = None
        self._access_points: dict[str, NetworkManagerAccessPoint] = {}
        self._temporary_connection_path: str | None = None
        self._temporary_active_path: str | None = None
        self._provisioning_active = False
        self._diagnostic_counts: Counter[str] = Counter()
        self._last_recovery_error: str | None = None
        self._capabilities = WifiCapabilities(
            ap_hosting=True,
            scan_while_hosting=False,
            temporary_profiles=True,
            associated_client_reporting=False,
            backend_recovery=True,
        )

    @property
    def capabilities(self) -> WifiCapabilities:
        """Return the stable capabilities of the Linux reference backend."""
        return self._capabilities

    async def initialize(self) -> None:
        """Connect to NetworkManager and resolve the configured radio/profile."""
        if self._initialized:
            return
        self._diagnostic_counts["initialize_attempts"] += 1
        try:
            await self._client.connect()
            await self._resolve_paths()
        except (NetworkManagerDBusError, TimeoutError) as exc:
            raise WifiBackendUnavailable(
                "Unable to initialize the NetworkManager Wi-Fi backend"
            ) from exc
        self._initialized = True
        self._diagnostic_counts["initialize_successes"] += 1

    async def _resolve_paths(self) -> None:
        """Resolve generation-scoped D-Bus paths from stable configuration."""
        self._device_path = await self._client.get_device_path(self._interface)
        self._ap_connection_path = await self._client.find_saved_connection(
            self.config.ap_profile
        )
        if self._ap_connection_path is None:
            raise WifiBackendUnavailable(
                f"NetworkManager profile {self.config.ap_profile!r} was not found"
            )

    async def ensure_ap_active(self) -> IPv4Configuration:
        """Validate, activate if necessary, and return the Nexus AP network."""
        self._ensure_initialized()
        self._diagnostic_counts["ensure_ap_attempts"] += 1
        try:
            await self._resolve_paths()
            settings = await self._client.get_connection_settings(
                self._ap_connection_path
            )
            mode = str(
                unwrap_setting(settings, "802-11-wireless", "mode") or ""
            ).lower()
            if mode != "ap":
                raise WifiBackendUnavailable(
                    f"NetworkManager connection {self.config.ap_profile!r} is not an AP"
                )

            raw_ssid = unwrap_setting(settings, "802-11-wireless", "ssid")
            if isinstance(raw_ssid, (bytes, bytearray)):
                ssid = bytes(raw_ssid).decode(errors="replace")
            elif isinstance(raw_ssid, list):
                ssid = bytes(raw_ssid).decode(errors="replace")
            else:
                ssid = str(raw_ssid or "")
            if ssid != self.config.ap_ssid:
                raise WifiBackendUnavailable(
                    f"Sensor AP SSID is {ssid!r}, expected {self.config.ap_ssid!r}"
                )

            ipv4_method = str(
                unwrap_setting(settings, "ipv4", "method") or ""
            ).lower()
            expected_method = (
                "shared"
                if self.config.ap_address_mode
                is ApAddressMode.NETWORKMANAGER_SHARED
                else "manual"
            )
            if ipv4_method != expected_method:
                raise WifiBackendUnavailable(
                    f"Sensor AP IPv4 method is {ipv4_method!r}, "
                    f"expected {expected_method!r}"
                )

            active_path = await self._client.find_active_connection(
                self.config.ap_profile
            )
            if active_path is None:
                await self._client.activate(
                    self._ap_connection_path,
                    self._device_path,
                )
                active_path = await self._client.wait_for_connection_id(
                    self.config.ap_profile,
                    self._device_path,
                    self.config.connect_timeout_s,
                )

            network = await self._client.get_ipv4(active_path)
            if network is None:
                network = await self._client.wait_for_ipv4(
                    active_path,
                    self.config.connect_timeout_s,
                )
            self._validate_network(network)
            self._diagnostic_counts["ensure_ap_successes"] += 1
            return network
        except WifiBackendUnavailable:
            raise
        except (NetworkManagerDBusError, TimeoutError) as exc:
            raise WifiBackendUnavailable(
                f"Failed to activate sensor AP {self.config.ap_profile!r}"
            ) from exc

    async def begin_provisioning(self) -> list[WifiAccessPoint]:
        """Release the AP radio and perform a fresh NetworkManager scan."""

        self._ensure_initialized()
        self._diagnostic_counts["scan_attempts"] += 1
        try:
            await self._resolve_paths()
            await self._client.quiesce(
                self._device_path,
                self.config.connect_timeout_s,
            )
            self._provisioning_active = True
            native_access_points = await self._client.scan(
                self._device_path,
                self.config.discovery_timeout_s,
            )
        except (NetworkManagerDBusError, TimeoutError) as exc:
            raise WifiBackendUnavailable(
                "NetworkManager failed to complete a fresh Wi-Fi scan"
            ) from exc

        self._access_points = {item.path: item for item in native_access_points}
        self._diagnostic_counts["scan_successes"] += 1
        self._diagnostic_counts["access_points_seen"] += len(native_access_points)
        return [
            WifiAccessPoint(
                id=item.path,
                ssid=item.ssid,
                bssid=item.bssid,
                strength=item.strength,
                frequency_mhz=item.frequency_mhz,
                secured=item.secured,
            )
            for item in native_access_points
        ]

    async def connect_temporary(
        self,
        access_point: WifiAccessPoint,
    ) -> IPv4Configuration:
        """Create a volatile client connection to an open sensor AP."""

        self._diagnostic_counts["temporary_connect_attempts"] += 1

        if access_point.secured:
            raise WifiBackendUnavailable(
                f"Provisioning AP {access_point.ssid!r} is unexpectedly secured"
            )
        native = self._access_points.get(access_point.id)
        if native is None:
            raise WifiBackendUnavailable(
                "The selected provisioning AP is no longer in the fresh scan"
            )
        try:
            (
                self._temporary_connection_path,
                self._temporary_active_path,
            ) = await self._client.add_and_activate_open_wifi(
                self._device_path,
                native,
                self._interface,
                self.config.provisioning_profile,
            )
            await self._client.wait_for_active(
                self._temporary_active_path,
                self.config.connect_timeout_s,
            )
            network = await self._client.wait_for_ipv4(
                self._temporary_active_path,
                self.config.connect_timeout_s,
            )
            self._diagnostic_counts["temporary_connect_successes"] += 1
            return network
        except (NetworkManagerDBusError, TimeoutError) as exc:
            raise WifiBackendUnavailable(
                f"Failed to connect to provisioning AP {access_point.ssid!r}"
            ) from exc

    async def restore_ap(
        self,
        *,
        remote_access_point_disappeared: bool = False,
    ) -> IPv4Configuration:
        """Remove volatile state and restore the saved Nexus AP."""

        self._diagnostic_counts["restore_ap_attempts"] += 1
        await self._cleanup_temporary_connection()
        if remote_access_point_disappeared:
            if not self.config.allow_network_stack_restart:
                raise WifiBackendUnavailable(
                    "Direct sensor AP recovery is required. Install the fixed "
                    "Wi-Fi recovery service and set "
                    "NEXUS_WIFI_ALLOW_NETWORK_STACK_RESTART=1."
                )
            await self._restart_network_stack()
            network = await self._activate_ap_after_cleanup()
        else:
            try:
                network = await self._activate_ap_after_cleanup()
            except WifiBackendUnavailable as normal_error:
                if not self.config.allow_network_stack_restart:
                    raise
                await self._restart_network_stack()
                try:
                    network = await self._activate_ap_after_cleanup()
                except WifiBackendUnavailable as recovery_error:
                    raise recovery_error from normal_error

        self._provisioning_active = False
        self._diagnostic_counts["restore_ap_successes"] += 1
        return network

    async def _cleanup_temporary_connection(self) -> None:
        """Best-effort deactivate and delete the volatile client profile."""
        if self._temporary_active_path is not None:
            try:
                await self._client.deactivate(self._temporary_active_path)
            except NetworkManagerDBusError:
                pass
        if self._temporary_connection_path is not None:
            try:
                await self._client.delete_connection(
                    self._temporary_connection_path
                )
            except NetworkManagerDBusError:
                pass
        self._temporary_active_path = None
        self._temporary_connection_path = None
        self._access_points.clear()

    async def _activate_ap_after_cleanup(self) -> IPv4Configuration:
        """Quiesce the radio and restore the saved Nexus AP profile."""
        try:
            await self._resolve_paths()
            await self._client.quiesce(
                self._device_path,
                self.config.connect_timeout_s,
            )
            await asyncio.sleep(2.0)
            await self._client.activate(
                self._ap_connection_path,
                self._device_path,
            )
            active_path = await self._client.wait_for_connection_id(
                self.config.ap_profile,
                self._device_path,
                self.config.connect_timeout_s,
            )
            network = await self._client.wait_for_ipv4(
                active_path,
                self.config.connect_timeout_s,
            )
            self._validate_network(network)
            return network
        except (NetworkManagerDBusError, TimeoutError) as exc:
            raise WifiBackendUnavailable(
                f"Failed to restore sensor AP {self.config.ap_profile!r}"
            ) from exc

    async def _restart_network_stack(self) -> None:
        """Run bounded fixed recovery and reconnect the D-Bus client."""
        timeout = self.config.network_stack_restart_timeout_s
        self._diagnostic_counts["network_stack_recovery_attempts"] += 1
        try:
            await self._run_recovery(WIFI_RECOVERY_UNIT, timeout)
        except Exception as exc:
            self._diagnostic_counts["network_stack_recovery_failures"] += 1
            self._last_recovery_error = f"{type(exc).__name__}: {exc}"
            raise
        self._diagnostic_counts["network_stack_recovery_successes"] += 1
        self._last_recovery_error = None
        self._client.close()
        await self._client.connect()
        try:
            self._device_path, self._ap_connection_path = (
                await self._client.wait_until_available(
                    self._interface,
                    self.config.ap_profile,
                    timeout,
                )
            )
        except (NetworkManagerDBusError, TimeoutError) as exc:
            raise WifiBackendUnavailable(
                "NetworkManager did not return after Wi-Fi recovery"
            ) from exc

    @staticmethod
    async def _run_fixed_recovery_unit(unit_name: str, timeout: float) -> None:
        """Run only the allow-listed recovery service without prompting."""
        if unit_name != WIFI_RECOVERY_UNIT:
            raise WifiBackendUnavailable("Refusing to run an unknown recovery unit")
        process = await asyncio.create_subprocess_exec(
            "sudo",
            "-n",
            "/usr/bin/systemctl",
            "restart",
            WIFI_RECOVERY_UNIT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.wait()
            raise WifiBackendUnavailable(
                f"Fixed Wi-Fi recovery unit {unit_name!r} timed out"
            ) from exc
        if process.returncode != 0:
            detail = stderr.decode(errors="replace").strip()
            if not detail:
                detail = stdout.decode(errors="replace").strip()
            raise WifiBackendUnavailable(
                f"Unable to run fixed Wi-Fi recovery unit {unit_name!r}: "
                f"{detail or 'permission or service failure'}"
            )

    def _validate_network(self, network: IPv4Configuration) -> None:
        """Validate the active IPv4 interface against an optional expected CIDR."""
        expected = self.config.expected_ap_cidr
        if expected is not None and (
            ipaddress.ip_interface(network.cidr)
            != ipaddress.ip_interface(expected)
        ):
            raise WifiBackendUnavailable(
                f"Sensor AP address is {network.cidr}, expected {expected}"
            )

    async def shutdown(self) -> None:
        """Close D-Bus resources and clear generation-scoped state."""
        self._client.close()
        self._initialized = False
        self._device_path = None
        self._ap_connection_path = None
        self._provisioning_active = False

    def reset_session_diagnostics(self) -> None:
        """Reset backend counters without changing NetworkManager state."""
        self._diagnostic_counts.clear()
        self._last_recovery_error = None

    def get_diagnostics_snapshot(self) -> dict:
        """Return safe NetworkManager state and session counters."""
        return {
            "implementation": "linux-networkmanager-dbus",
            "initialized": self._initialized,
            "interface": self.config.interface_name,
            "device_path": self._device_path,
            "ap_profile": self.config.ap_profile,
            "ap_ssid": self.config.ap_ssid,
            "ap_address_mode": self.config.ap_address_mode.value,
            "provisioning_active": self._provisioning_active,
            "temporary_profile_active": self._temporary_active_path is not None,
            "visible_access_points": len(self._access_points),
            "network_stack_recovery_enabled": (
                self.config.allow_network_stack_restart
            ),
            "last_recovery_error": self._last_recovery_error,
            "counters": dict(self._diagnostic_counts),
        }

    def _ensure_initialized(self) -> None:
        """Reject backend operations until initialization completes."""
        if not self._initialized:
            raise WifiBackendUnavailable(
                "The linux-networkmanager Wi-Fi backend is not initialized"
            )

    @property
    def _interface(self) -> str:
        """Return the required configured NetworkManager interface name."""
        interface = (self.config.interface_name or "").strip()
        if not interface:
            raise WifiBackendUnavailable(
                "The linux-networkmanager backend requires a Wi-Fi interface"
            )
        return interface


__all__ = ["LinuxNetworkManagerBackend"]
