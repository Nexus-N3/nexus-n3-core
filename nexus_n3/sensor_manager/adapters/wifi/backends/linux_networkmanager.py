"""Linux Wi-Fi backend implemented with the NetworkManager CLI."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
import ipaddress
import shutil

from ..config import WifiRuntimeConfig
from ..errors import WifiBackendUnavailable
from ..models import IPv4Configuration, WifiAccessPoint, WifiCapabilities


@dataclass(frozen=True)
class _CommandResult:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str]], Awaitable[_CommandResult]]


class LinuxNetworkManagerBackend:
    """Validate and, when necessary, activate the configured sensor AP.

    Sensor discovery and protocol traffic deliberately remain in sensor
    drivers.  This backend owns only the shared host-network lifecycle.
    """

    def __init__(
        self,
        config: WifiRuntimeConfig,
        *,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self.config = config
        self._uses_system_runner = command_runner is None
        self._run_command = command_runner or self._run_subprocess
        self._initialized = False
        self._closed = False
        self._activated_by_backend = False
        self._provisioning_active = False
        self._capabilities = WifiCapabilities(
            ap_hosting=True,
            scan_while_hosting=False,
            temporary_profiles=True,
            associated_client_reporting=False,
            backend_recovery=config.allow_network_stack_restart,
        )

    @property
    def capabilities(self) -> WifiCapabilities:
        return self._capabilities

    async def initialize(self) -> None:
        if self._initialized:
            return
        if self._uses_system_runner and shutil.which("nmcli") is None:
            raise WifiBackendUnavailable(
                "The linux-networkmanager Wi-Fi backend requires nmcli"
            )

        await self._nmcli(
            "-g",
            "GENERAL.STATE",
            "device",
            "show",
            self._interface,
        )
        self._initialized = True
        self._closed = False

    async def ensure_ap_active(self) -> IPv4Configuration:
        if not self._initialized:
            raise WifiBackendUnavailable(
                "The linux-networkmanager Wi-Fi backend is not initialized"
            )

        mode = (
            await self._nmcli(
                "-g",
                "802-11-wireless.mode",
                "connection",
                "show",
                self.config.ap_profile,
            )
        ).strip().lower()
        if mode != "ap":
            raise WifiBackendUnavailable(
                f"NetworkManager connection {self.config.ap_profile!r} is not an AP"
            )

        ssid = (
            await self._nmcli(
                "-g",
                "802-11-wireless.ssid",
                "connection",
                "show",
                self.config.ap_profile,
            )
        ).strip()
        if ssid != self.config.ap_ssid:
            raise WifiBackendUnavailable(
                f"Sensor AP SSID is {ssid!r}, expected {self.config.ap_ssid!r}"
            )

        active_profile = (
            await self._nmcli(
                "-g",
                "GENERAL.CONNECTION",
                "device",
                "show",
                self._interface,
            )
        ).strip()
        if active_profile != self.config.ap_profile:
            await self._nmcli(
                "connection",
                "up",
                self.config.ap_profile,
                "ifname",
                self._interface,
            )
            self._activated_by_backend = True

        state = (
            await self._nmcli(
                "-g",
                "GENERAL.STATE",
                "device",
                "show",
                self._interface,
            )
        ).strip()
        if not state.startswith("100"):
            raise WifiBackendUnavailable(
                f"Wi-Fi interface {self._interface!r} is not connected: {state!r}"
            )

        network = await self._read_ipv4_configuration()
        expected = self.config.expected_ap_cidr
        if expected is not None and ipaddress.ip_interface(network.cidr) != ipaddress.ip_interface(expected):
            raise WifiBackendUnavailable(
                f"Sensor AP address is {network.cidr}, expected {expected}"
            )
        return network

    async def begin_provisioning(self) -> list[WifiAccessPoint]:
        """Release the AP radio and return visible access points."""

        if not self._initialized:
            raise WifiBackendUnavailable(
                "The linux-networkmanager Wi-Fi backend is not initialized"
            )
        await self._nmcli("connection", "down", self.config.ap_profile)
        self._provisioning_active = True
        deadline = asyncio.get_running_loop().time() + self.config.discovery_timeout_s
        access_points: list[WifiAccessPoint] = []
        while asyncio.get_running_loop().time() < deadline:
            output = await self._nmcli(
                "-t",
                "-f",
                "SSID,SIGNAL,FREQ,SECURITY",
                "device",
                "wifi",
                "list",
                "ifname",
                self._interface,
                "--rescan",
                "yes",
            )
            access_points = self._parse_access_points(output)
            if access_points:
                break
            await asyncio.sleep(1.0)
        return access_points

    @staticmethod
    def _parse_access_points(output: str) -> list[WifiAccessPoint]:
        access_points = []
        for index, line in enumerate(output.splitlines()):
            fields = line.split(":", 3)
            if len(fields) != 4 or not fields[0].strip():
                continue
            ssid, strength, frequency, security = fields
            try:
                strength_value = int(strength)
                # Older nmcli versions include the unit even in terse output
                # (for example, ``5180 MHz``).
                frequency_value = int(frequency.split()[0])
            except ValueError:
                continue
            access_points.append(
                WifiAccessPoint(
                    id=f"nmcli:{index}:{ssid}",
                    ssid=ssid,
                    bssid="",
                    strength=strength_value,
                    frequency_mhz=frequency_value,
                    secured=bool(security.strip() and security.strip() != "--"),
                )
            )
        return access_points

    async def connect_temporary(
        self,
        access_point: WifiAccessPoint,
    ) -> IPv4Configuration:
        """Connect the sensor radio to one open provisioning AP."""

        if access_point.secured:
            raise WifiBackendUnavailable(
                f"Provisioning AP {access_point.ssid!r} is unexpectedly secured"
            )
        profile = self.config.provisioning_profile
        await self._nmcli_allow_failure("connection", "delete", profile)
        await self._nmcli(
            "connection",
            "add",
            "type",
            "wifi",
            "ifname",
            self._interface,
            "con-name",
            profile,
            "ssid",
            access_point.ssid,
        )
        await self._nmcli(
            "connection",
            "modify",
            profile,
            "connection.autoconnect",
            "no",
            "802-11-wireless.mode",
            "infrastructure",
            "ipv4.method",
            "auto",
            "ipv4.never-default",
            "yes",
            "ipv6.method",
            "disabled",
        )
        await self._nmcli(
            "connection",
            "up",
            profile,
            "ifname",
            self._interface,
        )
        return await self._read_ipv4_configuration()

    async def restore_ap(self) -> IPv4Configuration:
        """Remove the temporary client profile and restore the Nexus AP."""

        profile = self.config.provisioning_profile
        await self._nmcli_allow_failure("connection", "down", profile)
        await self._nmcli_allow_failure("connection", "delete", profile)
        try:
            network = await self._activate_ap()
        except WifiBackendUnavailable as normal_error:
            if not self.config.allow_network_stack_restart:
                raise WifiBackendUnavailable(
                    "Normal sensor AP restoration failed after provisioning. "
                    "This adapter may require the proven network-stack recovery. "
                    "Run sudo -v and set "
                    "NEXUS_WIFI_ALLOW_NETWORK_STACK_RESTART=1 before retrying."
                ) from normal_error
            await self._restart_network_stack()
            network = await self._activate_ap()

        self._provisioning_active = False
        return network

    async def _activate_ap(self) -> IPv4Configuration:
        await self._nmcli(
            "connection",
            "up",
            self.config.ap_profile,
            "ifname",
            self._interface,
        )
        return await self.ensure_ap_active()

    async def _restart_network_stack(self) -> None:
        """Apply the opt-in mt76x2u station-to-AP recovery sequence."""

        await self._run_recovery_command(
            "sudo",
            "-n",
            "systemctl",
            "restart",
            "wpa_supplicant.service",
        )
        await asyncio.sleep(3.0)
        await self._run_recovery_command(
            "sudo",
            "-n",
            "systemctl",
            "restart",
            "NetworkManager.service",
        )
        await asyncio.sleep(5.0)
        await self._run_recovery_command(
            "sudo",
            "-n",
            "iw",
            "reg",
            "set",
            self.config.regulatory_domain,
        )
        await self._wait_for_networkmanager()

    async def _run_recovery_command(self, *command: str) -> None:
        try:
            result = await asyncio.wait_for(
                self._run_command(command),
                timeout=self.config.network_stack_restart_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise WifiBackendUnavailable(
                f"Network recovery command timed out: {' '.join(command)}"
            ) from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            raise WifiBackendUnavailable(
                f"Network recovery command failed ({' '.join(command)}): {detail}"
            )

    async def _wait_for_networkmanager(self) -> None:
        deadline = (
            asyncio.get_running_loop().time()
            + self.config.network_stack_restart_timeout_s
        )
        while asyncio.get_running_loop().time() < deadline:
            result = await self._nmcli_allow_failure(
                "-g",
                "GENERAL.STATE",
                "device",
                "show",
                self._interface,
            )
            if result.returncode == 0:
                return
            await asyncio.sleep(1.0)
        raise WifiBackendUnavailable(
            "NetworkManager did not return after network-stack recovery"
        )

    async def _read_ipv4_configuration(self) -> IPv4Configuration:
        address_value = self._first_value(
            await self._nmcli(
                "-g",
                "IP4.ADDRESS",
                "device",
                "show",
                self._interface,
            )
        )
        try:
            address = ipaddress.ip_interface(address_value)
        except ValueError as exc:
            raise WifiBackendUnavailable(
                f"NetworkManager returned an invalid IPv4 address: {address_value!r}"
            ) from exc
        if not isinstance(address, ipaddress.IPv4Interface):
            raise WifiBackendUnavailable("The sensor AP requires an IPv4 address")

        gateway = self._first_value(
            await self._nmcli(
                "-g",
                "IP4.GATEWAY",
                "device",
                "show",
                self._interface,
            ),
            required=False,
        )
        return IPv4Configuration(
            address=str(address.ip),
            prefix=address.network.prefixlen,
            gateway=gateway,
        )

    async def shutdown(self) -> None:
        # The saved sensor AP is host infrastructure.  Do not tear it down when
        # core stops, even if this backend had to reactivate it.
        self._initialized = False
        self._closed = True
        self._activated_by_backend = False
        self._provisioning_active = False

    @property
    def _interface(self) -> str:
        interface_name = (self.config.interface_name or "").strip()
        if not interface_name:
            raise WifiBackendUnavailable(
                "The linux-networkmanager backend requires a Wi-Fi interface"
            )
        return interface_name

    async def _nmcli(self, *arguments: str) -> str:
        command = ("nmcli", *arguments)
        result = await self._run_command(command)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            raise WifiBackendUnavailable(
                f"NetworkManager command failed ({' '.join(command)}): {detail}"
            )
        return result.stdout

    async def _nmcli_allow_failure(self, *arguments: str) -> _CommandResult:
        return await self._run_command(("nmcli", *arguments))

    @staticmethod
    async def _run_subprocess(command: Sequence[str]) -> _CommandResult:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return _CommandResult(
            returncode=process.returncode,
            stdout=stdout.decode(errors="replace"),
            stderr=stderr.decode(errors="replace"),
        )

    @staticmethod
    def _first_value(output: str, *, required: bool = True) -> str:
        values = [line.strip() for line in output.splitlines() if line.strip()]
        if values:
            return values[0]
        if required:
            raise WifiBackendUnavailable(
                "NetworkManager did not report an IPv4 address for the sensor AP"
            )
        return ""


__all__ = ["LinuxNetworkManagerBackend"]
