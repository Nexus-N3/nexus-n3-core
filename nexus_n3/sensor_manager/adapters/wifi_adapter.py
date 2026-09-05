"""SensorManager-facing, platform-neutral Wi-Fi adapter."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
import inspect
import time
from typing import Any, Callable, Iterable, Mapping

from nexus_n3.sensor_manager.connection_status import ConnectionStatus

from .wifi.backends.base import WifiBackend
from .wifi.backends.fake import FakeWifiBackend
from .wifi.config import WifiRuntimeConfig
from .wifi.errors import (
    WifiBackendUnavailable,
    WifiCandidateAmbiguous,
    WifiCandidateNotFound,
    WifiConnectionFailed,
    WifiDeviceNotDiscovered,
    WifiDiscoveryResultInvalid,
    WifiNotInitialized,
    WifiRequestedCountNotMet,
    WifiSensorDriverUnavailable,
    WifiShuttingDown,
)
from .wifi.models import (
    IPv4Configuration,
    NexusWifiNetwork,
    WifiAccessPoint,
    WifiAdvertisement,
    WifiCapabilities,
    WifiCredentials,
    WifiDevice,
    WifiProvisioningCandidate,
    WifiTransportHandle,
)
from .wifi.session import ExclusiveClientSession


@dataclass(frozen=True)
class _DiscoveredRecord:
    device: WifiDevice
    advertisement: WifiAdvertisement
    driver: Any
    group_key: tuple[str, str]


@dataclass(frozen=True)
class _RequestMember:
    sensor: Any | None
    driver: Any


@dataclass
class _RequestGroup:
    key: tuple[str, str]
    sensor_name: str
    members: list[_RequestMember] = field(default_factory=list)

    @property
    def requested_count(self) -> int:
        """Return the number of requested sensor instances in this group."""
        return len(self.members)

    @property
    def primary_driver(self) -> Any:
        """Return the driver used for group-level provisioning operations."""
        return self.members[0].driver


class WifiAdapter:
    """Shared Wi-Fi capability used by all SensorManager Wi-Fi sensors."""

    adapter_type = "WIFI"

    def __init__(
        self,
        config: WifiRuntimeConfig | None = None,
        backend: WifiBackend | None = None,
        drivers: Mapping[str, Any] | None = None,
    ) -> None:
        self.config = config or WifiRuntimeConfig.from_env()
        self.backend = backend or self._create_backend(self.config)
        self._drivers = dict(drivers or {})
        self._discovered: dict[str, _DiscoveredRecord] = {}
        self._identity_drivers: dict[tuple[tuple[str, str], str], Any] = {}
        self._connected_sensors: dict[str, Any] = {}
        self._network: IPv4Configuration | None = None
        self._initialized = False
        self._shutting_down = False
        self._operation_lock = asyncio.Lock()
        self.diagnostics_callback: Callable[[dict[str, Any]], None] | None = None
        self._diagnostic_counts: Counter[str] = Counter()
        self._diagnostic_errors: list[dict[str, Any]] = []
        self._diagnostic_started_ns = time.monotonic_ns()

    @staticmethod
    def _create_backend(config: WifiRuntimeConfig) -> WifiBackend:
        """Create the configured private platform backend."""
        if config.backend == "fake":
            return FakeWifiBackend()
        if config.backend == "linux-networkmanager":
            from .wifi.backends.linux_networkmanager import (
                LinuxNetworkManagerBackend,
            )

            return LinuxNetworkManagerBackend(config)
        raise WifiBackendUnavailable(
            f"Wi-Fi backend {config.backend!r} is not implemented"
        )

    @property
    def capabilities(self) -> WifiCapabilities:
        """Expose stable backend capabilities to adapter consumers."""
        return self.backend.capabilities

    @property
    def network(self) -> IPv4Configuration | None:
        """Return the active Nexus sensor network configuration."""
        return self._network

    @property
    def operation_lock(self) -> asyncio.Lock:
        """Return the lock serializing disruptive radio operations."""
        return self._operation_lock

    def register_driver(self, sensor_name: str, driver: Any) -> None:
        """Register a direct-development driver by sensor name."""
        name = str(sensor_name).strip()
        if not name:
            raise ValueError("Wi-Fi sensor name must not be empty")
        self._drivers[name] = driver

    def set_diagnostics_callback(
        self,
        callback: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        """Register the same structured diagnostics callback used by BLE."""

        self.diagnostics_callback = callback

    async def initialize(self) -> None:
        """Initialize the backend and validate or activate the Nexus AP."""
        if self._shutting_down:
            raise WifiShuttingDown("Wi-Fi adapter is shutting down")
        if self._initialized:
            return
        if not self.config.enabled:
            raise WifiBackendUnavailable("Wi-Fi sensor networking is disabled")
        self._diagnostic_counts["initialize_attempts"] += 1
        try:
            await self.backend.initialize()
            self._network = await self.backend.ensure_ap_active()
        except Exception as exc:
            self._record_diagnostic_error("initialize", exc)
            raise
        self._initialized = True
        self._diagnostic_counts["initialize_successes"] += 1

    async def discover_devices(
        self,
        requested: list[str] | list[Any],
        timeout: float | None = None,
    ) -> dict[str, tuple[WifiDevice, WifiAdvertisement]]:
        """Discover connected devices and provision each requested deficit."""

        self._ensure_ready()
        self._diagnostic_counts["discovery_attempts"] += 1
        timeout_s = timeout or self.config.discovery_timeout_s
        groups = self._resolve_groups(requested)
        if not groups:
            self._discovered = {}
            self._diagnostic_counts["discovery_successes"] += 1
            return {}

        devices_by_group = await self._discover_groups(groups, timeout_s)
        known_identities = {
            device.address
            for devices in devices_by_group.values()
            for device in devices
        }
        attempted_candidates: set[tuple[str, str]] = set()

        for group in groups:
            while len(devices_by_group[group.key]) < group.requested_count:
                if not self.config.ap_password or all(
                    member.sensor is None for member in group.members
                ):
                    break
                identity = await self._provision_one(
                    target_group=group,
                    all_groups=groups,
                    known_identities=known_identities,
                    attempted_candidates=attempted_candidates,
                )
                known_identities.add(identity)
                devices_by_group[group.key] = await self._wait_for_group_rejoin(
                    group,
                    expected_identity=identity,
                    expected_count=min(
                        group.requested_count,
                        len(devices_by_group[group.key]) + 1,
                    ),
                    timeout_s=timeout_s,
                )

        missing = [
            f"{group.sensor_name}:{group.requested_count - len(devices_by_group[group.key])}"
            for group in groups
            if len(devices_by_group[group.key]) < group.requested_count
            and any(member.sensor is not None for member in group.members)
            and self.config.ap_password
        ]
        if missing:
            raise WifiRequestedCountNotMet(
                "Wi-Fi requested count was not met: " + ", ".join(missing)
            )

        discovered = self._cache_discovered(groups, devices_by_group)
        self._diagnostic_counts["discovery_successes"] += 1
        self._diagnostic_counts["devices_discovered"] += len(discovered)
        return discovered

    async def _discover_groups(
        self,
        groups: list[_RequestGroup],
        timeout_s: float,
    ) -> dict[tuple[str, str], list[WifiDevice]]:
        """Discover connected devices for every requested driver group."""
        return {
            group.key: await self._discover_group(group, timeout_s)
            for group in groups
        }

    async def _discover_group(
        self,
        group: _RequestGroup,
        timeout_s: float,
    ) -> list[WifiDevice]:
        """Merge stable devices reported by the unique drivers in one group."""
        devices: dict[str, WifiDevice] = {}
        seen_drivers: set[int] = set()
        for member in group.members:
            if id(member.driver) in seen_drivers:
                continue
            seen_drivers.add(id(member.driver))
            raw_devices = await asyncio.wait_for(
                member.driver.discover_connected(self._network),
                timeout=timeout_s,
            )
            if not isinstance(raw_devices, Iterable):
                raise WifiDiscoveryResultInvalid(
                    f"Wi-Fi driver for {group.sensor_name!r} returned a non-iterable result"
                )
            for raw_device in raw_devices:
                device = self._normalize_device(raw_device)
                devices[device.address] = device
        return sorted(devices.values(), key=lambda item: item.address)

    async def _provision_one(
        self,
        *,
        target_group: _RequestGroup,
        all_groups: list[_RequestGroup],
        known_identities: set[str],
        attempted_candidates: set[tuple[str, str]],
    ) -> str:
        """Provision one missing identity through an exclusive client session."""
        self._diagnostic_counts["provisioning_attempts"] += 1
        session = ExclusiveClientSession(self._operation_lock, self.backend)
        try:
            async with session:
                claims: dict[
                    tuple[str, str],
                    list[tuple[_RequestGroup, WifiProvisioningCandidate]],
                ] = {}
                for group in all_groups:
                    classify = getattr(
                        group.primary_driver,
                        "classify_access_points",
                        None,
                    )
                    if not callable(classify):
                        continue
                    raw_claims = classify(session.access_points)
                    if inspect.isawaitable(raw_claims):
                        raw_claims = await raw_claims
                    for raw_candidate in raw_claims or []:
                        candidate = self._normalize_candidate(raw_candidate)
                        candidate_key = self._candidate_key(candidate.access_point)
                        claims.setdefault(candidate_key, []).append((group, candidate))

                for candidate_key, candidate_claims in claims.items():
                    claimed_groups = {claim[0].key for claim in candidate_claims}
                    if len(claimed_groups) > 1:
                        raise WifiCandidateAmbiguous(
                            "A provisioning access point was claimed by multiple drivers: "
                            f"{candidate_key[0]!r}"
                        )

                available = [
                    candidate
                    for candidate_key, candidate_claims in claims.items()
                    if candidate_key not in attempted_candidates
                    for group, candidate in candidate_claims
                    if group.key == target_group.key
                ]
                if not available:
                    scanned = ", ".join(
                        f"{access_point.ssid!r} "
                        f"({'secured' if access_point.secured else 'open'})"
                        for access_point in session.access_points[:20]
                    ) or "<none>"
                    raise WifiCandidateNotFound(
                        f"No provisioning AP was found for "
                        f"{target_group.sensor_name!r}; scanned APs: {scanned}"
                    )
                candidate = max(
                    available,
                    key=lambda item: (
                        item.access_point.strength,
                        item.confidence,
                        item.access_point.ssid,
                    ),
                )
                attempted_candidates.add(self._candidate_key(candidate.access_point))
                temporary_network = await session.connect_temporary(
                    candidate.access_point
                )

                identify = getattr(
                    target_group.primary_driver,
                    "identify_candidate",
                    None,
                )
                if not callable(identify):
                    raise WifiSensorDriverUnavailable(
                        "Wi-Fi sensor driver does not implement candidate identification"
                    )
                identified = identify(temporary_network)
                if inspect.isawaitable(identified):
                    identified = await asyncio.wait_for(
                        identified,
                        timeout=self.config.connect_timeout_s,
                    )
                identified_device = self._normalize_device(identified)
                if identified_device.address in known_identities:
                    raise WifiDiscoveryResultInvalid(
                        f"Wi-Fi identity {identified_device.address!r} is already assigned"
                    )

                provision = getattr(
                    target_group.primary_driver,
                    "provision",
                    None,
                )
                if not callable(provision):
                    raise WifiSensorDriverUnavailable(
                        "Wi-Fi sensor driver does not implement provisioning"
                    )
                target = NexusWifiNetwork(
                    ssid=self.config.ap_ssid,
                    credentials=WifiCredentials(password=self.config.ap_password),
                    channel=self.config.ap_channel,
                )
                provisioned = provision(
                    temporary_network,
                    target,
                    session.controls,
                )
                if inspect.isawaitable(provisioned):
                    provisioned = await asyncio.wait_for(
                        provisioned,
                        timeout=self.config.connect_timeout_s,
                    )
                if provisioned is not None:
                    provisioned_device = self._normalize_device(provisioned)
                    if provisioned_device.address != identified_device.address:
                        raise WifiDiscoveryResultInvalid(
                            "Provisioned Wi-Fi identity changed after identification"
                        )
                self._diagnostic_counts["provisioning_successes"] += 1
                return identified_device.address
        except Exception as exc:
            self._diagnostic_counts["provisioning_failures"] += 1
            self._record_diagnostic_error("provision", exc)
            raise
        finally:
            if session.restored_network is not None:
                self._network = session.restored_network

    async def _wait_for_group_rejoin(
        self,
        group: _RequestGroup,
        *,
        expected_identity: str,
        expected_count: int,
        timeout_s: float,
    ) -> list[WifiDevice]:
        """Poll announcements until the provisioned identity rejoins the AP."""
        deadline = (
            asyncio.get_running_loop().time()
            + self.config.provisioning_join_timeout_s
        )
        last_devices: list[WifiDevice] = []
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(1.0)
            last_devices = await self._discover_group(group, timeout_s)
            identities = {device.address for device in last_devices}
            if expected_identity in identities and len(last_devices) >= expected_count:
                return last_devices
        raise WifiRequestedCountNotMet(
            f"Wi-Fi sensor {expected_identity!r} did not rejoin "
            f"{self.config.ap_ssid!r}"
        )

    def _cache_discovered(
        self,
        groups: list[_RequestGroup],
        devices_by_group: dict[tuple[str, str], list[WifiDevice]],
    ) -> dict[str, tuple[WifiDevice, WifiAdvertisement]]:
        """Cache discovered identities and build the legacy discovery shape."""
        records: dict[str, _DiscoveredRecord] = {}
        discovered: dict[str, tuple[WifiDevice, WifiAdvertisement]] = {}
        for group in groups:
            devices = devices_by_group[group.key]
            assigned_driver_ids: set[int] = set()
            for device in devices:
                driver = self._driver_for_identity(
                    group,
                    device.address,
                    assigned_driver_ids,
                )
                assigned_driver_ids.add(id(driver))
                existing = records.get(device.address)
                if existing is not None and existing.group_key != group.key:
                    raise WifiDiscoveryResultInvalid(
                        f"Wi-Fi identity {device.address!r} was claimed by multiple drivers"
                    )
                advertisement = WifiAdvertisement(local_name=group.sensor_name)
                record = _DiscoveredRecord(
                    device=device,
                    advertisement=advertisement,
                    driver=driver,
                    group_key=group.key,
                )
                records[device.address] = record
                discovered[device.address] = (device, advertisement)
                self._identity_drivers[(group.key, device.address)] = driver
        self._discovered = records
        return discovered

    def _driver_for_identity(
        self,
        group: _RequestGroup,
        address: str,
        assigned_driver_ids: set[int],
    ) -> Any:
        """Choose or reuse the plugin instance owning a stable identity."""
        for member in group.members:
            if (
                member.sensor is not None
                and str(getattr(member.sensor, "address", "") or "") == address
            ):
                return member.driver
        cached = self._identity_drivers.get((group.key, address))
        if cached is not None:
            return cached
        for member in group.members:
            if id(member.driver) not in assigned_driver_ids:
                return member.driver
        return group.primary_driver

    def _resolve_groups(
        self,
        requested: list[str] | list[Any],
    ) -> list[_RequestGroup]:
        """Resolve requested names or sensor instances into plugin groups."""
        groups: dict[tuple[str, str], _RequestGroup] = {}
        for item in requested:
            if isinstance(item, str):
                sensor = None
                sensor_name = item.strip()
                driver = self._drivers.get(sensor_name)
                plugin_id = f"registered:{sensor_name}"
            else:
                sensor = item
                sensor_name = str(getattr(item, "name", "")).strip()
                driver = self._driver_for_sensor(item)
                plugin_id = str(
                    getattr(item, "plugin_id", "")
                    or getattr(driver, "plugin_id", "")
                    or (
                        f"{driver.__class__.__module__}."
                        f"{driver.__class__.__qualname__}"
                        if driver is not None
                        else ""
                    )
                )
                if sensor_name and driver is not None:
                    self._drivers.setdefault(sensor_name, driver)
            if not sensor_name or driver is None:
                raise WifiSensorDriverUnavailable(
                    f"No Wi-Fi sensor driver is available for {sensor_name or item!r}"
                )
            key = (plugin_id, sensor_name)
            group = groups.setdefault(
                key,
                _RequestGroup(key=key, sensor_name=sensor_name),
            )
            group.members.append(_RequestMember(sensor=sensor, driver=driver))
        return list(groups.values())

    @staticmethod
    def _normalize_device(device: Any) -> WifiDevice:
        """Validate and normalize a driver-provided device value."""
        if isinstance(device, Mapping):
            device = WifiDevice(
                address=str(device.get("address") or ""),
                endpoint=device.get("endpoint"),
                metadata=dict(device.get("metadata") or {}),
            )
        if not isinstance(device, WifiDevice) or not device.address:
            raise WifiDiscoveryResultInvalid(
                "Wi-Fi sensor driver returned an invalid device"
            )
        return device

    @staticmethod
    def _normalize_candidate(candidate: Any) -> WifiProvisioningCandidate:
        """Validate and normalize a driver provisioning candidate."""
        if isinstance(candidate, WifiProvisioningCandidate):
            return candidate
        if not isinstance(candidate, Mapping):
            raise WifiDiscoveryResultInvalid(
                "Wi-Fi provisioning candidate must be a mapping"
            )
        raw_access_point = candidate.get("access_point")
        if isinstance(raw_access_point, Mapping):
            raw_access_point = WifiAccessPoint(
                id=str(raw_access_point.get("id") or ""),
                ssid=str(raw_access_point.get("ssid") or ""),
                bssid=str(raw_access_point.get("bssid") or ""),
                strength=int(raw_access_point.get("strength") or 0),
                frequency_mhz=int(raw_access_point.get("frequency_mhz") or 0),
                secured=bool(raw_access_point.get("secured", False)),
            )
        if not isinstance(raw_access_point, WifiAccessPoint):
            raise WifiDiscoveryResultInvalid(
                "Wi-Fi provisioning candidate is missing an access point"
            )
        return WifiProvisioningCandidate(
            access_point=raw_access_point,
            confidence=int(candidate.get("confidence") or 0),
            metadata=dict(candidate.get("metadata") or {}),
        )

    @staticmethod
    def _candidate_key(access_point: WifiAccessPoint) -> tuple[str, str]:
        """Build a stable scan-local key for a provisioning access point."""
        return (access_point.ssid, access_point.bssid or access_point.id)

    def create_transport_client(
        self,
        address: str,
        loop=None,
        disconnected_callback=None,
    ) -> WifiTransportHandle:
        """Create a sensor transport handle from the discovery cache."""
        self._ensure_ready()
        record = self._discovered.get(address)
        if record is None:
            raise WifiDeviceNotDiscovered(
                f"Wi-Fi device {address!r} was not returned by discovery"
            )
        return WifiTransportHandle(
            address=address,
            device=record.device,
            driver=record.driver,
            disconnected_callback=disconnected_callback,
        )

    async def connect_to_device(
        self,
        sensor,
        adapter=None,
        timeout: float = 10,
    ) -> bool:
        """Ask the owning plugin driver to establish its vendor connection."""
        self._ensure_ready()
        self._diagnostic_counts["connect_attempts"] += 1
        handle = getattr(sensor, "transport_client", None)
        if not isinstance(handle, WifiTransportHandle):
            raise WifiConnectionFailed(
                f"Sensor {getattr(sensor, 'name', '<unknown>')!r} "
                "does not have a Wi-Fi transport handle"
            )
        try:
            connection = await asyncio.wait_for(
                handle.driver.connect_sensor(sensor, handle.device, adapter or self),
                timeout=timeout,
            )
        except Exception as exc:
            self._diagnostic_counts["connect_failures"] += 1
            self._record_diagnostic_error("connect", exc, address=handle.address)
            raise WifiConnectionFailed(
                f"Failed to connect Wi-Fi sensor {handle.address!r}"
            ) from exc
        if connection is None or connection is False:
            self._diagnostic_counts["connect_failures"] += 1
            return False
        handle.connection = None if connection is True else connection
        handle.is_connected = True
        sensor.set_connection_status(ConnectionStatus.CONNECTED)
        self._connected_sensors[handle.address] = sensor
        self._diagnostic_counts["connect_successes"] += 1
        return True

    async def connect_all(self, sensors, adapter=None, timeout: float = 10) -> bool:
        """Connect each supplied Wi-Fi sensor sequentially."""
        results = []
        for sensor in sensors:
            if not getattr(sensor, "address", None) or not getattr(
                sensor, "transport_client", None
            ):
                results.append(False)
                continue
            results.append(
                await self.connect_to_device(sensor, adapter or self, timeout=timeout)
            )
        return all(results)

    async def disconnect_sensor(self, sensor) -> bool:
        """Disconnect one sensor while leaving the shared Nexus AP active."""
        self._ensure_ready()
        return await self._disconnect_sensor(sensor)

    async def _disconnect_sensor(self, sensor) -> bool:
        """Perform plugin disconnect and clear the cached connection state."""
        handle = getattr(sensor, "transport_client", None)
        if not isinstance(handle, WifiTransportHandle):
            return False
        self._diagnostic_counts["disconnect_attempts"] += 1
        try:
            await handle.driver.disconnect_sensor(sensor)
        except Exception as exc:
            self._diagnostic_counts["disconnect_failures"] += 1
            self._record_diagnostic_error("disconnect", exc, address=handle.address)
            raise WifiConnectionFailed(
                f"Failed to disconnect Wi-Fi sensor {handle.address!r}"
            ) from exc
        handle.connection = None
        handle.is_connected = False
        sensor.set_connection_status(ConnectionStatus.DISCONNECTED)
        self._connected_sensors.pop(handle.address, None)
        self._diagnostic_counts["disconnect_successes"] += 1
        # ConnectionService emits the aggregate on_disconnected event for an
        # intentional disconnect.  The transport callback is reserved for an
        # unexpected link loss; invoking it here produces a duplicate event.
        return True

    async def shutdown(self) -> None:
        """Disconnect sensors and shut down the shared platform backend."""
        if self._shutting_down:
            return
        self._shutting_down = True
        try:
            for sensor in list(self._connected_sensors.values()):
                try:
                    await self._disconnect_sensor(sensor)
                except Exception:
                    pass
            await self.backend.shutdown()
        finally:
            self._connected_sensors.clear()
            self._discovered.clear()
            self._network = None
            self._initialized = False

    async def reset_session_diagnostics(self) -> None:
        """Reset adapter, backend, and sensor-plugin session counters."""

        self._diagnostic_counts.clear()
        self._diagnostic_errors.clear()
        self._diagnostic_started_ns = time.monotonic_ns()
        await self._call_optional_diagnostic_method(
            self.backend,
            "reset_session_diagnostics",
        )
        for driver in self._unique_drivers():
            try:
                await self._call_optional_diagnostic_method(
                    driver,
                    "reset_session_diagnostics",
                )
            except Exception as exc:
                self._record_diagnostic_error("reset_plugin_diagnostics", exc)

    async def get_diagnostics_snapshot(self) -> dict[str, Any]:
        """Return backend, lifecycle, and per-sensor Wi-Fi diagnostics."""

        backend_snapshot = await self._optional_diagnostics_snapshot(self.backend)
        sensor_snapshots: dict[str, Any] = {}
        labels_by_driver = self._driver_labels()
        for driver in self._unique_drivers():
            label = labels_by_driver.get(id(driver), driver.__class__.__name__)
            try:
                snapshot = await self._optional_diagnostics_snapshot(driver)
            except Exception as exc:
                self._record_diagnostic_error(
                    "collect_plugin_diagnostics",
                    exc,
                    address=label,
                )
                snapshot = {"error": f"{type(exc).__name__}: {exc}"}
            if snapshot:
                sensor_snapshots[label] = snapshot

        network = None
        if self._network is not None:
            network = {
                "address": self._network.address,
                "prefix": self._network.prefix,
                "gateway": self._network.gateway,
                "cidr": self._network.cidr,
            }
        capabilities = {
            name: getattr(self.capabilities, name)
            for name in (
                "ap_hosting",
                "scan_while_hosting",
                "temporary_profiles",
                "associated_client_reporting",
                "backend_recovery",
            )
        }
        payload = {
            "event": "wifi_status_snapshot",
            "adapter": {
                "initialized": self._initialized,
                "shutting_down": self._shutting_down,
                "network": network,
                "discovered_addresses": sorted(self._discovered),
                "connected_addresses": sorted(self._connected_sensors),
                "capabilities": capabilities,
                "session_elapsed_s": round(
                    (time.monotonic_ns() - self._diagnostic_started_ns)
                    / 1_000_000_000,
                    3,
                ),
                "counters": dict(self._diagnostic_counts),
                "errors": list(self._diagnostic_errors),
            },
            "backend": backend_snapshot,
            "sensors": sensor_snapshots,
        }
        if self.diagnostics_callback:
            self.diagnostics_callback(payload)
        return payload

    def _unique_drivers(self) -> list[Any]:
        """Return each currently known plugin driver exactly once."""
        drivers = [
            *self._drivers.values(),
            *(record.driver for record in self._discovered.values()),
            *self._identity_drivers.values(),
        ]
        unique: list[Any] = []
        seen: set[int] = set()
        for driver in drivers:
            if id(driver) in seen:
                continue
            seen.add(id(driver))
            unique.append(driver)
        return unique

    def _driver_labels(self) -> dict[int, str]:
        """Map driver identities to stable sensor addresses when available."""
        labels = {
            id(record.driver): address
            for address, record in self._discovered.items()
        }
        for name, driver in self._drivers.items():
            labels.setdefault(id(driver), name)
        return labels

    @staticmethod
    async def _call_optional_diagnostic_method(target, method_name: str) -> Any:
        """Call an optional synchronous or asynchronous diagnostics method."""
        method = getattr(target, method_name, None)
        if not callable(method):
            return None
        result = method()
        if inspect.isawaitable(result):
            result = await result
        return result

    @classmethod
    async def _optional_diagnostics_snapshot(cls, target) -> dict[str, Any]:
        """Normalize an optional diagnostics result to a dictionary."""
        result = await cls._call_optional_diagnostic_method(
            target,
            "get_diagnostics_snapshot",
        )
        return dict(result or {})

    def _record_diagnostic_error(
        self,
        operation: str,
        exc: Exception,
        *,
        address: str | None = None,
    ) -> None:
        """Append a sanitized error to the bounded diagnostics history."""
        entry = {
            "operation": operation,
            "type": type(exc).__name__,
            "message": str(exc),
        }
        if address:
            entry["address"] = address
        self._diagnostic_errors.append(entry)
        del self._diagnostic_errors[:-20]

    @staticmethod
    def _driver_for_sensor(sensor) -> Any:
        """Resolve the Wi-Fi driver exposed by a sensor instance."""
        getter = getattr(sensor, "get_wifi_driver", None)
        if callable(getter):
            return getter()
        return getattr(sensor, "wifi_driver", None)

    def _ensure_ready(self) -> None:
        """Reject operations before initialization or during shutdown."""
        if self._shutting_down:
            raise WifiShuttingDown("Wi-Fi adapter is shutting down")
        if not self._initialized:
            raise WifiNotInitialized("Wi-Fi adapter is not initialized")


WiFiAdapter = WifiAdapter

__all__ = ["WiFiAdapter", "WifiAdapter"]
