"""Regression coverage for subject-owned logical sensor discovery slots."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

from nexus_n3.sensor_manager.discovery_service import DiscoveryService
from nexus_n3.sensor_manager.types.connections import ConnectionStatus


class _Sensor:
    def __init__(self, address=None, status=ConnectionStatus.DISCONNECTED):
        self.name = "Movella DOT"
        self.address = address
        self.connection_status = status
        self.spec = {}


class _Adapter:
    def __init__(self, addresses):
        self.addresses = addresses

    async def discover_devices(self, _requested):
        return {
            address: (
                SimpleNamespace(address=address, name="Movella DOT"),
                SimpleNamespace(local_name="Movella DOT", service_uuids=[]),
            )
            for address in self.addresses
        }


class _AdapterPool:
    def __init__(self, adapter):
        self.adapter = adapter

    def group_sensors(self, sensors):
        return {self.adapter: sensors}

    @staticmethod
    def has_method(adapter, method):
        return callable(getattr(adapter, method, None))


def _discover(all_sensors, requested_sensors, addresses):
    async def scenario():
        events = []
        service = DiscoveryService(_AdapterPool(_Adapter(addresses)), Mock())
        discovered = await service.discover_for_subject(
            sensors=all_sensors,
            requested_sensors=requested_sensors,
            loop=asyncio.get_running_loop(),
            register_listeners_with_sensor=lambda sensor: None,
            emit_to_client=lambda event, payload: events.append((event, payload)),
        )
        return discovered, events

    return asyncio.run(scenario())


def test_subject_discovery_assigns_only_the_requested_logical_slots():
    subject_1 = [_Sensor(), _Sensor()]
    subject_2 = [_Sensor(), _Sensor()]

    discovered, events = _discover(
        all_sensors=subject_1 + subject_2,
        requested_sensors=subject_2,
        addresses=["AA", "BB", "CC", "DD"],
    )

    assert discovered == subject_2
    assert [sensor.address for sensor in subject_1] == [None, None]
    assert [sensor.address for sensor in subject_2] == ["AA", "BB"]
    assert events[-1] == ("on_discover", subject_2)


def test_subject_discovery_excludes_addresses_connected_to_another_subject():
    subject_1 = [
        _Sensor("AA", ConnectionStatus.CONNECTED),
        _Sensor("BB", ConnectionStatus.CONNECTED),
    ]
    subject_2 = [_Sensor(), _Sensor()]

    discovered, _events = _discover(
        all_sensors=subject_1 + subject_2,
        requested_sensors=subject_2,
        addresses=["AA", "BB", "CC", "DD"],
    )

    assert discovered == subject_2
    assert [sensor.address for sensor in subject_1] == ["AA", "BB"]
    assert [sensor.address for sensor in subject_2] == ["CC", "DD"]
