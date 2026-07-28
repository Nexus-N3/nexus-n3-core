from unittest.mock import Mock
from pathlib import Path
from types import SimpleNamespace

from nexus_n3.plugins.runtime.sensor_runtime import (
    InstalledSensorPlugin,
    InstalledSensorProxy,
)

from nexus_n3.core.subject import Subject
from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


class DummySensor(InstalledSensorProxy):
    sensor_type = SimpleNamespace(local_name="Dummy Sensor")

    _plugin = InstalledSensorPlugin(
        plugin_id="dummy-sensor",
        sensor_name="Dummy Sensor",
        install_path=Path("/unused"),
        runtime_path=Path("/unused"),
        manifest={
            "inputs": [],
            "outputs": [],
        },
        metadata={
            "sensor": {
                "name": "Dummy Sensor",
                "adapter": "BLE",
            },
            "events": ["on_data", "on_error"],
            "locations": {
                "supported": ["CHEST", "LEFT_ANKLE"],
            },
            "data_streams": {},
        },
    )
    # this removes the need for sensor = DummySensor(None)
    def __init__(self):
        super().__init__(None)

class DummyBLEAdapter:
    def close(self):
        pass


def test_init_sensor_manager_accepts_system_sensor_input(monkeypatch):
    # Prevent hardware and background-thread activity.
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.platform.system",
        lambda: "Darwin",
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.threading.Thread",
        Mock(return_value=Mock()),
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class",
        lambda adapter_type, ble_runtime_config=None: DummyBLEAdapter,
    )

    manager = SensorManager(
        ble_runtime_config=BLERuntimeConfig(backend="bleak")
    )

    try:
        # Build the input in the same way SubjectGraph does.
        subject = Subject(
            subject_id="subject-1",
            sensor_configs=[],
        )
        sensor = DummySensor()

        metadata = {
            "location": "CHEST",
            "f_block_size": 300,
            "compute_algorithm": {
                "name": "pass_through",
                "inputs": {},
            },
        }

        subject.add_sensor(sensor, meta_data=metadata)

        # This is the same flattening performed by Core._init_sensor_manager().
        sensors_to_init = [
            entry
            for current_subject in [subject]
            for entry in current_subject.sensors
        ]

        manager.init_sensor_manager(sensors_to_init)

        assert sensor._manager_loop is manager.loop

        # The sensor was extracted from the system input.
        assert manager.sensors == [sensor]

        # Its production metadata was preserved.
        assert manager.sensor_meta[sensor] == metadata
        assert manager.sensor_meta[sensor]["f_block_size"] == 300
        assert (
            manager.sensor_meta[sensor]["compute_algorithm"]["name"]
            == "pass_through"
        )

        # Location metadata was applied to the actual sensor.
        assert sensor.location == "CHEST"

        # The required adapter was created.
        assert "BLE" in manager.adapters
        assert isinstance(manager.adapters["BLE"], DummyBLEAdapter)

        # Manager callbacks were registered on the sensor.
        assert callable(sensor.listeners["on_data"])
        assert callable(sensor.listeners["on_error"])

        # No routes should exist for a sensor with no declared outputs.
        assert manager.routing_table == {}

    finally:
        manager.running = False
        manager.adapter_pool.close_all()
        manager.loop.close()