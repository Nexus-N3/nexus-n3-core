from unittest.mock import Mock

from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


def test_sensor_manager_init(monkeypatch):
    # Prevent Linux-specific Bluetooth cleanup.
    remove_devices = Mock()
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.utils.remove_all_devices",
        remove_devices,
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.platform.system",
        lambda: "Linux",
    )

    # Prevent real background threads from starting.
    loop_thread = Mock()
    rate_thread = Mock()
    thread_factory = Mock(side_effect=[loop_thread, rate_thread])
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.threading.Thread",
        thread_factory,
    )

    event_bus = Mock()
    error_callback = Mock()
    config = BLERuntimeConfig(backend="bleak")

    manager = SensorManager(
        system_event_bus=event_bus,
        error_cb=error_callback,
        ble_runtime_config=config,
    )

    try:
        assert manager.system_event_bus is event_bus
        assert manager.ble_runtime_config is config
        assert manager.listeners["on_error"] is error_callback

        assert manager.sensors == []
        assert manager.sensor_meta == {}
        assert manager.routing_table == {}
        assert manager.adapters == {}
        assert manager.running is True

        remove_devices.assert_called_once_with()
        assert thread_factory.call_count == 2
        loop_thread.start.assert_called_once_with()
        rate_thread.start.assert_called_once_with()
    finally:
        # Threads were mocked, so close the event loop directly.
        manager.running = False
        manager.loop.close()