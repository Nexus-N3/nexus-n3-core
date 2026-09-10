from types import SimpleNamespace

from nexus_n3.plugins.runtime.sensor_host import SensorHost


class _RecordingConnection:
    def __init__(self):
        self.calls = []

    def request(self, method, params):
        self.calls.append((method, params))


def test_forward_event_uses_timing_captured_by_plugin_payload():
    connection = _RecordingConnection()
    host = object.__new__(SensorHost)
    host._adapter = SimpleNamespace(
        _connection=connection,
        current_notification_timing=lambda: {},
    )
    payload = SimpleNamespace(
        address="sensor-1",
        timestamp=123,
        sample_type="imu",
        _nexus_timing={"host_receive_monotonic_ns": 200},
    )

    host._forward_event("on_data", payload)

    method, params = connection.calls[0]
    assert method == "sensor.emit_event"
    assert params["timing"] == {"host_receive_monotonic_ns": 200}
    assert "_nexus_timing" not in params["payload"]
