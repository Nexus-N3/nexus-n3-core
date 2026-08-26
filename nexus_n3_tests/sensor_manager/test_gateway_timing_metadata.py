from pathlib import Path
import sys
from types import SimpleNamespace

CORE_ROOT = Path(__file__).resolve().parents[2]
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from nexus_n3.sensor_manager.adapters.gateway_ble_adapter import GatewayBLEAdapter
from nexus_n3.sensor_manager.adapters.gateway_ble_client import StreamFrame


def test_binary_gateway_frame_preserves_gateway_and_host_timestamps():
    observed = []

    def callback(sender, payload, timing):
        observed.append((sender, payload, timing))

    transport = SimpleNamespace(
        sensor_id=7,
        binary_notify_uuid="imu-data",
        notify_callbacks={"imu-data": callback},
    )
    adapter = GatewayBLEAdapter.__new__(GatewayBLEAdapter)
    adapter.transport_clients = {"sensor-1": transport}

    adapter._handle_stream_frame(
        StreamFrame(sensor_id=7, gateway_timestamp_us=123456, payload=b"sample")
    )

    assert len(observed) == 1
    sender, payload, timing = observed[0]
    assert sender == "imu-data"
    assert payload == b"sample"
    assert timing["gateway_timestamp_us"] == 123456
    assert timing["host_receive_monotonic_ns"] > 0
