import json

from nexus_n3.robots.adapters.wave_rover import WaveRoverAdapter
from nexus_n3.robots.models.commands import MotionAction, MotionCommand
from nexus_n3.robots.protocols.waveshare_uart import WaveshareUartProtocol
from nexus_n3.robots.transports.mock import MockTransport


def make_adapter(stop_on_disconnect_ms: int = 0):
    transport = MockTransport()
    protocol = WaveshareUartProtocol()

    adapter = WaveRoverAdapter(
        transport=transport,
        protocol=protocol,
        robot_id="rover-01",
        stop_on_disconnect_ms=stop_on_disconnect_ms,
    )

    adapter.connect()
    return adapter, transport


def test_wave_rover_adapter_sends_forward_command():
    adapter, transport = make_adapter()

    adapter.handle_motion(MotionCommand(action=MotionAction.FWD, speed=0.5))

    assert len(transport.sent_payloads) == 1

    payload = transport.sent_payloads[0].decode("utf-8").strip()
    assert json.loads(payload) == {"T": 1, "L": 0.25, "R": 0.25}


def test_wave_rover_adapter_sends_left_command():
    adapter, transport = make_adapter()

    adapter.handle_motion(MotionCommand(action=MotionAction.LEFT, speed=0.25))

    payload = transport.sent_payloads[0].decode("utf-8").strip()
    assert json.loads(payload) == {"T": 1, "L": -0.125, "R": 0.125}


def test_wave_rover_adapter_stop_sends_zero_speed():
    adapter, transport = make_adapter()

    adapter.stop()

    payload = transport.sent_payloads[0].decode("utf-8").strip()
    assert json.loads(payload) == {"T": 1, "L": 0.0, "R": 0.0}


def test_wave_rover_adapter_poll_decodes_feedback():
    adapter, transport = make_adapter()

    transport.queue_rx(b'{"T":126,"heading":90}\n')

    result = adapter.poll()

    assert result == {"T": 126, "heading": 90}
