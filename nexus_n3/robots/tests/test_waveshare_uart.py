from nexus_n3.robots.models.commands import MotionCommand, MotionAction
from nexus_n3.robots.protocols.waveshare_uart import WaveshareUartProtocol


def test_encode_forward():
    protocol = WaveshareUartProtocol()

    cmd = MotionCommand(action=MotionAction.FWD, speed=0.5)
    payload = protocol.encode_motion(cmd)

    assert payload == b'{"T":1,"L":0.25,"R":0.25}\n'


def test_encode_backward():
    protocol = WaveshareUartProtocol()

    cmd = MotionCommand(action=MotionAction.BWD, speed=0.5)
    payload = protocol.encode_motion(cmd)

    assert payload == b'{"T":1,"L":-0.25,"R":-0.25}\n'


def test_encode_left_turn():
    protocol = WaveshareUartProtocol()

    cmd = MotionCommand(action=MotionAction.LEFT, speed=0.3)
    payload = protocol.encode_motion(cmd)

    assert payload == b'{"T":1,"L":-0.15,"R":0.15}\n'


def test_encode_full_normalized_speed_maps_to_waveshare_max():
    protocol = WaveshareUartProtocol()

    cmd = MotionCommand(action=MotionAction.FWD, speed=1.0)
    payload = protocol.encode_motion(cmd)

    assert payload == b'{"T":1,"L":0.5,"R":0.5}\n'


def test_encode_stop():
    protocol = WaveshareUartProtocol()

    cmd = MotionCommand.stop()
    payload = protocol.encode_motion(cmd)

    assert payload == b'{"T":1,"L":0.0,"R":0.0}\n'


def test_decode_valid_json():
    protocol = WaveshareUartProtocol()

    result = protocol.decode(b'{"T":126,"heading":90}\n')

    assert result == {"T": 126, "heading": 90}


def test_decode_empty_returns_empty_dict():
    protocol = WaveshareUartProtocol()

    assert protocol.decode(b"") == {}
