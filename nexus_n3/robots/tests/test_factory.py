from nexus_n3.robots.config.schema import RobotModuleConfig
from nexus_n3.robots.runtime.factory import build_robot
from nexus_n3.robots.adapters.wave_rover import WaveRoverAdapter
from nexus_n3.robots.transports.mock import MockTransport
from nexus_n3.robots.protocols.waveshare_uart import WaveshareUartProtocol


def test_factory_builds_wave_rover_with_mock_transport():
    config = RobotModuleConfig(
        raw={
            "is_robot": True,
            "robot": {"id": "rover-01", "type": "wave_rover"},
            "transport": {"type": "mock"},
            "protocol": {"type": "waveshare_uart"},
        }
    )

    robot = build_robot(config)

    assert isinstance(robot, WaveRoverAdapter)
    assert isinstance(robot.transport, MockTransport)
    assert isinstance(robot.protocol, WaveshareUartProtocol)


def test_factory_returns_none_when_not_robot():
    config = RobotModuleConfig(raw={"is_robot": False})

    robot = build_robot(config)

    assert robot is None