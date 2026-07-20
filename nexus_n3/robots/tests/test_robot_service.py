import json
from pathlib import Path

from nexus_n3.robots.config.schema import RobotModuleConfig
from nexus_n3.robots.config.loader import load_robot_config
from nexus_n3.robots.runtime.factory import build_robot
from nexus_n3.robots.runtime.service import RobotService


def make_service():
    config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    config = load_robot_config(config_path)
    #config = RobotModuleConfig(
    #    raw={
    #        "is_robot": True,
    #        "robot": {"id": "rover-01", "type": "wave_rover"},
    #        "transport": {"type": "mock"},
    #        "protocol": {"type": "waveshare_uart"},
    #        "motion": {"stop_on_disconnect_ms": 1000},
    #    }
    #)

    robot = build_robot(config)
    service = RobotService(robot)
    service.start()

    return service, robot


def test_service_motion_message_reaches_mock_transport():
    service, robot = make_service()

    service.on_message({
        "type": "cmd_motion",
        "action": "FWD",
        "speed": 0.5,
        "robot_id": "rover-01",
        "source": "test",
    })

    payload = robot.transport.sent_payloads[0].decode("utf-8").strip()

    assert json.loads(payload) == {"T": 1, "L": 0.25, "R": 0.25}


def test_service_stop_message_reaches_mock_transport():
    service, robot = make_service()

    service.on_message({"type": "cmd_stop"})

    payload = robot.transport.sent_payloads[0].decode("utf-8").strip()

    assert json.loads(payload) == {"T": 1, "L": 0.0, "R": 0.0}


def test_service_ignores_messages_when_not_robot():
    service = RobotService(robot=None)

    service.start()
    service.on_message({
        "type": "cmd_motion",
        "action": "FWD",
        "speed": 1.0,
    })
    service.tick()
    service.shutdown()

    assert service.robot is None
