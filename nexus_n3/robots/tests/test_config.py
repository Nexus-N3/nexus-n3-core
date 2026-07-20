import pytest

from nexus_n3.robots.config.loader import (
    load_robot_config,
    validate_robot_config,
    RobotConfigError,
)
from nexus_n3.robots.config.schema import RobotModuleConfig


def test_non_robot_config_is_valid():
    config = RobotModuleConfig(raw={"is_robot": False})

    # should not raise
    validate_robot_config(config)

    assert config.is_robot is False


def test_robot_config_requires_fields():
    config = RobotModuleConfig(raw={"is_robot": True})

    with pytest.raises(RobotConfigError):
        validate_robot_config(config)


def test_valid_robot_config_passes():
    raw = {
        "is_robot": True,
        "robot": {"id": "rover-01", "type": "wave_rover"},
        "transport": {"type": "mock"},
        "protocol": {"type": "waveshare_uart"},
    }

    config = RobotModuleConfig(raw=raw)

    # should not raise
    validate_robot_config(config)

    assert config.robot_id == "rover-01"
    assert config.robot_type == "wave_rover"


def test_uart_transport_requires_port_and_baudrate():
    raw = {
        "is_robot": True,
        "robot": {"id": "rover-01", "type": "wave_rover"},
        "transport": {"type": "uart"},  # missing fields
        "protocol": {"type": "waveshare_uart"},
    }

    config = RobotModuleConfig(raw=raw)

    with pytest.raises(RobotConfigError):
        validate_robot_config(config)


def test_config_get_nested_value():
    raw = {
        "is_robot": True,
        "motion": {"stop_on_disconnect_ms": 1000},
    }

    config = RobotModuleConfig(raw=raw)

    assert config.get("motion.stop_on_disconnect_ms") == 1000


def test_config_get_missing_returns_default():
    config = RobotModuleConfig(raw={"is_robot": False})

    assert config.get("motion.missing", 123) == 123