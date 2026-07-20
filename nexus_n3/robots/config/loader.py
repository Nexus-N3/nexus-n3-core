from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from nexus_n3.core.runtime_env import load_runtime_env

# import the config schema
from nexus_n3.robots.config.schema import RobotModuleConfig


# ansible will render the file here for production
DEFAULT_PROD_CONFIG_PATH = Path("/etc/nexus-n3/robot.yaml")
ENV_CONFIG_PATH = "NEXUS_N3_ROBOT_CONFIG"


class RobotConfigError(Exception):
    pass


def load_robot_config(config_path: str | Path | None = None) -> RobotModuleConfig:
    path = _resolve_config_path(config_path)

    if not path.exists():
        raise RobotConfigError(f"Robot config file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    if not isinstance(raw, dict):
        raise RobotConfigError("Robot config must be a YAML object")

    config = RobotModuleConfig(raw=raw)
    validate_robot_config(config)
    return config


def validate_robot_config(config: RobotModuleConfig) -> None:
    if "is_robot" not in config.raw:
        raise RobotConfigError("Missing required field: is_robot")

    if not isinstance(config.raw["is_robot"], bool):
        raise RobotConfigError("Field is_robot must be true or false")

    if not config.is_robot:
        return

    _require(config.robot_id, "robot.id")
    _require(config.robot_type, "robot.type")
    _require(config.transport_type, "transport.type")
    _require(config.protocol_type, "protocol.type")

    # UART is the wave rover's communication transport - others could be supported here
    if config.transport_type == "uart":
        _require(config.get("transport.port"), "transport.port")
        _require(config.get("transport.baudrate"), "transport.baudrate")


def _resolve_config_path(config_path: str | Path | None = None) -> Path:
    if config_path is not None:
        return Path(config_path)

    load_runtime_env()
    env_path = os.getenv(ENV_CONFIG_PATH)
    if env_path:
        return Path(env_path)

    # config.yaml in the root of the project.
    local_dev_path = Path(__file__).resolve().parents[1] / "config.yaml"
    if local_dev_path.exists():
        return local_dev_path

    return DEFAULT_PROD_CONFIG_PATH


def _require(value: Any, field_name: str) -> None:
    if value is None or value == "":
        raise RobotConfigError(f"Missing required field: {field_name}")
