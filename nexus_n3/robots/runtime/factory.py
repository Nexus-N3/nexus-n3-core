from __future__ import annotations

import importlib
from typing import Any

from nexus_n3.robots.config.schema import RobotModuleConfig
from nexus_n3.robots.adapters.registry import (
    ROBOT_TYPES,
    TRANSPORT_TYPES,
    PROTOCOL_TYPES,
)


def build_robot(config: RobotModuleConfig):
    if not config.is_robot:
        return None

    robot_cls = _load_symbol(ROBOT_TYPES[config.robot_type])
    transport_cls = _load_symbol(TRANSPORT_TYPES[config.transport_type])
    protocol_cls = _load_symbol(PROTOCOL_TYPES[config.protocol_type])

    transport_kwargs = _build_transport_kwargs(config)
    transport = transport_cls(**transport_kwargs)

    protocol = protocol_cls()

    robot = robot_cls(
        transport=transport,
        protocol=protocol,
        robot_id=config.robot_id,
        stop_on_disconnect_ms=int(
            config.get("motion.stop_on_disconnect_ms", 0) or 0
        ),
    )

    return robot


def _load_symbol(path: str) -> Any:
    module_name, class_name = path.split(":")
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _build_transport_kwargs(config: RobotModuleConfig) -> dict[str, Any]:
    transport_type = config.transport_type

    if transport_type == "uart":
        return {
            "port": config.get("transport.port"),
            "baudrate": int(config.get("transport.baudrate", 115200)),
            "timeout": float(config.get("transport.timeout", 1.0)),
        }

    if transport_type == "mock":
        return {}

    raise ValueError(f"Unsupported transport type: {transport_type}")