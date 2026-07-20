"""Distributed node capability helpers."""

from __future__ import annotations

from typing import Any


def build_node_capabilities(*, include_sensors: bool, include_algorithms: bool) -> dict[str, list[str]]:
    """Build a compact capability payload for distributed node registration."""
    from nexus_n3.plugins.runtime.discovery import get_supported_algorithms, get_supported_sensors

    supported_sensors: list[str] = []
    supported_algorithms: list[str] = []

    if include_sensors:
        supported_sensors = [
            str(sensor.get("name") or "").strip()
            for sensor in get_supported_sensors()
            if str(sensor.get("name") or "").strip()
        ]

    if include_algorithms:
        supported_algorithms = [
            str(name).strip()
            for name in get_supported_algorithms()
            if str(name).strip()
        ]

    return {
        "supported_sensors": sorted(set(supported_sensors), key=str.lower),
        "supported_algorithms": sorted(set(supported_algorithms), key=str.lower),
    }


def supports_sensor(capabilities: dict[str, Any] | None, sensor_name: str) -> bool:
    if not sensor_name:
        return False
    names = capabilities.get("supported_sensors", []) if isinstance(capabilities, dict) else []
    return _normalize_name(sensor_name) in {_normalize_name(name) for name in names}


def supports_algorithm(capabilities: dict[str, Any] | None, algorithm_name: str) -> bool:
    if not algorithm_name:
        return False
    names = capabilities.get("supported_algorithms", []) if isinstance(capabilities, dict) else []
    return _normalize_name(algorithm_name) in {_normalize_name(name) for name in names}


def _normalize_name(name: str) -> str:
    return str(name).strip().lower()
