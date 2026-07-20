"""Serialization helpers for plugin runtime transport."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
from types import SimpleNamespace
from typing import Any


def to_jsonable(value: Any) -> Any:
    """Convert nested dataclass/object values into JSON-safe structures."""
    if is_dataclass(value):
        return {key: to_jsonable(val) for key, val in asdict(value).items()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): to_jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "__dict__"):
        return {
            key: to_jsonable(val)
            for key, val in vars(value).items()
            if not key.startswith("_")
        }
    return value


def object_to_mapping(value: Any) -> dict[str, Any]:
    """Return a mapping view over a result-like value."""
    if isinstance(value, dict):
        return value
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "__dict__"):
        return {key: val for key, val in vars(value).items() if not key.startswith("_")}
    raise TypeError(f"unsupported result value: {type(value)!r}")


def deep_namespace(value: Any) -> Any:
    """Convert dict/list trees into attribute-accessible namespaces."""
    if isinstance(value, dict):
        return SimpleNamespace(**{key: deep_namespace(val) for key, val in value.items()})
    if isinstance(value, list):
        return [deep_namespace(item) for item in value]
    return value


class RemoteComputeResult:
    """Runtime result wrapper for host-backed algorithm outputs."""

    def __init__(self, payload: dict[str, Any]):
        self._payload = payload
        for key, value in payload.items():
            setattr(self, key, deep_namespace(value))

    def to_dict(self) -> dict[str, Any]:
        return dict(self._payload)
