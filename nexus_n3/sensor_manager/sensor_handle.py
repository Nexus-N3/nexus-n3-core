"""Common host-side sensor handle abstraction."""

from __future__ import annotations

import inspect
from pathlib import Path

from nexus_n3.plugins.runtime.spec_loader import load_yaml

from .connection_status import ConnectionStatus


class SensorBase:
    """
    Host-side sensor handle contract.

    Sensor plugins implement their own protocol/parsing logic, but the host still
    uses a common object shape for lifecycle, routing, and event wiring.
    """

    def __init__(self, sensor, spec: dict):
        self.address = None
        self.name = sensor.local_name
        self.spec = spec
        self.adapter = spec["sensor"]["adapter"]
        self.connection_status = ConnectionStatus.DISCONNECTED
        self.transport_client = None
        self.location = str
        self.capabilities = set(spec.get("capabilities", []))
        self.attributes = {k: v.get("default") for k, v in spec.get("attributes", {}).items()}
        self.listeners = {event: None for event in spec.get("events", [])}

    @classmethod
    def load_raw_spec(cls) -> dict:
        spec_path = getattr(cls, "SPEC_PATH", None)
        if not spec_path:
            raise ValueError(f"{cls.__name__} missing SPEC_PATH")
        path = Path(spec_path)
        if not path.is_absolute():
            module_dir = Path(inspect.getfile(cls)).parent
            candidate = module_dir / path
            if candidate.exists():
                path = candidate
        return load_yaml(path)

    def set_transport_client(self, client):
        self.transport_client = client

    def set_connection_status(self, status: ConnectionStatus):
        self.connection_status = status

    def get_connection_status(self):
        return self.connection_status

    def set_location(self, location: str):
        locations = self.spec.get("locations")
        if not locations:
            raise ValueError(f"Sensor '{self.name}' does not support body locations")

        supported = locations.get("supported", [])
        if location not in supported:
            self._emit("on_error", {"ERROR": f"Unsupported body location '{location}'. "})
            return None

        self.location = location

    def has_capability(self, capability: str) -> bool:
        return capability in self.capabilities

    def register_listener(self, event: str, callback):
        if event not in self.listeners:
            raise ValueError(f"Unsupported event: {event}")
        self.listeners[event] = callback

    def unregister_listener(self, event: str):
        if event in self.listeners:
            self.listeners[event] = None

    def _emit(self, event: str, payload: dict):
        cb = self.listeners.get(event)
        if cb:
            cb(payload)
