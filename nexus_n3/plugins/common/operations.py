"""Attempt identity propagated through plugin RPC and adapter worker threads."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import threading
import time
import uuid


@dataclass
class SensorOperation:
    deadline: float
    operation_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    cancelled: threading.Event = field(default_factory=threading.Event)
    adapter_write_unconfirmed: bool = False
    adapter_operation_confirmed: bool = False

    def check(self):
        if self.cancelled.is_set() or time.monotonic() >= self.deadline:
            raise TimeoutError("Sensor operation expired or cancelled")

    def wire(self):
        return {"id": self.operation_id, "deadline": self.deadline}


current_operation = ContextVar("sensor_operation", default=None)


@contextmanager
def operation_scope(operation):
    token = current_operation.set(operation)
    try:
        yield
    finally:
        current_operation.reset(token)
