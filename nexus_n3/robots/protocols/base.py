from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from nexus_n3.robots.models.commands import MotionCommand


class RobotWireProtocol(ABC):
    @abstractmethod
    def encode_motion(self, cmd: MotionCommand) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def decode(self, payload: bytes) -> dict[str, Any]:
        raise NotImplementedError