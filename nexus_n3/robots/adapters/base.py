from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any


# connects commands -> protocol -> transport

from nexus_n3.robots.models.commands import MotionCommand
from nexus_n3.robots.transports.base import Transport
from nexus_n3.robots.protocols.base import RobotWireProtocol


# adapter definition for specific robot implementations.
# this interface defines what every robot should do.
class RobotAdapter(ABC):
    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def handle_motion(self, cmd: MotionCommand) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def tick(self) -> None:
        raise NotImplementedError


class BaseRobotAdapter(RobotAdapter):
    def __init__(
        self,
        *,
        transport: Transport,
        protocol: RobotWireProtocol,
        robot_id: str,
        stop_on_disconnect_ms: int = 0,
    ) -> None:
        self.transport = transport
        self.protocol = protocol
        self.robot_id = robot_id
        self.stop_on_disconnect_ms = stop_on_disconnect_ms

        self._last_motion_ts: float = 0.0

    def connect(self) -> None:
        self.transport.connect()

    def close(self) -> None:
        self.transport.close()

    def stop(self) -> None:
        self.handle_motion(MotionCommand.stop(robot_id=self.robot_id))

    
    def poll(self) -> dict[str, Any] | None:
        payload = self.transport.recv()
        if not payload:
            return None

        return self.protocol.decode(payload)

    #prevents runaway robots. 
    def tick(self) -> None:
        if self.stop_on_disconnect_ms <= 0:
            return

        if self._last_motion_ts == 0.0:
            return

        elapsed_ms = (time.monotonic() - self._last_motion_ts) * 1000

        if elapsed_ms >= self.stop_on_disconnect_ms:
            self.stop()
            self._last_motion_ts = 0.0

    def _mark_motion(self) -> None:
        self._last_motion_ts = time.monotonic()