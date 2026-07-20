from __future__ import annotations

import json
from typing import Any

from nexus_n3.robots.models.commands import MotionCommand, MotionAction
from nexus_n3.robots.protocols.base import RobotWireProtocol


WAVE_ROVER_MAX_WHEEL_SPEED = 0.5


class WaveshareUartProtocol(RobotWireProtocol):
    def encode_motion(self, cmd: MotionCommand) -> bytes:
        left, right = self._action_to_wheels(cmd)

        message = {
            "T": 1,
            "L": left,
            "R": right,
        }

        return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")

    def decode(self, payload: bytes) -> dict[str, Any]:
        text = payload.decode("utf-8").strip()

        if not text:
            return {}

        return json.loads(text)

    def _action_to_wheels(self, cmd: MotionCommand) -> tuple[float, float]:
        speed = cmd.normalized_speed() * WAVE_ROVER_MAX_WHEEL_SPEED

        if cmd.action == MotionAction.FWD:
            return speed, speed

        if cmd.action == MotionAction.BWD:
            return -speed, -speed

        if cmd.action == MotionAction.LEFT:
            return -speed, speed

        if cmd.action == MotionAction.RIGHT:
            return speed, -speed

        if cmd.action == MotionAction.STOP:
            return 0.0, 0.0

        raise ValueError(f"Unsupported motion action: {cmd.action}")
