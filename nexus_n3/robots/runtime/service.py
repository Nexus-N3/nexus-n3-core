from __future__ import annotations

from typing import Any

from nexus_n3.robots.models.commands import MotionCommand, MotionAction
from nexus_n3.robots.adapters.base import RobotAdapter


class RobotService:
    def __init__(self, robot: RobotAdapter | None) -> None:
        self.robot = robot
        self._running = False

    def start(self) -> None:
        if self.robot is None:
            return

        self.robot.connect()
        self._running = True

    def shutdown(self) -> None:
        if self.robot is None:
            return

        self.robot.close()
        self._running = False

    def status(self) -> dict[str, Any]:
        if self.robot is None:
            return {
                "supported": False,
                "running": False,
                "robot_id": None,
            }
        return {
            "supported": True,
            "running": self._running,
            "robot_id": getattr(self.robot, "robot_id", None),
        }

    def on_message(self, message: dict[str, Any]) -> None:
        if self.robot is None:
            return

        msg_type = message.get("type")

        if msg_type == "cmd_motion":
            cmd = self._parse_motion(message)
            self.robot.handle_motion(cmd)
            return

        if msg_type == "cmd_stop":
            self.robot.stop()
            return

        raise ValueError(f"Unsupported message type: {msg_type}")

    def poll_feedback(self) -> dict[str, Any] | None:
        if self.robot is None:
            return None

        return self.robot.poll()

    def tick(self) -> None:
        if self.robot is None:
            return

        self.robot.tick()

    def _parse_motion(self, message: dict[str, Any]) -> MotionCommand:
        action_raw = message.get("action")
        speed = float(message.get("speed", 0.0))

        try:
            action = MotionAction(action_raw)
        except Exception:
            raise ValueError(f"Invalid motion action: {action_raw}")

        return MotionCommand(
            action=action,
            speed=speed,
            robot_id=message.get("robot_id"),
            source=message.get("source"),
        )
