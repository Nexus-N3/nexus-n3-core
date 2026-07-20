from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# defines core motion actions 
class MotionAction(str, Enum):
    FWD = "FWD"
    BWD = "BWD"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    STOP = "STOP"

# the nexus-n3-gateway or azure bridge recieves 
'''
{
  "type": "cmd_motion",
  "action": "FWD",
  "speed": 0.5
}

'''


@dataclass(slots=True)
class MotionCommand:
    action: MotionAction
    speed: float
    robot_id: str | None = None
    source: str | None = None

    @classmethod
    def stop(
        cls,
        robot_id: str | None = None,
        source: str | None = None,
    ) -> "MotionCommand":
        return cls(
            action=MotionAction.STOP,
            speed=0.0,
            robot_id=robot_id,
            source=source,
        )


    def normalized_speed(self) -> float:
        ''' clamps speed to [0.0, 1.0] range '''
        return max(0.0, min(1.0, float(self.speed)))