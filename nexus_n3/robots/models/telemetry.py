from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# models aspects of the rover we can get as telemetry. There may be more important pieces
# but this will suffice for now.

@dataclass(slots=True)
class RobotTelemetry:
    robot_id: str
    raw: dict[str, Any]

    @property
    def packet_type(self) -> int | None:
        value = self.raw.get("T")
        if value is None:
            return None
        return int(value)

    @property
    def heading_deg(self) -> float | None:
        value = self.raw.get("heading")
        if value is None:
            return None
        return float(value)

    @property
    def voltage(self) -> float | None:
        value = self.raw.get("voltage")
        if value is None:
            return None
        return float(value)