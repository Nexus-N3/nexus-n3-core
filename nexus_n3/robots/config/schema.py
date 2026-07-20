from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# this is a dataclass representing the configuration supplied in config.yaml.
# essentailly a wrapper around config.yaml
# optional fields are there because the node may not be a robot
# the default is that the node is not a robot and should be explicitly marked as a robot in the config if it is one. this is to avoid accidentally treating a non-robot node as a robot and causing issues.

@dataclass(slots=True)
class RobotModuleConfig:
    raw: dict[str, Any]

    @property
    def is_robot(self) -> bool:
        return bool(self.raw.get("is_robot", False))

    @property
    def robot_id(self) -> str | None:
        return self.raw.get("robot", {}).get("id")

    @property
    def robot_type(self) -> str | None:
        return self.raw.get("robot", {}).get("type")

    @property
    def transport_type(self) -> str | None:
        return self.raw.get("transport", {}).get("type")

    @property
    def protocol_type(self) -> str | None:
        return self.raw.get("protocol", {}).get("type")

    def get(self, path: str, default: Any = None) -> Any:
        node: Any = self.raw

        for part in path.split("."):
            if not isinstance(node, dict):
                return default

            if part not in node:
                return default

            node = node[part]

        return node