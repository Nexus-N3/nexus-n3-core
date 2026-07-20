from __future__ import annotations

from nexus_n3.robots.adapters.base import BaseRobotAdapter
from nexus_n3.robots.models.commands import MotionCommand


class WaveRoverAdapter(BaseRobotAdapter):
    """
    Adapter for Waveshare WAVE ROVER.

    The actual Waveshare wire format is handled by WaveshareUartProtocol.
    This adapter is responsible for connecting the generic robot runtime
    to that protocol and transport.
    """

    def handle_motion(self, cmd: MotionCommand) -> None:
        payload = self.protocol.encode_motion(cmd)
        self.transport.send(payload)
        self._mark_motion()