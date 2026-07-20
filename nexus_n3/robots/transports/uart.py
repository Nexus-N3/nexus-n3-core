from __future__ import annotations

import serial

from nexus_n3.robots.transports.base import Transport

# thin wrapper around pyserial
# MotionCommand → Protocol → bytes → UartTransport → robot
class UartTransport(Transport):
    def __init__(
        self,
        port: str,
        baudrate: int = 115200,
        timeout: float = 1.0,
    ) -> None:
        self._port = port
        self._baudrate = baudrate
        self._timeout = timeout
        self._serial: serial.Serial | None = None

    def connect(self) -> None:
        if self._serial is not None and self._serial.is_open:
            return

        self._serial = serial.Serial(
            port=self._port,
            baudrate=self._baudrate,
            timeout=self._timeout,
        )

    def close(self) -> None:
        if self._serial is not None and self._serial.is_open:
            self._serial.close()

    def send(self, payload: bytes) -> None:
        if self._serial is None or not self._serial.is_open:
            raise RuntimeError("UART transport not connected")

        self._serial.write(payload)

    def recv(self) -> bytes:
        if self._serial is None or not self._serial.is_open:
            raise RuntimeError("UART transport not connected")

        return self._serial.readline()