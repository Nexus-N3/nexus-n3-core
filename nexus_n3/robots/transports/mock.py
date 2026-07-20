from __future__ import annotations

from nexus_n3.robots.transports.base import Transport

# this is a dummy transport that can be used for testing and development without an actual robot.

class MockTransport(Transport):
    def __init__(self) -> None:
        self.connected = False
        self.sent_payloads: list[bytes] = []
        self.rx_queue: list[bytes] = []

    def connect(self) -> None:
        '''Mock connect just sets the connected flag to True.'''
        self.connected = True

    def close(self) -> None:
        '''Mock close just sets the connected flag to False.'''
        self.connected = False

    def send(self, payload: bytes) -> None:
        if not self.connected:
            raise RuntimeError("MockTransport not connected")
        self.sent_payloads.append(payload)

    def recv(self) -> bytes:
        if not self.connected:
            raise RuntimeError("MockTransport not connected")

        if not self.rx_queue:
            return b""

        return self.rx_queue.pop(0)

    def queue_rx(self, payload: bytes) -> None:
        """
        Push fake incoming data into the transport.
        """
        self.rx_queue.append(payload)