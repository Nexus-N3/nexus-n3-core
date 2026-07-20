"""Local ZeroMQ client for talking to nexus-n3-core."""

from __future__ import annotations

import threading
from typing import Callable

import zmq


class LocalGatewayClient:
    """Bridge client for local command/event transport."""

    def __init__(self, *, cmd_pub_addr: str, evt_sub_addr: str):
        # Use a dedicated context so the bridge can shut its sockets down
        # deterministically without interfering with the server gateway.
        self._ctx = zmq.Context()
        self._cmd_pub = self._ctx.socket(zmq.PUB)
        self._cmd_pub.setsockopt(zmq.LINGER, 0)
        self._cmd_pub.connect(cmd_pub_addr)

        self._evt_sub = self._ctx.socket(zmq.SUB)
        self._evt_sub.setsockopt(zmq.LINGER, 0)
        self._evt_sub.setsockopt(zmq.RCVTIMEO, 250)
        self._evt_sub.connect(evt_sub_addr)
        self._evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self._running = False
        self._thread: threading.Thread | None = None

    def start(self, on_event: Callable[[dict], None]) -> None:
        """Start listening for local events."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._event_loop,
            args=(on_event,),
            daemon=True,
            name="nexus-n3-local-gateway-client",
        )
        self._thread.start()

    def _event_loop(self, on_event: Callable[[dict], None]) -> None:
        while self._running:
            try:
                event = self._evt_sub.recv_json()
            except zmq.Again:
                continue
            except zmq.ZMQError:
                break
            on_event(event)

    def send_command(self, command: dict) -> None:
        """Publish a command to the local gateway."""
        self._cmd_pub.send_json(command)

    def close(self) -> None:
        """Stop sockets owned by the bridge."""
        self._running = False
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=1.0)
        self._thread = None
        self._cmd_pub.close(linger=0)
        self._evt_sub.close(linger=0)
        self._ctx.term()
