"""ZeroMQ-based gateway implementation."""

import os
import threading
from typing import Callable

import zmq

from nexus_n3.gateway.gateways.gateway_interface import GatewayInterface
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.core.runtime_env import load_runtime_env

logger = get_module_logger("ZeroMQ Gateway")


# implements the ABC gateway interface contract.
class ZeroMQGateway(GatewayInterface):
    """
    ZeroMQ gateway for command input and event output.

    Loads bind addresses from the shared runtime environment using
    `ZEROMQ_CMD_BIND` and `ZEROMQ_EVENT_BIND`.
    """

    scope = "local"

    def __init__(self, site: str):
        """
        Initialize ZeroMQ sockets and internal state.

        Args:
            site: Site identifier used in published events.
        """
        load_runtime_env()
        self.site = site
        print("ZEROMQ", self.site)
        # Use a dedicated context so shutdown can terminate local sockets
        # without affecting other in-process ZeroMQ users.
        self.ctx = zmq.Context()

        # Server receives commands from clients.
        self._cmd_sub = self.ctx.socket(zmq.SUB)
        self._cmd_sub.setsockopt(zmq.LINGER, 0)
        self._cmd_sub.setsockopt(zmq.RCVTIMEO, 250)
        cmd_bind = os.environ.get("ZEROMQ_CMD_BIND", "tcp://*:5555")
        self._cmd_sub.bind(cmd_bind)
        self._cmd_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        # Server publishes system events.
        self._event_pub = self.ctx.socket(zmq.PUB)
        self._event_pub.setsockopt(zmq.LINGER, 0)
        evt_bind = os.environ.get("ZEROMQ_EVENT_BIND", "tcp://*:5556")
        self._event_pub.bind(evt_bind)

        self._running = False
        self._recv_thread: threading.Thread | None = None

    def start(self, on_message: Callable[[dict], None]):
        """
        Start the receive loop in a background thread.

        Args:
            on_message: Callback to handle incoming messages.
        """
        if self._running:
            return
        self._running = True
        self._recv_thread = threading.Thread(
            target=self._recv_loop,
            args=(on_message,),
            daemon=True,
            name="nexus-n3-zeromq-recv",
        )
        self._recv_thread.start()

    def _recv_loop(self, handler: Callable[[dict], None]):
        """
        Receive and dispatch messages from clients.

        Args:
            handler: Callback for each received message.
        """
        while self._running:
            try:
                msg = self._cmd_sub.recv_json()
            except zmq.Again:
                continue
            except zmq.ZMQError as exc:
                if self._running:
                    logger.warning(f"ZeroMQ command receive failed during runtime: {exc}")
                break
            handler(msg)

    def publish_command(self, command: dict):
        """
        Publish a command to clients.

        Args:
            command: Command dictionary with type and payload.
        """
        self._event_pub.send_json(command)

    def publish_event(self, event: dict):
        """
        Publish a system event to clients.

        Args:
            event: Event dictionary with type and payload.
        """
        try:
            print(f"[GatewayDebug] Publishing event: {event.get('type')}")
            
            #print(f"[GatewayDebug] Publishing event: {event}")
            event["site"] = self.site
            self._event_pub.send_json(event)
        except Exception as exc:
            print(f"[GatewayDebug] Failed to publish event {event.get('type')}: {exc}")

    def stop(self):
        """Stop the receive loop and release ZeroMQ resources."""
        if not self._running and self._recv_thread is None:
            return
        self._running = False

        thread = self._recv_thread
        if thread and thread.is_alive():
            thread.join(timeout=1.0)
        self._recv_thread = None

        self._cmd_sub.close(linger=0)
        self._event_pub.close(linger=0)
        self.ctx.term()
