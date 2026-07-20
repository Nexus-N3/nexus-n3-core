"""
Example ZeroMQ client for interacting with the Nexus N3 Core Gateway.

This client:
- Publishes commands to the gateway command channel (PUB)
- Subscribes to system and sensor events from the gateway (SUB)
- Implements a simple state-driven control flow based on server events

Intended as:
- A reference implementation
- A debugging / testing client
- An example for higher-level applications
"""

import zmq
import threading
import time
from nexus_n3.gateway.messaging import message_types as mt


class Client:
    """
    Example Nexus N3 Core client using ZeroMQ.

    Responsibilities:
    - Manage PUB/SUB sockets
    - Send control commands to the gateway
    - Receive and react to gateway events
    - Maintain minimal client-side state (subjects, pending connections)
    """

    def __init__(self,
                 cmd_pub_addr="tcp://localhost:5555",
                 evt_sub_addr="tcp://localhost:5556"):
        """
        Initialize the client and connect ZeroMQ sockets.

        Args:
            cmd_pub_addr: Address of the gateway command PUB socket
            evt_sub_addr: Address of the gateway event SUB socket
        """

        # Shared ZeroMQ context
        self.ctx = zmq.Context.instance()

        # PUB socket for sending commands to the server
        self.cmd_pub = self.ctx.socket(zmq.PUB)
        self.cmd_pub.connect(cmd_pub_addr)

        # SUB socket for receiving events from the server
        self.evt_sub = self.ctx.socket(zmq.SUB)
        self.evt_sub.connect(evt_sub_addr)

        # Subscribe to all event topics
        self.evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self._running = False

        # ---- Client-side state ----

        # List of subject definitions (provided by user)
        self.subjects = []

        # Track which subjects are waiting for sensor connections
        self.pending_connect = set()

    def start(self):
        """
        Start the client event loop in a background thread.
        """
        self._running = True
        threading.Thread(
            target=self._event_loop,
            daemon=True
        ).start()

    def _event_loop(self):
        """
        Background loop that listens for gateway events.

        Runs until the client is stopped.
        """
        while self._running:
            try:
                msg = self.evt_sub.recv_json()
                self.handle_event(msg)
            except Exception as e:
                print("Error receiving event:", e)

    def handle_event(self, event: dict):
        """
        Handle an incoming event from the gateway.

        Events are dispatched based on their `type` field
        and drive the control flow of the client.

        Args:
            event: Event message received from the gateway
        """

        print("SYSTEM EVENT:", event)

        evt_type = event.get("type")
        payload = event.get("payload", {})

        # --- SERVER READY → INIT SYSTEM ---
        if evt_type == mt.EVT_SERVER_READY:
            init_command = {
                "type": mt.CMD_INIT_SYSTEM,
                "payload": {
                    "subjects": self.subjects,
                    "init_label": "baseline_data_collection",
                }
            }
            self.send_command(init_command)

        # --- SYSTEM INITIALIZED ---
        elif evt_type == mt.EVT_SYSTEM_INITIALIZED:
            print(f"system intialised with {payload}")

        elif evt_type == mt.EVT_ERROR:
            print(f"ERROR: {payload}")
            self.stop()

    def send_command(self, command: dict):
        """
        Send a command message to the gateway.

        Args:
            command: Command dictionary matching gateway protocol
        """
        self.cmd_pub.send_json(command)
        print("Sent command:", command["type"])

    def stop(self):
        """
        Stop the client and clean up ZeroMQ resources.
        """
        self._running = False
        self.cmd_pub.close()
        self.evt_sub.close()
        self.ctx.term()


if __name__ == "__main__":
    """
    Example standalone execution of the client.
    """

    # Define subjects managed by this client
    subjects = [
       {
            "subject_id": "subject1",
            "sensors": [
                {
                    "local_name": "Movella DOT", 
                    "number_of": 2,
                    "compute_algorithm": 
                        { 
                            "name": "standard_loading_intensity",
                            "inputs": {
                                "gravity": 9.80665
                            }
                        },
                    "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
                }
            ],
        },
    ]

    client = Client()
    client.subjects = subjects

    client.start()
    time.sleep(1)  # allow sockets to connect

    # Trigger server readiness check
    client.send_command({
        "type": mt.CMD_IS_SERVER_READY
    })

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.stop()
