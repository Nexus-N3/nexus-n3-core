import zmq
import threading
import time
from nexus_n3.gateway.messaging import message_types as mt


class Client:
    """
    Example Nexus N3 Core client using ZeroMQ.
    """

    def __init__(self,
                 cmd_pub_addr="tcp://localhost:5555",
                 evt_sub_addr="tcp://localhost:5556"):

        self.ctx = zmq.Context.instance()

        # PUB socket for sending commands to the server
        self.cmd_pub = self.ctx.socket(zmq.PUB)
        self.cmd_pub.connect(cmd_pub_addr)

        # SUB socket for receiving events from the server
        self.evt_sub = self.ctx.socket(zmq.SUB)
        self.evt_sub.connect(evt_sub_addr)
        self.evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self._running = False
        self.subjects = []
        self.pending_connect = set()

    def start(self):
        """Start the client event loop in a background thread."""
        self._running = True
        threading.Thread(target=self._event_loop, daemon=True).start()

    def _event_loop(self):
        """Background loop that listens for gateway events."""
        while self._running:
            try:
                msg = self.evt_sub.recv_json()
                self.handle_event(msg)
            except Exception as e:
                print("Error receiving event:", e)

    def handle_event(self, event: dict):
        print("SYSTEM EVENT:", event)

        evt_type = event.get("type")
        payload = event.get("payload", {})

        if evt_type == mt.EVT_SERVER_READY:
            self.send_command({
                "type": mt.CMD_INIT_SYSTEM,
                "payload": {"subjects": self.subjects, "init_label": "Anna_bdc"},
            })

        elif evt_type == mt.EVT_SYSTEM_INITIALIZED:
            self.send_command({"type": mt.CMD_DISCOVER_SENSORS})
            for sub in self.subjects:
                self.pending_connect.add(sub["subject_id"])

        elif evt_type == mt.EVT_SENSORS_DISCOVERED:
            if not payload:
                return
            self.send_command({"type": mt.CMD_CONNECT_TO_ALL})
            for subject_info in payload:
                self.pending_connect.discard(subject_info["subject_id"])

        elif evt_type == mt.EVT_SENSOR_CONNECTED:
            # Trigger identify commands immediately
            for sub in self.subjects:
                for sensor_conf in sub.get("sensors", []):
                    for location in sensor_conf.get("locations", []):
                        self.send_command({
                            "type": mt.CMD_IDENTIFY_SENSOR,
                            "payload": {"subject_id": sub["subject_id"], "location": location}
                        })

            # Handle identify → stream → stop in a background thread
            threading.Thread(target=self._handle_stream_sequence, daemon=True).start()

        elif evt_type == mt.EVT_STREAM_STARTED:
            print(f"stream started for {payload}")

        elif evt_type == mt.EVT_STREAM_STOPPED:
            print(f"stream stopped for {payload}")
            self.send_command({"type": mt.CMD_DISCONNECT_ALL})

        elif evt_type == mt.EVT_SENSOR_DISCONNECTED:
            print("Sensor's Disconnect", payload)
        
        elif evt_type == mt.EVT_COMPUTE_RESULT:
            print(f"compute result received {payload['result']['result_count']}")

        # to view the results
        elif evt_type == mt.EVT_INTERMEDIATE_RESULT:
            print(f"INTERMEDIATE RESULT: {payload}")

        elif evt_type == mt.EVT_ERROR:
            print(f"ERROR: {payload}")
            self.stop()

    def _handle_stream_sequence(self):
        """Run identify + start streaming + stop streaming with delays in a thread."""
        time.sleep(10)  # 10s for identify
        self.send_command({
            "type": mt.CMD_START_STREAM_FOR_ALL,
            "payload": {
                "tags": {
                    "subject1": "run",
                }
            },
        })
        time.sleep(20)  # 20s streaming
        self.send_command({"type": mt.CMD_STOP_STREAM_FOR_ALL})

    def send_command(self, command: dict):
        """Send a command message to the gateway."""
        self.cmd_pub.send_json(command)

    def stop(self):
        """Stop the client and clean up ZeroMQ resources."""
        self._running = False
        self.cmd_pub.close()
        self.evt_sub.close()
        self.ctx.term()


if __name__ == "__main__":
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

    client.send_command({"type": mt.CMD_IS_SERVER_READY})

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.stop()
