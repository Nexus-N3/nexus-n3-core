"""
Live end-to-end ZeroMQ smoke test for stream stop -> drain -> upload.

Run this while:
- `nexus_n3_server.py` is running with the ZeroMQ gateway
- `python -m nexus_n3.azure_bridge` is running

This script follows the same flow as the core start/stop test and relies on the
bridge to upload the finished session archive on `EVT_STREAM_DRAINED`.
"""

import threading
import time

import zmq

from nexus_n3.gateway.messaging import message_types as mt


class Client:
    def __init__(self, cmd_pub_addr="tcp://localhost:5555", evt_sub_addr="tcp://localhost:5556"):
        self.ctx = zmq.Context()
        self.cmd_pub = self.ctx.socket(zmq.PUB)
        self.cmd_pub.setsockopt(zmq.LINGER, 0)
        self.cmd_pub.connect(cmd_pub_addr)

        self.evt_sub = self.ctx.socket(zmq.SUB)
        self.evt_sub.setsockopt(zmq.LINGER, 0)
        self.evt_sub.setsockopt(zmq.RCVTIMEO, 250)
        self.evt_sub.connect(evt_sub_addr)
        self.evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self._running = False
        self._event_thread = None
        self._stop_lock = threading.Lock()
        self.subjects = []

    def start(self):
        self._running = True
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()

    def _event_loop(self):
        while self._running:
            try:
                msg = self.evt_sub.recv_json()
                self.handle_event(msg)
            except zmq.Again:
                continue
            except zmq.ZMQError:
                if not self._running:
                    break
            except Exception as exc:
                print("Error receiving event:", exc)

    def handle_event(self, event: dict):
        print("SYSTEM EVENT:", event)
        evt_type = event.get("type")

        if evt_type == mt.EVT_SERVER_READY:
            self.send_command(
                {
                    "type": mt.CMD_INIT_SYSTEM,
                    "payload": {
                        "subjects": self.subjects,
                        "init_label": "Right Step",
                        "app_id": "nexus",
                        "app_name": "Nexus",
                    },
                }
            )
        elif evt_type == mt.EVT_SYSTEM_INITIALIZED:
            self.send_command({"type": mt.CMD_DISCOVER_SENSORS})
        elif evt_type == mt.EVT_SENSORS_DISCOVERED:
            self.send_command({"type": mt.CMD_CONNECT_TO_ALL})
        elif evt_type == mt.EVT_SENSOR_CONNECTED:
            threading.Thread(target=self._handle_stream_sequence, daemon=True).start()
        elif evt_type == mt.EVT_STREAM_DRAINED:
            print("Success: stream drained. The bridge should upload the archived session now.")
            self.send_command({"type": mt.CMD_DISCONNECT_ALL})
        elif evt_type == mt.EVT_SENSOR_DISCONNECTED:
            self.stop()
        elif evt_type == mt.EVT_ERROR:
            print("ERROR:", event.get("payload"))
            self.stop()

    def _handle_stream_sequence(self):
        time.sleep(5)
        self.send_command({"type": mt.CMD_START_STREAM_FOR_ALL, "payload": {"tag": "azure_upload_e2e"}})
        time.sleep(60)
        self.send_command({"type": mt.CMD_STOP_STREAM_FOR_ALL})

    def send_command(self, command: dict):
        self.cmd_pub.send_json(command)

    def stop(self):
        with self._stop_lock:
            if not self._running:
                return
            self._running = False
            self.evt_sub.close()
            self.cmd_pub.close()
            if self._event_thread and threading.current_thread() is not self._event_thread:
                self._event_thread.join(timeout=1.0)
            self.ctx.term()


if __name__ == "__main__":
    subjects = [
        {
            "subject_id": "subject1",
            "sensors": [
                {
                    "local_name": "Movella DOT",
                    "number_of": 2,
                    "compute_algorithm": {
                        "name": "standard_loading_intensity",
                        "inputs": {"gravity": 9.80665},
                    },
                    "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
                }
            ],
        },
    ]

    client = Client()
    client.subjects = subjects
    client.start()
    time.sleep(1)
    client.send_command({"type": mt.CMD_IS_SERVER_READY})

    try:
        while client._running:
            time.sleep(1)
    except KeyboardInterrupt:
        client.stop()
