import threading
import time

import zmq

from nexus_n3.gateway.messaging import message_types as mt


class Client:
    def __init__(self, cmd_pub_addr="tcp://localhost:5555", evt_sub_addr="tcp://localhost:5556"):
        self.ctx = zmq.Context.instance()
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
        self.lifecycle_events = []
        self.failed = False
        self.disconnect_sent = False

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
                self.failed = True
                self.stop()

    def handle_event(self, event: dict):
        print("SYSTEM EVENT:", event)
        evt_type = event.get("type")

        if evt_type == mt.EVT_SERVER_READY:
            self.send_command(
                {
                    "type": mt.CMD_INIT_SYSTEM,
                    "payload": {"subjects": self.subjects, "init_label": "Startup_gate"},
                }
            )
        elif evt_type == mt.EVT_SYSTEM_INITIALIZED:
            self.send_command({"type": mt.CMD_DISCOVER_SENSORS})
        elif evt_type == mt.EVT_SENSORS_DISCOVERED:
            self.send_command({"type": mt.CMD_CONNECT_TO_ALL})
        elif evt_type == mt.EVT_SENSOR_CONNECTED:
            self.send_command({"type": mt.CMD_START_STREAM_FOR_ALL, "payload": {"tag": "startup_gate"}})
        elif evt_type in {
            mt.EVT_STREAM_STARTED,
            mt.EVT_STREAM_WARMUP_STARTED,
            mt.EVT_STREAM_WARMUP_STATUS,
            mt.EVT_STREAM_OFFICIAL_STARTED,
            mt.EVT_STREAM_STARTUP_RETRY,
            mt.EVT_STREAM_STARTUP_FAILED,
        }:
            self.lifecycle_events.append(evt_type)
            if evt_type == mt.EVT_STREAM_OFFICIAL_STARTED:
                threading.Thread(target=self._stop_later, daemon=True).start()
            elif evt_type == mt.EVT_STREAM_STARTUP_FAILED:
                self.failed = True
                self.stop()
        elif evt_type == mt.EVT_STREAM_STOPPED:
            if not self.disconnect_sent:
                self.disconnect_sent = True
                self.send_command({"type": mt.CMD_DISCONNECT_ALL})
        elif evt_type == mt.EVT_SENSOR_DISCONNECTED:
            self.stop()
        elif evt_type == mt.EVT_ERROR:
            print("ERROR:", event.get("payload"))
            self.failed = True
            self.stop()

    def _stop_later(self):
        time.sleep(2.0)
        self.send_command({"type": mt.CMD_STOP_STREAM_FOR_ALL})

    def send_command(self, command: dict):
        self.cmd_pub.send_json(command)

    def stop(self):
        with self._stop_lock:
            if not self._running:
                return
            self._running = False
            try:
                self.evt_sub.close()
            except Exception:
                pass
            try:
                self.cmd_pub.close()
            except Exception:
                pass
            if self._event_thread and threading.current_thread() is not self._event_thread:
                self._event_thread.join(timeout=1.0)


if __name__ == "__main__":
    subjects = [
        {
            "subject_id": "subject1",
            "sensors": [
                {
                    "local_name": "Movella DOT",
                    "number_of": 1,
                    "compute_algorithm": {
                        "name": "standard_loading_intensity",
                        "inputs": {"gravity": 9.80665},
                    },
                    "locations": ["LEFT_ANKLE"],
                }
            ],
        }
    ]

    client = Client()
    client.subjects = subjects
    client.start()
    time.sleep(1.0)
    client.send_command({"type": mt.CMD_IS_SERVER_READY})

    deadline = time.time() + 60.0
    while client._running and time.time() < deadline:
        time.sleep(0.25)

    if client._running:
        client.failed = True
        print("FAILED: timed out waiting for startup gate lifecycle")
        client.stop()
        raise SystemExit(1)

    if client.failed:
        raise SystemExit(1)

    required = [
        mt.EVT_STREAM_STARTED,
        mt.EVT_STREAM_WARMUP_STARTED,
        mt.EVT_STREAM_OFFICIAL_STARTED,
    ]
    missing = [event_type for event_type in required if event_type not in client.lifecycle_events]
    if missing:
        print("FAILED: missing lifecycle events", missing)
        raise SystemExit(1)

    if client.lifecycle_events.index(mt.EVT_STREAM_WARMUP_STARTED) > client.lifecycle_events.index(
        mt.EVT_STREAM_OFFICIAL_STARTED
    ):
        print("FAILED: official start arrived before warmup started")
        raise SystemExit(1)

    print("PASSED: lifecycle events", client.lifecycle_events)
