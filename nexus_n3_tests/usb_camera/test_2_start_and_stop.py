import zmq
import threading
import time
from nexus_n3.gateway.messaging import message_types as mt


class Client:
    def __init__(self,
                 cmd_pub_addr="tcp://localhost:5555",
                 evt_sub_addr="tcp://localhost:5556"):
        self.ctx = zmq.Context.instance()
        self.cmd_pub = self.ctx.socket(zmq.PUB)
        self.cmd_pub.connect(cmd_pub_addr)
        self.evt_sub = self.ctx.socket(zmq.SUB)
        self.evt_sub.connect(evt_sub_addr)
        self.evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")
        self._running = False
        self.subjects = []

    def start(self):
        self._running = True
        threading.Thread(target=self._event_loop, daemon=True).start()

    def _event_loop(self):
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
                "payload": {"subjects": self.subjects, "init_label": "usb_camera_test"},
            })
        elif evt_type == mt.EVT_SYSTEM_INITIALIZED:
            self.send_command({"type": mt.CMD_DISCOVER_SENSORS})
        elif evt_type == mt.EVT_SENSORS_DISCOVERED:
            if not payload:
                return
            self.send_command({"type": mt.CMD_CONNECT_TO_ALL})
        elif evt_type == mt.EVT_SENSOR_CONNECTED:
            threading.Thread(target=self._stream_sequence, daemon=True).start()
        elif evt_type == mt.EVT_STREAM_STARTED:
            print("stream started")
        elif evt_type == mt.EVT_STREAM_STOPPED:
            print("stream stopped")
            self.send_command({"type": mt.CMD_DISCONNECT_ALL})
        elif evt_type == mt.EVT_COMPUTE_RESULT:
            print(f"compute result received {payload.get('algorithm_name')}")
        elif evt_type == mt.EVT_ERROR:
            print(f"ERROR: {payload}")
            self.stop()

    def _stream_sequence(self):
        time.sleep(2)
        self.send_command({"type": mt.CMD_START_STREAM_FOR_ALL, "payload": {"tag": "usb_cam"}})
        time.sleep(5)
        self.send_command({"type": mt.CMD_STOP_STREAM_FOR_ALL})

    def send_command(self, command: dict):
        self.cmd_pub.send_json(command)

    def stop(self):
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
                    "local_name": "XWF 1080P PC Camera: XWF 1080P",
                    "number_of": 1,
                    "locations": ["STARLAB"],
                    "compute_algorithm": {
                        "name": "pass_through",
                        "inputs": {"batch_size": 5, "max_interval_ms": 1000},
                    },
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
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.stop()
