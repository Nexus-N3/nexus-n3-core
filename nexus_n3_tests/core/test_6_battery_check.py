"""
Example ZeroMQ client for pre-init battery checks.

Flow:
1) Send CMD_IS_SERVER_READY
2) On EVT_SERVER_READY, send CMD_CHECK_BATTERY
3) On EVT_BATTERY_CHECK, print results and exit
"""

import threading
import time
import zmq

from nexus_n3.gateway.messaging import message_types as mt


class Client:
    def __init__(
        self,
        cmd_pub_addr="tcp://localhost:5555",
        evt_sub_addr="tcp://localhost:5556",
    ):
        self.ctx = zmq.Context.instance()
        self.cmd_pub = self.ctx.socket(zmq.PUB)
        self.cmd_pub.connect(cmd_pub_addr)

        self.evt_sub = self.ctx.socket(zmq.SUB)
        self.evt_sub.connect(evt_sub_addr)
        self.evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self._running = False

    def start(self):
        self._running = True
        threading.Thread(target=self._event_loop, daemon=True).start()

    def stop(self):
        self._running = False
        time.sleep(0.1)

    def _event_loop(self):
        while self._running:
            try:
                msg = self.evt_sub.recv_json()
                self.handle_event(msg)
            except Exception as exc:
                print("Error receiving event:", exc)

    def send_command(self, msg: dict):
        print(f"sending msg {msg}")
        self.cmd_pub.send_json(msg)

    def handle_event(self, event: dict):
        print("SYSTEM EVENT:", event)
        evt_type = event.get("type")
        payload = event.get("payload", {})

        if evt_type == mt.EVT_SERVER_READY:
            self.send_command({
                "type": mt.CMD_CHECK_BATTERY,
                "payload": {
                    "scan_timeout": 5.0,
                    "read_timeout": 10.0,
                },
            })
        elif evt_type == mt.EVT_BATTERY_CHECK:
            print("Battery check results:", payload)
            self.stop()
        elif evt_type == mt.EVT_ERROR:
            print("Error:", payload)
            self.stop()


if __name__ == "__main__":
    client = Client()
    client.start()
    time.sleep(1)  # allow sockets to connect
    client.send_command({"type": mt.CMD_IS_SERVER_READY})
    while client._running:
        time.sleep(0.1)
