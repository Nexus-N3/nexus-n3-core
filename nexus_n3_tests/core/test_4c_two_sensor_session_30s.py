import argparse
import threading
import time

import zmq

from nexus_n3.gateway.messaging import message_types as mt


DEFAULT_SUBJECTS = [
    {
        "subject_id": "subject1",
        "sensors": [
            {
                "local_name": "Movella DOT",
                "number_of": 2,
                "compute_algorithm": {
                    "name": "standard_loading_intensity",
                    "inputs": {
                        "gravity": 9.80665,
                    },
                },
                "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
            }
        ],
    }
]


class Client:
    """Live ZeroMQ client for a 2-sensor, 30-second session run."""

    def __init__(
        self,
        cmd_pub_addr="tcp://localhost:5555",
        evt_sub_addr="tcp://localhost:5556",
        stream_seconds=30,
        subjects=None,
    ):
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
        self._done = threading.Event()

        self.subjects = subjects or DEFAULT_SUBJECTS
        self.stream_seconds = stream_seconds
        self.expected_sensor_count = sum(
            sensor.get("number_of", 0)
            for subject in self.subjects
            for sensor in subject.get("sensors", [])
        )

        self.discovered_addresses = set()
        self.connected_addresses = set()
        self.disconnected_addresses = set()

        self.connect_sent = False
        self.stream_start_sent = False
        self.stream_stop_sent = False
        self.disconnect_sent = False
        self.stop_timer_started = False

        self.failed = False
        self.failure_reason = ""

    def start(self):
        self._running = True
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()

    def run(self, timeout_seconds):
        self.start()
        time.sleep(1)
        self.send_command({"type": mt.CMD_IS_SERVER_READY})

        completed = self._done.wait(timeout=timeout_seconds)
        if not completed:
            self._fail(
                f"Timed out after {timeout_seconds}s waiting for 2-sensor session completion."
            )
        self.stop()
        return not self.failed

    def _event_loop(self):
        while self._running:
            try:
                self.handle_event(self.evt_sub.recv_json())
            except zmq.Again:
                continue
            except zmq.ZMQError:
                if not self._running:
                    break
                self._fail("ZeroMQ error while waiting for gateway events.")
            except Exception as exc:
                self._fail(f"Unhandled client exception: {exc}")

    def handle_event(self, event):
        evt_type = event.get("type")
        payload = event.get("payload", {})
        print(f"EVENT: {evt_type}")

        if evt_type == mt.EVT_SERVER_READY:
            self.send_command(
                {
                    "type": mt.CMD_INIT_SYSTEM,
                    "payload": {
                        "subjects": self.subjects,
                        "init_label": "two_sensor_session_30s",
                    },
                }
            )
            return

        if evt_type == mt.EVT_SYSTEM_INITIALIZED:
            self.send_command({"type": mt.CMD_DISCOVER_SENSORS})
            return

        if evt_type == mt.EVT_SENSORS_DISCOVERED:
            discovered = self._collect_addresses(payload, "discovered_sensors")
            self.discovered_addresses.update(discovered)
            print(
                "DISCOVERED:",
                sorted(self.discovered_addresses),
                f"({len(self.discovered_addresses)}/{self.expected_sensor_count})",
            )
            if not self.connect_sent:
                self.connect_sent = True
                self.send_command({"type": mt.CMD_CONNECT_TO_ALL})
            return

        if evt_type == mt.EVT_SENSOR_CONNECTED:
            connected = self._collect_addresses(payload, "connected_sensors")
            self.connected_addresses.update(connected)
            print(
                "CONNECTED:",
                sorted(self.connected_addresses),
                f"({len(self.connected_addresses)}/{self.expected_sensor_count})",
            )
            if (
                len(self.connected_addresses) >= self.expected_sensor_count
                and not self.stream_start_sent
            ):
                self.stream_start_sent = True
                self.send_command(
                    {
                        "type": mt.CMD_START_STREAM_FOR_ALL,
                        "payload": {"tag": "two_sensor_session_30s"},
                    }
                )
            return

        if evt_type == mt.EVT_STREAM_STARTED:
            print(f"STREAM STARTED: {payload}")
            if not self.stop_timer_started:
                self.stop_timer_started = True
                threading.Thread(target=self._stop_stream_after_delay, daemon=True).start()
            return

        if evt_type == mt.EVT_STREAM_STOPPED:
            print(f"STREAM STOPPED: {payload}")
            if not self.disconnect_sent:
                self.disconnect_sent = True
                self.send_command({"type": mt.CMD_DISCONNECT_ALL})
            return

        if evt_type == mt.EVT_SENSOR_DISCONNECTED:
            disconnected = payload.get("disconnected_sensors") or []
            for sensor in disconnected:
                if isinstance(sensor, str):
                    self.disconnected_addresses.add(sensor)
                elif isinstance(sensor, dict):
                    address = sensor.get("address")
                    if address:
                        self.disconnected_addresses.add(address)
            print(
                "DISCONNECTED:",
                sorted(self.disconnected_addresses),
                f"({len(self.disconnected_addresses)}/{self.expected_sensor_count})",
            )
            if self.disconnect_sent and len(self.disconnected_addresses) >= self.expected_sensor_count:
                self._done.set()
            return

        if evt_type == mt.EVT_ERROR:
            self._fail(f"Gateway error: {payload}")

    def _stop_stream_after_delay(self):
        print(f"Streaming for {self.stream_seconds}s...")
        time.sleep(self.stream_seconds)
        if not self.stream_stop_sent:
            self.stream_stop_sent = True
            self.send_command({"type": mt.CMD_STOP_STREAM_FOR_ALL})

    def _collect_addresses(self, payload, field_name):
        addresses = set()
        for subject in payload.get("subjects", []):
            for sensor in subject.get(field_name, []):
                if isinstance(sensor, str):
                    addresses.add(sensor)
                elif isinstance(sensor, dict):
                    address = sensor.get("address")
                    if address:
                        addresses.add(address)
        return addresses

    def send_command(self, command):
        print("COMMAND:", command["type"], command.get("payload", {}))
        self.cmd_pub.send_json(command)

    def _fail(self, reason):
        if self.failed:
            return
        self.failed = True
        self.failure_reason = reason
        print(f"FAILED: {reason}")
        self._done.set()

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
            try:
                self.ctx.term()
            except Exception:
                pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a live 2-sensor Nexus N3 Core session for 30 seconds."
    )
    parser.add_argument("--cmd-pub-addr", default="tcp://localhost:5555")
    parser.add_argument("--evt-sub-addr", default="tcp://localhost:5556")
    parser.add_argument("--stream-seconds", type=int, default=30)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.stream_seconds <= 0:
        raise SystemExit("--stream-seconds must be a positive integer")
    if args.timeout_seconds <= args.stream_seconds:
        raise SystemExit("--timeout-seconds must be greater than --stream-seconds")

    client = Client(
        cmd_pub_addr=args.cmd_pub_addr,
        evt_sub_addr=args.evt_sub_addr,
        stream_seconds=args.stream_seconds,
    )
    ok = client.run(timeout_seconds=args.timeout_seconds)
    if not ok:
        raise SystemExit(client.failure_reason or "2-sensor session test failed")
