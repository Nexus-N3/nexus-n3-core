import argparse
import threading
import time
from pathlib import Path
import zipfile

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
        self.compute_event_count = 0
        self.compute_before_official = 0
        self.disconnect_sent = False
        self.stop_sent = False
        self.failed = False
        self.official_started = False
        self.stream_drained_payload = None

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
        payload = event.get("payload", {})

        if evt_type == mt.EVT_SERVER_READY:
            self.send_command(
                {
                    "type": mt.CMD_INIT_SYSTEM,
                    "payload": {"subjects": self.subjects, "init_label": "Gateway_compute"},
                }
            )
        elif evt_type == mt.EVT_SYSTEM_INITIALIZED:
            self.send_command({"type": mt.CMD_DISCOVER_SENSORS})
        elif evt_type == mt.EVT_SENSORS_DISCOVERED:
            self.send_command({"type": mt.CMD_CONNECT_TO_ALL})
        elif evt_type == mt.EVT_SENSOR_CONNECTED:
            self.send_command({"type": mt.CMD_START_STREAM_FOR_ALL, "payload": {"tag": "compute_test"}})
        elif evt_type in {
            mt.EVT_STREAM_STARTED,
            mt.EVT_STREAM_WARMUP_STARTED,
            mt.EVT_STREAM_OFFICIAL_STARTED,
            mt.EVT_STREAM_STARTUP_RETRY,
            mt.EVT_STREAM_STARTUP_FAILED,
            mt.EVT_STREAM_STOPPED,
            mt.EVT_STREAM_DRAINED,
        }:
            self.lifecycle_events.append(evt_type)
            if evt_type == mt.EVT_STREAM_OFFICIAL_STARTED:
                self.official_started = True
                threading.Thread(target=self._stop_later, daemon=True).start()
            elif evt_type == mt.EVT_STREAM_STARTUP_FAILED:
                self.failed = True
                self.stop()
            elif evt_type == mt.EVT_STREAM_DRAINED:
                self.stream_drained_payload = payload
        elif evt_type == mt.EVT_COMPUTE_RESULT:
            if not self.official_started:
                self.compute_before_official += 1
            self.compute_event_count += 1
        elif evt_type == mt.EVT_SENSOR_DIAGNOSTICS:
            if payload.get("trigger") != "stream_stopped":
                return
            if not self.disconnect_sent:
                self.disconnect_sent = True
                self.send_command({"type": mt.CMD_DISCONNECT_ALL})
        elif evt_type == mt.EVT_SENSOR_DISCONNECTED:
            self.stop()
        elif evt_type == mt.EVT_ERROR:
            print("ERROR:", payload)
            self.failed = True
            self.stop()

    def _stop_later(self):
        time.sleep(12.0)
        if not self.stop_sent:
            self.stop_sent = True
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


def _assert_path_exists(path_value: str | None, description: str):
    if not path_value:
        raise SystemExit(f"FAILED: missing {description}")
    path = Path(path_value)
    if not path.exists():
        raise SystemExit(f"FAILED: {description} does not exist: {path}")


def _assert_archive_contains(archive_path_value: str | None, suffix: str):
    if not archive_path_value:
        raise SystemExit(f"FAILED: missing archive path for {suffix}")
    archive_path = Path(archive_path_value)
    if not archive_path.exists():
        raise SystemExit(f"FAILED: archive missing: {archive_path}")
    with zipfile.ZipFile(archive_path) as handle:
        names = handle.namelist()
    if not any(name.endswith(suffix) for name in names):
        raise SystemExit(f"FAILED: archive missing {suffix}")


def _build_subjects(sensor_count: int) -> list[dict]:
    locations = [
        "LEFT_ANKLE",
        "RIGHT_ANKLE",
        "LEFT_WRIST",
        "RIGHT_WRIST",
        "CHEST",
        "LOWER_BACK",
        "LEFT_FOOT",
        "RIGHT_FOOT",
    ]
    if sensor_count < 1:
        raise ValueError("sensor_count must be at least 1")
    if sensor_count > len(locations):
        raise ValueError(f"sensor_count must be <= {len(locations)}")
    return [
        {
            "subject_id": "subject1",
            "sensors": [
                {
                    "local_name": "Movella DOT",
                    "number_of": sensor_count,
                    "compute_algorithm": {
                        "name": "standard_loading_intensity",
                        "inputs": {"gravity": 9.80665},
                    },
                    "locations": locations[:sensor_count],
                }
            ],
        }
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gateway end-to-end compute integration test")
    parser.add_argument(
        "--sensor-count",
        type=int,
        default=2,
        help="Number of Movella DOT sensors to request for the test subject.",
    )
    args = parser.parse_args()

    subjects = _build_subjects(args.sensor_count)

    client = Client()
    client.subjects = subjects
    client.start()
    time.sleep(1.0)
    client.send_command({"type": mt.CMD_IS_SERVER_READY})

    deadline = time.time() + 90.0
    while client._running and time.time() < deadline:
        time.sleep(0.25)

    if client._running:
        client.failed = True
        print("FAILED: timed out waiting for end-to-end compute flow")
        client.stop()
        raise SystemExit(1)

    if client.failed:
        raise SystemExit(1)

    required = [
        mt.EVT_STREAM_STARTED,
        mt.EVT_STREAM_WARMUP_STARTED,
        mt.EVT_STREAM_OFFICIAL_STARTED,
        mt.EVT_STREAM_STOPPED,
        mt.EVT_STREAM_DRAINED,
    ]
    missing = [event_type for event_type in required if event_type not in client.lifecycle_events]
    if missing:
        print("FAILED: missing lifecycle events", missing)
        raise SystemExit(1)

    if client.compute_before_official:
        print("FAILED: compute emitted before official start", client.compute_before_official)
        raise SystemExit(1)

    if client.compute_event_count < 2:
        print("FAILED: expected at least 2 compute events, got", client.compute_event_count)
        raise SystemExit(1)

    if not client.stream_drained_payload:
        print("FAILED: stream_drained payload missing")
        raise SystemExit(1)

    session_dir = client.stream_drained_payload.get("session_dir")
    archive_path = client.stream_drained_payload.get("session_archive_path")
    if archive_path:
        _assert_path_exists(archive_path, "session archive")
        _assert_archive_contains(archive_path, "diagnostics/session_diagnostics.json")
        _assert_archive_contains(archive_path, "diagnostics/session_diagnostics.jsonl")
    else:
        _assert_path_exists(session_dir, "session directory")
        diagnostics_dir = Path(session_dir) / "diagnostics"
        _assert_path_exists(str(diagnostics_dir / "session_diagnostics.json"), "session diagnostics summary")
        _assert_path_exists(str(diagnostics_dir / "session_diagnostics.jsonl"), "session diagnostics event log")

    print(
        "PASSED: compute_events=%s lifecycle=%s archive=%s"
        % (client.compute_event_count, client.lifecycle_events, archive_path)
    )
