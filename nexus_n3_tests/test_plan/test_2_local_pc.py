import argparse
import csv
import threading
import time
from datetime import datetime
from pathlib import Path

import zmq

from nexus_n3.gateway.messaging import message_types as mt


class Client:
    """
    Example Nexus N3 Core client using ZeroMQ.
    """

    def __init__(
        self,
        cmd_pub_addr="tcp://localhost:5555",
        evt_sub_addr="tcp://localhost:5556",
        stop_stage="full",
        stream_seconds=20,
    ):

        self.ctx = zmq.Context()

        # PUB socket for sending commands to the server
        self.cmd_pub = self.ctx.socket(zmq.PUB)
        self.cmd_pub.setsockopt(zmq.LINGER, 0)
        self.cmd_pub.connect(cmd_pub_addr)

        # SUB socket for receiving events from the server
        self.evt_sub = self.ctx.socket(zmq.SUB)
        self.evt_sub.setsockopt(zmq.LINGER, 0)
        self.evt_sub.setsockopt(zmq.RCVTIMEO, 250)
        self.evt_sub.connect(evt_sub_addr)
        self.evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self._running = False
        self._event_thread = None
        self._stop_lock = threading.Lock()
        self.subjects = []
        self.pending_connect = set()
        self.stop_stage = stop_stage
        self.stream_seconds = stream_seconds
        self.identify_wait_seconds = 10
        self.identify_sent = False
        self.identify_shutdown_started = False
        self.stream_sequence_started = False
        self.viewer_countdown_started = False
        self.identified_subject_locations = set()
        self.connected = False
        self.awaiting_disconnect_stop = False
        self.stream_started = False
        self.stream_stop_scheduled = False
        self.expected_compute_cadence_seconds = 5.0
        self.expected_window_samples = 300
        self._last_compute_ts = {}
        self._cadence_log_lock = threading.Lock()
        self.cadence_csv_path = Path("compute_cadence.csv").resolve()
        self._init_cadence_csv()

    def start(self):
        """Start the client event loop in a background thread."""
        self._running = True
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()

    def _event_loop(self):
        """Background loop that listens for gateway events."""
        while self._running:
            try:
                msg = self.evt_sub.recv_json()
                self.handle_event(msg)
            except zmq.Again:
                continue
            except zmq.ZMQError:
                if not self._running:
                    break
                print("ZeroMQ error in event loop")
            except Exception as e:
                print("Error receiving event:", e)

    def handle_event(self, event: dict):
        print("SYSTEM EVENT:", event.get("type"))

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
            if self.stop_stage == "discover":
                print("[STAGE] Discovery complete. Stopping.")
                self.stop()
                return

            self.send_command({"type": mt.CMD_CONNECT_TO_ALL})
            for subject_info in payload:
                self.pending_connect.discard(subject_info["subject_id"])

        elif evt_type == mt.EVT_SENSOR_CONNECTED:
            self.connected = True
            if self.stop_stage == "connect":
                print("[STAGE] Connection complete. Disconnecting before stop.")
                self._disconnect_then_stop()
                return

            # Run identify sequence once after first connection event.
            if not self.identify_sent:
                self.identify_sent = True
                threading.Thread(target=self._run_identify_sequence, daemon=True).start()

            # Some setups do not emit EVT_SENSOR_IDENTIFIED reliably.
            # For identify stage, disconnect after issuing identify commands.
            if self.stop_stage == "identify" and not self.identify_shutdown_started:
                self.identify_shutdown_started = True
                threading.Thread(target=self._identify_then_disconnect, daemon=True).start()

        elif evt_type == mt.EVT_SENSOR_IDENTIFIED:
            subject_id = payload.get("subject_id", "unknown")
            location = payload.get("location", "unknown")
            self.identified_subject_locations.add((subject_id, location))

            if self.stop_stage == "identify":
                print("[STAGE] Identification complete. Disconnecting before stop.")
                self._disconnect_then_stop()
                return

        elif evt_type == mt.EVT_STREAM_STARTED:
            print(f"stream started for {payload}")
            self.stream_started = True
            if not self.viewer_countdown_started:
                self.viewer_countdown_started = True
                threading.Thread(target=self._viewer_countdown, daemon=True).start()
            if self.stop_stage in {"stream", "full"} and not self.stream_stop_scheduled:
                self.stream_stop_scheduled = True
                threading.Thread(target=self._stop_stream_after_duration, daemon=True).start()

        elif evt_type == mt.EVT_STREAM_STOPPED:
            print(f"stream stopped for {payload}")
            self.send_command({"type": mt.CMD_DISCONNECT_ALL})
            if self.stop_stage == "stream":
                self.awaiting_disconnect_stop = True
                return

        elif evt_type == mt.EVT_SENSOR_DISCONNECTED:
            print("Sensor's Disconnect", payload)
            self.connected = False
            if self.awaiting_disconnect_stop:
                self.stop()
                return
            if self.stop_stage == "full":
                self.stop()
        
        elif evt_type == mt.EVT_COMPUTE_RESULT:
            now = time.monotonic()
            key = (
                payload.get("subject_id"),
                payload.get("algorithm_name"),
                payload.get("address"),
            )
            prev = self._last_compute_ts.get(key)
            self._last_compute_ts[key] = now

            count = payload.get("result", {}).get("result_count")
            if prev is None:
                self._write_cadence_row(payload, count, None, "FIRST")
                print(
                    "compute window received "
                    f"count={count} cadence=first inferred_rate_hz=unknown"
                )
            else:
                delta = now - prev
                drift = abs(delta - self.expected_compute_cadence_seconds)
                status = "OK" if drift <= 0.75 else "DRIFT"
                inferred_rate_hz = self.expected_window_samples / delta if delta > 0 else 0.0
                self._write_cadence_row(payload, count, delta, status)
                print(
                    "compute window received "
                    f"count={count} cadence={delta:.3f}s "
                    f"(expected {self.expected_compute_cadence_seconds:.1f}s) "
                    f"inferred_rate_hz={inferred_rate_hz:.2f} [{status}]"
                )

        # to view the results
        elif evt_type == mt.EVT_INTERMEDIATE_RESULT:
            print("intermediate window received")

        elif evt_type == mt.EVT_ERROR:
            print(f"ERROR: {payload}")
            self.stop()

    def _handle_stream_sequence(self):
        """Start streaming after identify sequence finishes."""
        self.send_command({
            "type": mt.CMD_START_STREAM_FOR_ALL,
            "payload": {"tag": "test_activity"},
        })

    def _run_identify_sequence(self):
        identify_targets = []
        for sub in self.subjects:
            for sensor_conf in sub.get("sensors", []):
                for location in sensor_conf.get("locations", []):
                    identify_targets.append((sub["subject_id"], location))

        total = len(identify_targets)
        if total == 0:
            print("No identify targets configured, starting stream immediately.")
            if self.stop_stage in {"stream", "full"} and not self.stream_sequence_started:
                self.stream_sequence_started = True
                self._handle_stream_sequence()
            return

        for idx, (subject_id, location) in enumerate(identify_targets, start=1):
            print(f"[IDENTIFY {idx}/{total}] subject={subject_id} location={location} for {self.identify_wait_seconds}s")
            self.send_command({
                "type": mt.CMD_IDENTIFY_SENSOR,
                "payload": {"subject_id": subject_id, "location": location},
            })
            time.sleep(self.identify_wait_seconds)

        print("Identify sequence complete.")
        if self.stop_stage in {"stream", "full"} and not self.stream_sequence_started:
            self.stream_sequence_started = True
            self._handle_stream_sequence()

    def _viewer_countdown(self):
        for remaining in range(5, 0, -1):
            print(f"[VIEWER COUNTDOWN] {remaining}")
            time.sleep(1)
        print("[VIEWER COUNTDOWN] complete")

    def _stop_stream_after_duration(self):
        """Stop streaming after configured duration starting from EVT_STREAM_STARTED."""
        time.sleep(self.stream_seconds)
        self.send_command({"type": mt.CMD_STOP_STREAM_FOR_ALL})

    def send_command(self, command: dict):
        """Send a command message to the gateway."""
        self.cmd_pub.send_json(command)

    def _disconnect_then_stop(self):
        """Disconnect sensors first, then stop when disconnection event arrives."""
        if self.connected:
            self.awaiting_disconnect_stop = True
            self.send_command({"type": mt.CMD_DISCONNECT_ALL})
        else:
            self.stop()

    def _identify_then_disconnect(self):
        """Allow identify action to run briefly, then disconnect and stop."""
        time.sleep(3)
        self._disconnect_then_stop()

    def _init_cadence_csv(self):
        with self._cadence_log_lock, open(self.cadence_csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "logged_at",
                    "subject_id",
                    "algorithm_name",
                    "address",
                    "result_count",
                    "cadence_seconds",
                    "expected_cadence_seconds",
                    "status",
                ]
            )

    def _write_cadence_row(self, payload, result_count, cadence_seconds, status):
        with self._cadence_log_lock, open(self.cadence_csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    datetime.now().isoformat(timespec="seconds"),
                    payload.get("subject_id"),
                    payload.get("algorithm_name"),
                    payload.get("address"),
                    result_count,
                    "" if cadence_seconds is None else f"{cadence_seconds:.6f}",
                    f"{self.expected_compute_cadence_seconds:.1f}",
                    status,
                ]
            )

    def stop(self):
        """Stop the client and clean up ZeroMQ resources."""
        with self._stop_lock:
            if not self._running:
                return
            print("stopping client...")
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
    parser = argparse.ArgumentParser(description="Nexus N3 Core local PC staged test plan (sequential identify)")
    stages = parser.add_mutually_exclusive_group()
    stages.add_argument("--discover", action="store_true", help="Run until discovery then stop")
    stages.add_argument("--connect", action="store_true", help="Run until connect then stop")
    stages.add_argument("--identify", action="store_true", help="Run until identify then stop")
    stages.add_argument(
        "--stream",
        type=int,
        metavar="SECONDS",
        help="Run through streaming and stop after SECONDS (example: --stream 60)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    stop_stage = "full"
    stream_seconds = 20
    if args.discover:
        stop_stage = "discover"
    elif args.connect:
        stop_stage = "connect"
    elif args.identify:
        stop_stage = "identify"
    elif args.stream is not None:
        stop_stage = "stream"
        if args.stream <= 0:
            raise ValueError("--stream must be a positive integer")
        stream_seconds = args.stream

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

    client = Client(stop_stage=stop_stage, stream_seconds=stream_seconds)
    client.subjects = subjects
    client.start()
    time.sleep(1)  # allow sockets to connect

    client.send_command({"type": mt.CMD_IS_SERVER_READY})

    try:
        while client._running:
            time.sleep(1)
    except KeyboardInterrupt:
        client.stop()
