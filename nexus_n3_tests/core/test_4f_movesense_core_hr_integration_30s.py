import argparse
import csv
import io
import threading
import time
import zipfile
from pathlib import Path

import zmq

from nexus_n3.gateway.messaging import message_types as mt


DEFAULT_SUBJECTS = [
    {
        "subject_id": "subject1",
        "sensors": [
            {
                "local_name": "Movesense",
                "number_of": 1,
                "compute_algorithm": {},
                "locations": ["CHEST"],
            },
            {
                "local_name": "Core 2",
                "number_of": 1,
                "compute_algorithm": {},
                "locations": ["CHEST"],
            },
        ],
    }
]


class Client:
    """Live Movesense -> CORE 2 routing integration test."""

    def __init__(
        self,
        cmd_pub_addr="tcp://localhost:5555",
        evt_sub_addr="tcp://localhost:5556",
        stream_seconds=30,
        subjects=None,
        session_label="movesense_core_hr_integration_30s",
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
        self.session_label = session_label

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

        self.official_started = False
        self.stream_drained_payload = None

        self.failed = False
        self.failure_reason = ""

    def start(self):
        self._running = True
        self._event_thread = threading.Thread(
            target=self._event_loop,
            daemon=True,
        )
        self._event_thread.start()

    def run(self, timeout_seconds):
        self.start()

        # Give PUB/SUB sockets time to establish.
        time.sleep(1)

        self.send_command({"type": mt.CMD_IS_SERVER_READY})

        completed = self._done.wait(timeout=timeout_seconds)

        if not completed:
            self._fail(
                f"Timed out after {timeout_seconds}s "
                "waiting for Movesense + CORE integration session."
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
                self._fail(
                    "ZeroMQ error while waiting for gateway events."
                )

            except Exception as exc:
                self._fail(
                    f"Unhandled client exception: {exc}"
                )

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
                        "init_label": self.session_label,
                    },
                }
            )
            return

        if evt_type == mt.EVT_SYSTEM_INITIALIZED:
            self.send_command(
                {"type": mt.CMD_DISCOVER_SENSORS}
            )
            return

        if evt_type == mt.EVT_SENSORS_DISCOVERED:
            discovered = self._collect_addresses(
                payload,
                "discovered_sensors",
            )
            self.discovered_addresses.update(discovered)

            print(
                "DISCOVERED:",
                sorted(self.discovered_addresses),
                f"({len(self.discovered_addresses)}/"
                f"{self.expected_sensor_count})",
            )

            if (
                len(self.discovered_addresses)
                >= self.expected_sensor_count
                and not self.connect_sent
            ):
                self.connect_sent = True
                self.send_command(
                    {"type": mt.CMD_CONNECT_TO_ALL}
                )

            return

        if evt_type == mt.EVT_SENSOR_CONNECTED:
            connected = self._collect_addresses(
                payload,
                "connected_sensors",
            )
            self.connected_addresses.update(connected)

            print(
                "CONNECTED:",
                sorted(self.connected_addresses),
                f"({len(self.connected_addresses)}/"
                f"{self.expected_sensor_count})",
            )

            if (
                len(self.connected_addresses)
                >= self.expected_sensor_count
                and not self.stream_start_sent
            ):
                self.stream_start_sent = True

                self.send_command(
                    {
                        "type": mt.CMD_START_STREAM_FOR_ALL,
                        "payload": {
                            "tag": self.session_label,
                        },
                    }
                )

            return

        if evt_type == mt.EVT_STREAM_STARTED:
            print("PHYSICAL STREAM STARTED")
            return

        if evt_type == mt.EVT_STREAM_WARMUP_STARTED:
            print("STARTUP WARMUP STARTED")
            return

        if evt_type == mt.EVT_STREAM_OFFICIAL_STARTED:
            print(
                "OFFICIAL STREAM STARTED:",
                payload,
            )

            self.official_started = True

            if not self.stop_timer_started:
                self.stop_timer_started = True

                threading.Thread(
                    target=self._stop_stream_after_delay,
                    daemon=True,
                ).start()

            return

        if evt_type == mt.EVT_STREAM_STARTUP_RETRY:
            print(
                "STARTUP RETRY:",
                payload,
            )
            return

        if evt_type == mt.EVT_STREAM_STARTUP_FAILED:
            self._fail(
                f"Startup gate failed: {payload}"
            )
            return

        if evt_type == mt.EVT_STREAM_STOPPED:
            print(
                "STREAM STOPPED:",
                payload,
            )

            # Do not disconnect yet.
            #
            # We want FileManager to drain and finalize the
            # session before disconnecting.
            return

        if evt_type == mt.EVT_STREAM_DRAINED:
            print(
                "STREAM DRAINED:",
                payload,
            )

            if payload.get("status", "ok") != "ok":
                self._fail(
                    "Stream drain failed: "
                    f"{payload.get('reason') or payload}"
                )
                return

            self.stream_drained_payload = payload

            if not self.disconnect_sent:
                self.disconnect_sent = True
                self.send_command(
                    {"type": mt.CMD_DISCONNECT_ALL}
                )

            return

        if evt_type == mt.EVT_SENSOR_DISCONNECTED:
            disconnected = (
                payload.get("disconnected_sensors")
                or []
            )

            if isinstance(disconnected, dict):
                disconnected = [disconnected]

            for sensor in disconnected:
                if isinstance(sensor, str):
                    self.disconnected_addresses.add(sensor)

                elif isinstance(sensor, dict):
                    address = sensor.get("address")

                    if address:
                        self.disconnected_addresses.add(
                            address
                        )

            print(
                "DISCONNECTED:",
                sorted(self.disconnected_addresses),
                f"({len(self.disconnected_addresses)}/"
                f"{self.expected_sensor_count})",
            )

            if (
                self.disconnect_sent
                and len(self.disconnected_addresses)
                >= self.expected_sensor_count
            ):
                self._done.set()

            return

        if evt_type == mt.EVT_ERROR:
            self._fail(
                f"Nexus N3 Core error: {payload}"
            )

    def _stop_stream_after_delay(self):
        print(
            f"Official acquisition running for "
            f"{self.stream_seconds}s..."
        )

        time.sleep(self.stream_seconds)

        if not self.stream_stop_sent:
            self.stream_stop_sent = True

            self.send_command(
                {"type": mt.CMD_STOP_STREAM_FOR_ALL}
            )

    def _collect_addresses(
        self,
        payload,
        field_name,
    ):
        addresses = set()

        for subject in payload.get("subjects", []):
            for sensor in subject.get(
                field_name,
                [],
            ):
                if isinstance(sensor, str):
                    addresses.add(sensor)

                elif isinstance(sensor, dict):
                    address = sensor.get("address")

                    if address:
                        addresses.add(address)

        return addresses

    def send_command(self, command):
        print(
            "COMMAND:",
            command["type"],
            command.get("payload", {}),
        )

        self.cmd_pub.send_json(command)

    def _fail(self, reason):
        if self.failed:
            return

        self.failed = True
        self.failure_reason = reason

        print(
            f"FAILED: {reason}"
        )

        self._done.set()

    def stop(self):
        with self._stop_lock:
            if not self._running:
                return

            self._running = False

            if (
                self._event_thread
                and threading.current_thread()
                is not self._event_thread
            ):
                self._event_thread.join()

            try:
                self.evt_sub.close()
            except Exception:
                pass

            try:
                self.cmd_pub.close()
            except Exception:
                pass

            try:
                self.ctx.term()
            except Exception:
                pass


def _read_csv_text(name, text):
    reader = csv.DictReader(
        io.StringIO(text)
    )

    rows = list(reader)

    return {
        "name": name,
        "fields": set(reader.fieldnames or []),
        "rows": rows,
    }


def _load_raw_csvs(stream_drained_payload):
    archive_value = stream_drained_payload.get(
        "session_archive_path"
    )

    session_dir_value = stream_drained_payload.get(
        "session_dir"
    )

    files = []

    if archive_value:
        archive_path = Path(archive_value)

        if not archive_path.exists():
            raise RuntimeError(
                f"Session archive does not exist: "
                f"{archive_path}"
            )

        with zipfile.ZipFile(archive_path) as archive:
            for name in archive.namelist():
                if not name.endswith(".csv"):
                    continue

                if "/raw/" not in f"/{name}":
                    continue

                text = archive.read(name).decode(
                    "utf-8-sig"
                )

                files.append(
                    _read_csv_text(
                        name,
                        text,
                    )
                )

        return files

    if not session_dir_value:
        raise RuntimeError(
            "stream_drained did not contain "
            "session_dir or session_archive_path"
        )

    session_dir = Path(
        session_dir_value
    )

    if not session_dir.exists():
        raise RuntimeError(
            f"Session directory does not exist: "
            f"{session_dir}"
        )

    for path in session_dir.rglob("*.csv"):
        if "raw" not in path.parts:
            continue

        files.append(
            _read_csv_text(
                str(path),
                path.read_text(
                    encoding="utf-8-sig"
                ),
            )
        )

    return files


def _float_value(row, key):
    raw = row.get(key)

    if raw is None:
        return None

    raw = str(raw).strip()

    if not raw:
        return None

    try:
        return float(raw)
    except ValueError:
        return None


def validate_movesense_core_integration(
    stream_drained_payload,
):
    raw_files = _load_raw_csvs(
        stream_drained_payload
    )

    print("\nRAW FILES:")

    for item in raw_files:
        print(
            " ",
            item["name"],
            sorted(item["fields"]),
        )

    core_file = next(
        (
            item
            for item in raw_files
            if {
                "core_temperature",
                "heart_rate_state",
                "heart_rate",
            }.issubset(item["fields"])
        ),
        None,
    )

    if core_file is None:
        raise RuntimeError(
            "Could not identify CORE 2 raw CSV."
        )

    movesense_file = next(
        (
            item
            for item in raw_files
            if "heart_rate" in item["fields"]
            and "core_temperature"
            not in item["fields"]
        ),
        None,
    )

    if movesense_file is None:
        raise RuntimeError(
            "Could not identify Movesense HR "
            "raw CSV."
        )

    movesense_hr = []

    for row in movesense_file["rows"]:
        timestamp = _float_value(
            row,
            "timestamp",
        )

        hr = _float_value(
            row,
            "heart_rate",
        )

        if timestamp is None or hr is None:
            continue

        movesense_hr.append(
            (timestamp, hr)
        )

    core_hr = []

    for row in core_file["rows"]:
        timestamp = _float_value(
            row,
            "timestamp",
        )

        state = _float_value(
            row,
            "heart_rate_state",
        )

        hr = _float_value(
            row,
            "heart_rate",
        )

        if (
            timestamp is None
            or state is None
            or hr is None
        ):
            continue

        if int(state) != 2:
            continue

        core_hr.append(
            (timestamp, int(hr))
        )

    print(
        "\nMOVESENSE HR SAMPLES:",
        len(movesense_hr),
    )

    print(
        "CORE EXTERNAL-HR SAMPLES:",
        len(core_hr),
    )

    if len(movesense_hr) < 5:
        raise RuntimeError(
            "Too few Movesense HR samples "
            f"persisted: {len(movesense_hr)}"
        )

    if len(core_hr) < 5:
        raise RuntimeError(
            "CORE did not report enough "
            "heart_rate_state=2 samples: "
            f"{len(core_hr)}"
        )

    matched = 0

    for core_timestamp, core_value in core_hr:
        recent_movesense = [
            int(round(hr))
            for timestamp, hr in movesense_hr
            if 0
            <= core_timestamp - timestamp
            <= 3000
        ]

        if core_value in recent_movesense:
            matched += 1

    print(
        "CORE HR VALUES MATCHING RECENT "
        "MOVESENSE HR:",
        f"{matched}/{len(core_hr)}",
    )

    if matched < 5:
        raise RuntimeError(
            "CORE external HR values do not "
            "sufficiently match recent "
            "Movesense HR samples."
        )

    print(
        "\nINTEGRATION VALIDATED:"
    )

    print(
        "Movesense HR -> "
        "Nexus N3 routing -> "
        "CORE consume_input -> "
        "CORE Control Point -> "
        "CORE measurement heart_rate_state=2"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run a live Movesense -> CORE 2 "
            "Nexus N3 integration session."
        )
    )

    parser.add_argument(
        "--cmd-pub-addr",
        default="tcp://localhost:5555",
    )

    parser.add_argument(
        "--evt-sub-addr",
        default="tcp://localhost:5556",
    )

    parser.add_argument(
        "--stream-seconds",
        type=int,
        default=30,
    )

    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=180,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.stream_seconds <= 0:
        raise SystemExit(
            "--stream-seconds must be "
            "a positive integer"
        )

    if (
        args.timeout_seconds
        <= args.stream_seconds
    ):
        raise SystemExit(
            "--timeout-seconds must be greater "
            "than --stream-seconds"
        )

    client = Client(
        cmd_pub_addr=args.cmd_pub_addr,
        evt_sub_addr=args.evt_sub_addr,
        stream_seconds=args.stream_seconds,
    )

    ok = client.run(
        timeout_seconds=args.timeout_seconds
    )

    if not ok:
        raise SystemExit(
            client.failure_reason
            or "Movesense/CORE integration "
            "session failed"
        )

    if not client.official_started:
        raise SystemExit(
            "FAILED: official stream never started"
        )

    if not client.stream_drained_payload:
        raise SystemExit(
            "FAILED: no stream_drained payload"
        )

    try:
        validate_movesense_core_integration(
            client.stream_drained_payload
        )
    except Exception as exc:
        raise SystemExit(
            f"FAILED integration validation: {exc}"
        ) from exc

    print(
        "\nPASSED: Movesense -> CORE 2 "
        "integration test"
    )