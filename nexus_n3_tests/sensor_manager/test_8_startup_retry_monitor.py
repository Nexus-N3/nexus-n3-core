import argparse
import subprocess
import threading
import time
import random
from dataclasses import dataclass, replace

from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.plugins.runtime.sensor_runtime import resolve_installed_sensor_class


DEFAULT_LOCATIONS = [
    "LEFT_ANKLE",
    "RIGHT_ANKLE",
    "LEFT_THIGH",
    "RIGHT_THIGH",
    "CHEST",
    "LOWER_BACK",
    "HEAD",
    "UPPER_BACK",
]


@dataclass
class SensorStats:
    address: str
    location: str | None
    expected_rate_hz: int | None
    connect_time: float | None = None
    stream_start_command_time: float | None = None
    first_packet_time: float | None = None
    startup_first_sensor_timestamp: int | None = None
    startup_last_sensor_timestamp: int | None = None
    startup_first_wall_time: float | None = None
    startup_last_wall_time: float | None = None
    startup_packets_received: int = 0
    startup_gap_events: int = 0
    startup_estimated_dropped_packets: int = 0
    measurement_first_sensor_timestamp: int | None = None
    measurement_last_sensor_timestamp: int | None = None
    measurement_first_wall_time: float | None = None
    measurement_last_wall_time: float | None = None
    measurement_packets_received: int = 0
    gap_events: int = 0
    estimated_dropped_packets: int = 0

    def record_sample(self, sample, wall_time: float, measurement_active: bool):
        timestamp = getattr(sample, "timestamp", None)
        sampling_rate = getattr(sample, "sampling_rate", None)
        if self.expected_rate_hz is None and sampling_rate:
            self.expected_rate_hz = int(sampling_rate)

        if self.first_packet_time is None:
            self.first_packet_time = wall_time

        if measurement_active:
            self._record_measurement_sample(timestamp, wall_time)
        else:
            self._record_startup_sample(timestamp, wall_time)

    def _record_startup_sample(self, timestamp: int | None, wall_time: float):
        if self.startup_first_sensor_timestamp is None:
            self.startup_first_sensor_timestamp = timestamp
            self.startup_first_wall_time = wall_time
        else:
            self._record_startup_gap_if_needed(timestamp)

        self.startup_last_sensor_timestamp = timestamp
        self.startup_last_wall_time = wall_time
        self.startup_packets_received += 1

    def _record_measurement_sample(self, timestamp: int | None, wall_time: float):
        if self.measurement_first_sensor_timestamp is None:
            self.measurement_first_sensor_timestamp = timestamp
            self.measurement_first_wall_time = wall_time
        else:
            self._record_measurement_gap_if_needed(timestamp)

        self.measurement_last_sensor_timestamp = timestamp
        self.measurement_last_wall_time = wall_time
        self.measurement_packets_received += 1

    def _record_startup_gap_if_needed(self, timestamp: int | None):
        if timestamp is None or self.startup_last_sensor_timestamp is None:
            return
        expected_delta_us = self.expected_delta_us
        if expected_delta_us is None:
            return
        observed_delta_us = timestamp - self.startup_last_sensor_timestamp
        if observed_delta_us <= int(expected_delta_us * 1.5):
            return
        missing_packets = max(int(round(observed_delta_us / expected_delta_us)) - 1, 0)
        if missing_packets <= 0:
            return
        self.startup_gap_events += 1
        self.startup_estimated_dropped_packets += missing_packets

    def _record_measurement_gap_if_needed(self, timestamp: int | None):
        if timestamp is None or self.measurement_last_sensor_timestamp is None:
            return
        expected_delta_us = self.expected_delta_us
        if expected_delta_us is None:
            return
        observed_delta_us = timestamp - self.measurement_last_sensor_timestamp
        if observed_delta_us <= int(expected_delta_us * 1.5):
            return
        missing_packets = max(int(round(observed_delta_us / expected_delta_us)) - 1, 0)
        if missing_packets <= 0:
            return
        self.gap_events += 1
        self.estimated_dropped_packets += missing_packets

    def reset_measurement(self):
        self.measurement_first_sensor_timestamp = None
        self.measurement_last_sensor_timestamp = None
        self.measurement_first_wall_time = None
        self.measurement_last_wall_time = None
        self.measurement_packets_received = 0
        self.gap_events = 0
        self.estimated_dropped_packets = 0

    @property
    def expected_delta_us(self) -> float | None:
        if not self.expected_rate_hz:
            return None
        return 1_000_000.0 / float(self.expected_rate_hz)

    @property
    def startup_duration_seconds(self) -> float:
        if self.startup_first_wall_time is None or self.startup_last_wall_time is None:
            return 0.0
        return max(self.startup_last_wall_time - self.startup_first_wall_time, 0.0)

    @property
    def startup_observed_rate_hz(self) -> float:
        duration = self.startup_duration_seconds
        if duration <= 0:
            return 0.0
        return self.startup_packets_received / duration

    @property
    def measurement_duration_seconds(self) -> float:
        if self.measurement_first_wall_time is None or self.measurement_last_wall_time is None:
            return 0.0
        return max(self.measurement_last_wall_time - self.measurement_first_wall_time, 0.0)

    @property
    def observed_rate_hz(self) -> float:
        duration = self.measurement_duration_seconds
        if duration <= 0:
            return 0.0
        return self.measurement_packets_received / duration

    @property
    def time_to_first_packet_ms(self) -> float | None:
        if self.stream_start_command_time is None or self.first_packet_time is None:
            return None
        return max((self.first_packet_time - self.stream_start_command_time) * 1000.0, 0.0)


class StartupRetryPacketMonitorTest:
    """Retry startup until all sensors stabilize, then run a measurement capture."""

    def __init__(
        self,
        sensor_count: int,
        stream_seconds: int,
        timeout_seconds: int,
        startup_stability_window_seconds: float,
        startup_packets_required: int,
        startup_min_rate_hz: float,
        startup_min_observation_seconds: float,
        retry_delay_seconds: float,
        post_connect_settle_seconds: float,
        reset_hci_between_attempts: bool,
        hci_adapter: str,
    ):
        self.sensor_count = sensor_count
        self.stream_seconds = stream_seconds
        self.timeout_seconds = timeout_seconds

        self.max_start_attempts = 2
        self.startup_stability_window_seconds = startup_stability_window_seconds
        self.retry_delay_seconds = retry_delay_seconds
        self.identify_pause_seconds = 1.0
        self.startup_packets_required = startup_packets_required
        self.startup_min_rate_hz = startup_min_rate_hz
        self.startup_min_observation_seconds = startup_min_observation_seconds
        self.post_connect_settle_seconds = post_connect_settle_seconds
        self.reset_hci_between_attempts = reset_hci_between_attempts
        self.hci_adapter = hci_adapter

        self.manager = None
        self._done = None
        self._lock = threading.Lock()

        self.attempt_number = 0
        self.attempt_outcome = ""
        self.attempt_reason = ""

        self.connected_addresses = set()
        self.disconnected_addresses = set()
        self.stream_start_scheduled = False
        self.stream_started = False
        self.stream_active = False
        self.measurement_active = False
        self.stream_stop_sent = False
        self.disconnect_sent = False
        self.awaiting_disconnect = False
        self.startup_gate_started = False

        self.stats_by_address: dict[str, SensorStats] = {}
        self.location_by_address: dict[str, str | None] = {}
        self.attempt_summaries: list[dict] = []

    def run(self) -> int:
        final_success = False
        for attempt in range(1, self.max_start_attempts + 1):
            self._reset_attempt_state(attempt)
            self._start_attempt()

            completed = self._done.wait(timeout=self.timeout_seconds)
            if not completed:
                self._begin_retry_cleanup(
                    f"Attempt {attempt} timed out after {self.timeout_seconds}s."
                )
                self._done.wait(timeout=5)

            self._print_attempt_summary()
            self.attempt_summaries.append(
                {
                    "attempt": attempt,
                    "outcome": self.attempt_outcome,
                    "reason": self.attempt_reason,
                    "stats": {
                        address: replace(stats) for address, stats in self.stats_by_address.items()
                    },
                }
            )

            self._shutdown_manager()

            if self.attempt_outcome == "success":
                final_success = True
                break

            if attempt < self.max_start_attempts:
                if self.reset_hci_between_attempts:
                    self._reset_ble_controller()

                print(
                    f"Retrying startup after {self.retry_delay_seconds:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_start_attempts})."
                )
                time.sleep(self.retry_delay_seconds)

        self._print_overall_summary()
        return 0 if final_success else 1

    def _reset_attempt_state(self, attempt_number: int):
        self.manager = None
        self._done = threading.Event()
        self.attempt_number = attempt_number
        self.attempt_outcome = ""
        self.attempt_reason = ""

        self.connected_addresses = set()
        self.disconnected_addresses = set()
        self.stream_start_scheduled = False
        self.stream_started = False
        self.stream_active = False
        self.measurement_active = False
        self.stream_stop_sent = False
        self.disconnect_sent = False
        self.awaiting_disconnect = False
        self.startup_gate_started = False

        self.stats_by_address = {}
        self.location_by_address = {}

    def _start_attempt(self):
        sensors = self._build_sensors()
        self.manager = SensorManager()
        self.manager.init_sensor_manager(sensors)
        self.manager.register_listener("on_discover", self.on_discover)
        self.manager.register_listener("on_connected", self.on_connected)
        self.manager.register_listener("on_disconnected", self.on_disconnected)
        self.manager.register_listener("on_stream_started", self.on_stream_started)
        self.manager.register_listener("on_stream_stopped", self.on_stream_stopped)
        self.manager.register_listener("on_data", self.on_data)
        self.manager.register_listener("on_error", self.on_error)

        print("")
        print(
            f"Attempt {self.attempt_number}/{self.max_start_attempts}: "
            f"starting startup-retry monitor with {self.sensor_count} sensor(s)."
        )
        self.manager.discover_and_connect()

    def _build_sensors(self):
        sensor_cls = resolve_installed_sensor_class("Movella DOT")
        if sensor_cls is None:
            raise RuntimeError("Movella DOT plugin is not installed")

        locations = DEFAULT_LOCATIONS[: self.sensor_count]

        # Rotate order by attempt so retries do not recreate the same BLE connection layout.
        if locations:
            shift = (self.attempt_number - 1) % len(locations)
            locations = locations[shift:] + locations[:shift]

        sensors = []
        for location in locations:
            sensor = sensor_cls(None)
            sensors.append({"sensor": sensor, "meta": {"location": location}})
        return sensors

    def on_discover(self, payload):
        if isinstance(payload, dict) and payload.get("valid") is False:
            missing = payload.get("missing", [])
            self.attempt_reason = f"Discovery missing sensors: {missing}"
            self.attempt_outcome = "retry"
            print(f"FAILED: {self.attempt_reason}")
            self._done.set()
            return
        print(f"DISCOVERED: {payload}")

    def on_connected(self, payload):
        connected_time = time.monotonic()
        action = None
        with self._lock:
            for sensor in payload or []:
                if not getattr(sensor, "address", None):
                    continue
                self.connected_addresses.add(sensor.address)
                self.location_by_address[sensor.address] = getattr(sensor, "location", None)
                stats = self._get_or_create_stats(
                    sensor.address,
                    getattr(sensor, "location", None),
                    getattr(sensor, "sampling_rate", None),
                )
                if stats.connect_time is None:
                    stats.connect_time = connected_time

            print(
                "CONNECTED:",
                sorted(self.connected_addresses),
                f"({len(self.connected_addresses)}/{self.sensor_count})",
            )

            if len(self.connected_addresses) < self.sensor_count:
                self.attempt_outcome = "retry"
                self.attempt_reason = (
                    f"Only {len(self.connected_addresses)} of {self.sensor_count} sensors connected."
                )
                self.awaiting_disconnect = True
                if self.connected_addresses:
                    self.disconnect_sent = True
                    action = "disconnect"
                else:
                    action = "done"
            elif not self.stream_start_scheduled and not self.stream_started:
                self.stream_start_scheduled = True
                action = "schedule_start_stream"

        if action == "disconnect":
            print(f"FAILED: {self.attempt_reason}")
            print("Disconnecting partially connected sensors.")
            self.manager.disconnect_all()
        elif action == "done":
            print(f"FAILED: {self.attempt_reason}")
            self._done.set()
        elif action == "schedule_start_stream":
            print(
                "All sensors connected. "
                f"Waiting {self.post_connect_settle_seconds:.1f}s for BLE links to settle."
            )
            threading.Thread(target=self._delayed_start_all, daemon=True).start()

    def _delayed_start_all(self):
        time.sleep(self.post_connect_settle_seconds)

        with self._lock:
            if self.manager is None:
                return
            if self.attempt_outcome in {"retry", "fatal"}:
                return
            if self.stream_started or self.stream_stop_sent or self.disconnect_sent:
                return

            self.stream_started = True
            start_command_time = time.monotonic()
            for address in self.connected_addresses:
                self.stats_by_address[address].stream_start_command_time = start_command_time

        print(f"Starting stream for {self.stream_seconds}s.")
        self.manager.start_all()

    def on_stream_started(self, payload):
        print(f"STREAM STARTED: {payload}")
        should_start_gate = False
        with self._lock:
            self.stream_active = True
            if not self.startup_gate_started:
                self.startup_gate_started = True
                should_start_gate = True
        if should_start_gate:
            threading.Thread(target=self._evaluate_startup_stability, daemon=True).start()

    def on_data(self, payload):
        address = getattr(payload, "address", None)
        if not address:
            return

        wall_time = time.monotonic()
        with self._lock:
            stats = self._get_or_create_stats(
                address,
                getattr(payload, "location", None) or self.location_by_address.get(address),
                getattr(payload, "sampling_rate", None),
            )
            stats.record_sample(payload, wall_time, measurement_active=self.measurement_active)

    def on_stream_stopped(self, payload):
        print(f"STREAM STOPPED: {payload}")
        action = None
        with self._lock:
            self.stream_active = False
            if self.manager is None:
                return
            if self.attempt_outcome in {"retry", "fatal"}:
                if not self.disconnect_sent:
                    self.disconnect_sent = True
                    self.awaiting_disconnect = True
                    action = "disconnect"
            elif self.attempt_outcome == "success":
                if not self.disconnect_sent:
                    self.disconnect_sent = True
                    self.awaiting_disconnect = True
                    action = "identify_disconnect"

        if action == "disconnect":
            print("Disconnecting sensors.")
            self.manager.disconnect_all()
        elif action == "identify_disconnect":
            threading.Thread(
                target=self._identify_drop_sensors_then_disconnect,
                daemon=True,
            ).start()

    def on_disconnected(self, payload):
        with self._lock:
            disconnected_addresses = self._extract_disconnected_addresses(payload)
            for address in disconnected_addresses:
                if address:
                    self.disconnected_addresses.add(address)
            print(
                "DISCONNECTED:",
                sorted(self.disconnected_addresses),
                f"({len(self.disconnected_addresses)}/{self.sensor_count})",
            )

            if self.awaiting_disconnect and (
                len(self.disconnected_addresses) >= len(self.connected_addresses)
            ):
                self._done.set()

    def on_error(self, payload):
        self._begin_retry_cleanup(f"Sensor manager error: {payload}")

    def _evaluate_startup_stability(self):
        deadline = time.monotonic() + self.startup_stability_window_seconds
        print(
            "Waiting for startup stability gate:",
            f"up to {self.startup_stability_window_seconds:.1f}s.",
        )

        while time.monotonic() < deadline:
            with self._lock:
                addresses = sorted(self.connected_addresses)
                if len(addresses) < self.sensor_count:
                    return
                stable, unstable = self._evaluate_current_stability(addresses)
                if stable:
                    for address in addresses:
                        self.stats_by_address[address].reset_measurement()
                    self.measurement_active = True
                    self.attempt_outcome = "success"
                    print(
                        "Startup stability gate passed. "
                        f"Starting official measurement window for {self.stream_seconds}s."
                    )
                    threading.Thread(target=self._stop_stream_after_delay, daemon=True).start()
                    return
            time.sleep(0.1)

        with self._lock:
            addresses = sorted(self.connected_addresses)
            _stable, unstable = self._evaluate_current_stability(addresses)
            details = ", ".join(unstable) if unstable else "unknown startup instability"
        self._begin_retry_cleanup(f"Startup stability gate failed: {details}")

    def _evaluate_current_stability(self, addresses):
        unstable = []
        for address in addresses:
            stats = self.stats_by_address[address]
            if stats.first_packet_time is None:
                unstable.append(f"{address}: no_first_packet")
                continue
            if stats.startup_packets_received < self.startup_packets_required:
                unstable.append(
                    f"{address}: packets={stats.startup_packets_received}"
                )
                continue
            if stats.startup_duration_seconds < self.startup_min_observation_seconds:
                unstable.append(
                    f"{address}: warmup_window={stats.startup_duration_seconds:.2f}s"
                )
                continue
            if stats.startup_observed_rate_hz < self.startup_min_rate_hz:
                unstable.append(
                    f"{address}: rate={stats.startup_observed_rate_hz:.2f}Hz"
                )
                continue
            if stats.startup_gap_events > 0:
                unstable.append(
                    f"{address}: startup_gap_events={stats.startup_gap_events} "
                    f"startup_drops={stats.startup_estimated_dropped_packets}"
                )
                continue
        return len(unstable) == 0, unstable

    def _stop_stream_after_delay(self):
        time.sleep(self.stream_seconds)
        with self._lock:
            if self.stream_stop_sent or self.manager is None:
                return
            self.stream_stop_sent = True
        print("Stopping stream now.")
        self.manager.stop_all()

    def _begin_retry_cleanup(self, reason: str):
        action = None
        with self._lock:
            if self.attempt_outcome in {"retry", "fatal"} and self.awaiting_disconnect:
                return
            self.attempt_outcome = "retry"
            self.attempt_reason = reason
            self.measurement_active = False
            if self.stream_active and not self.stream_stop_sent:
                self.stream_stop_sent = True
                action = "stop_stream"
            elif self.connected_addresses and not self.disconnect_sent:
                self.disconnect_sent = True
                self.awaiting_disconnect = True
                action = "disconnect"
            else:
                action = "done"

        print(f"FAILED: {reason}")
        if action == "stop_stream":
            print("Stopping streams before retry cleanup.")
            self.manager.stop_all()
        elif action == "disconnect":
            print("Disconnecting sensors before retry.")
            self.manager.disconnect_all()
        elif action == "done":
            self._done.set()

    def _shutdown_manager(self):
        if self.manager is None:
            return

        with self._lock:
            already_disconnected = (
                bool(self.connected_addresses)
                and len(self.disconnected_addresses) >= len(self.connected_addresses)
            )
            stream_already_stopped = self.stream_stop_sent or not self.stream_active

        if not stream_already_stopped:
            try:
                self.manager.stop_all()
            except Exception:
                pass

        if not already_disconnected:
            try:
                self.manager.disconnect_all()
            except Exception:
                pass

        time.sleep(1)

        try:
            self.manager.stop_manager()
        except Exception:
            pass

        self.manager = None

    def _reset_ble_controller(self):
        print(f"Resetting BLE controller {self.hci_adapter} before next attempt.")

        commands = [
            ["sudo", "-n", "btmgmt", "-i", self.hci_adapter, "power", "off"],
            ["sudo", "-n", "btmgmt", "-i", self.hci_adapter, "power", "on"],
        ]

        for command in commands:
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    timeout=8,
                    capture_output=True,
                    text=True,
                )
                if completed.returncode != 0:
                    stderr = completed.stderr.strip()
                    stdout = completed.stdout.strip()
                    print(
                        "BLE controller reset command returned "
                        f"{completed.returncode}: {' '.join(command)}"
                    )
                    if stdout:
                        print(stdout)
                    if stderr:
                        print(stderr)
            except Exception as exc:
                print(f"BLE controller reset command failed: {' '.join(command)}: {exc}")

            time.sleep(1.0)

        time.sleep(2.0)

    def _identify_drop_sensors_then_disconnect(self):
        if self.manager is None:
            return
        with self._lock:
            addresses_to_identify = [
                address
                for address, stats in sorted(self.stats_by_address.items())
                if stats.estimated_dropped_packets > 0
            ]

        if addresses_to_identify:
            print(
                "Identifying sensors with packet drops before disconnect:",
                addresses_to_identify,
            )
            for address in addresses_to_identify:
                print(f"IDENTIFY: {address}")
                self.manager.identify(address)
                time.sleep(self.identify_pause_seconds)

        print("Disconnecting sensors.")
        self.manager.disconnect_all()

    def _extract_disconnected_addresses(self, payload):
        if payload is None:
            return []
        if isinstance(payload, str):
            return [payload]
        if isinstance(payload, dict):
            address = payload.get("address")
            return [address] if address else []
        addresses = []
        for item in payload:
            if isinstance(item, str):
                addresses.append(item)
            elif isinstance(item, dict):
                address = item.get("address")
                if address:
                    addresses.append(address)
        return addresses

    def _get_or_create_stats(self, address, location, expected_rate_hz):
        stats = self.stats_by_address.get(address)
        if stats is None:
            stats = SensorStats(
                address=address,
                location=location,
                expected_rate_hz=int(expected_rate_hz) if expected_rate_hz else None,
            )
            self.stats_by_address[address] = stats
            return stats
        if stats.location is None and location is not None:
            stats.location = location
        if stats.expected_rate_hz is None and expected_rate_hz is not None:
            stats.expected_rate_hz = int(expected_rate_hz)
        return stats

    def _print_attempt_summary(self):
        print("")
        print(
            f"Attempt {self.attempt_number} summary "
            f"outcome={self.attempt_outcome or 'unknown'}"
        )
        print(f"Reason: {self.attempt_reason or 'n/a'}")
        for address in sorted(self.stats_by_address):
            stats = self.stats_by_address[address]
            time_to_first_packet_ms = (
                "n/a"
                if stats.time_to_first_packet_ms is None
                else f"{stats.time_to_first_packet_ms:.1f}"
            )
            print(
                f"{address} location={stats.location} "
                f"startup_packets={stats.startup_packets_received} "
                f"startup_rate_hz={stats.startup_observed_rate_hz:.2f} "
                f"startup_gap_events={stats.startup_gap_events} "
                f"startup_drops={stats.startup_estimated_dropped_packets} "
                f"time_to_first_packet_ms={time_to_first_packet_ms} "
                f"packets={stats.measurement_packets_received} "
                f"observed_rate_hz={stats.observed_rate_hz:.2f} "
                f"expected_rate_hz={stats.expected_rate_hz} "
                f"gap_events={stats.gap_events} "
                f"estimated_dropped_packets={stats.estimated_dropped_packets}"
            )

    def _print_overall_summary(self):
        print("")
        print("Overall summary")
        for attempt in self.attempt_summaries:
            print(
                f"attempt={attempt['attempt']} "
                f"outcome={attempt['outcome'] or 'unknown'} "
                f"reason={attempt['reason'] or 'n/a'}"
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Retry startup until all sensors stabilize, then run an official "
            "packet monitor capture."
        )
    )
    parser.add_argument(
        "--sensor-count",
        type=int,
        default=6,
        choices=[1, 2, 3, 4, 5, 6, 7, 8],
        help="How many sensors to instantiate and connect.",
    )
    parser.add_argument(
        "--stream-seconds",
        type=int,
        default=30,
        help="How long the official measurement capture should run.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=90,
        help="Per-attempt timeout for discover/connect/stream/disconnect flow.",
    )
    parser.add_argument(
        "--startup-stability-window-seconds",
        type=float,
        default=5.0,
        help="How long to wait for all sensors to pass the startup stability gate.",
    )
    parser.add_argument(
        "--startup-packets-required",
        type=int,
        default=60,
        help="Minimum startup packets required per sensor before evaluating rate.",
    )
    parser.add_argument(
        "--startup-min-rate-hz",
        type=float,
        default=58.0,
        help="Minimum accepted startup packet rate per sensor.",
    )
    parser.add_argument(
        "--startup-min-observation-seconds",
        type=float,
        default=2.0,
        help="Minimum startup observation window per sensor before accepting stability.",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=5.0,
        help="Delay between failed startup attempts.",
    )
    parser.add_argument(
        "--post-connect-settle-seconds",
        type=float,
        default=2.0,
        help="Delay after all sensors connect before starting streams.",
    )
    parser.add_argument(
        "--reset-hci-between-attempts",
        action="store_true",
        help="Power-cycle the HCI adapter with btmgmt between failed attempts.",
    )
    parser.add_argument(
        "--hci-adapter",
        default="hci0",
        help="HCI adapter name used for optional reset, for example hci0.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.timeout_seconds <= args.stream_seconds:
        raise SystemExit("--timeout-seconds must be greater than --stream-seconds")

    exit_code = StartupRetryPacketMonitorTest(
        sensor_count=args.sensor_count,
        stream_seconds=args.stream_seconds,
        timeout_seconds=args.timeout_seconds,
        startup_stability_window_seconds=args.startup_stability_window_seconds,
        startup_packets_required=args.startup_packets_required,
        startup_min_rate_hz=args.startup_min_rate_hz,
        startup_min_observation_seconds=args.startup_min_observation_seconds,
        retry_delay_seconds=args.retry_delay_seconds,
        post_connect_settle_seconds=args.post_connect_settle_seconds,
        reset_hci_between_attempts=args.reset_hci_between_attempts,
        hci_adapter=args.hci_adapter,
    ).run()
    raise SystemExit(exit_code)
