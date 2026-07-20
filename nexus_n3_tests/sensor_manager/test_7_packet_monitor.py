import argparse
import threading
import time
from dataclasses import dataclass

from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.plugins.runtime.sensor_runtime import resolve_installed_sensor_class


DEFAULT_LOCATIONS = [
    "LEFT_ANKLE",
    "RIGHT_ANKLE",
    "LEFT_THIGH",
    "RIGH_THIGH",
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
    startup_packets_received: int = 0
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

        self.startup_packets_received += 1

        if self.first_packet_time is None:
            self.first_packet_time = wall_time

        if not measurement_active:
            return

        if self.measurement_first_sensor_timestamp is None:
            self.measurement_first_sensor_timestamp = timestamp
            self.measurement_first_wall_time = wall_time
        else:
            self._record_gap_if_needed(timestamp)

        self.measurement_last_sensor_timestamp = timestamp
        self.measurement_last_wall_time = wall_time
        self.measurement_packets_received += 1

    def reset_measurement(self):
        self.measurement_first_sensor_timestamp = None
        self.measurement_last_sensor_timestamp = None
        self.measurement_first_wall_time = None
        self.measurement_last_wall_time = None
        self.measurement_packets_received = 0
        self.gap_events = 0
        self.estimated_dropped_packets = 0

    def _record_gap_if_needed(self, timestamp: int | None):
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

    @property
    def expected_delta_us(self) -> float | None:
        if not self.expected_rate_hz:
            return None
        return 1_000_000.0 / float(self.expected_rate_hz)

    @property
    def stream_duration_seconds(self) -> float:
        if self.measurement_first_wall_time is None or self.measurement_last_wall_time is None:
            return 0.0
        return max(self.measurement_last_wall_time - self.measurement_first_wall_time, 0.0)

    @property
    def observed_rate_hz(self) -> float:
        duration = self.stream_duration_seconds
        if duration <= 0:
            return 0.0
        return self.measurement_packets_received / duration

    @property
    def time_to_first_packet_ms(self) -> float | None:
        if self.stream_start_command_time is None or self.first_packet_time is None:
            return None
        return max((self.first_packet_time - self.stream_start_command_time) * 1000.0, 0.0)


class PacketMonitorTest:
    """Standalone sensor-manager packet counter and drop monitor."""

    def __init__(self, sensor_count: int, stream_seconds: int, timeout_seconds: int):
        self.sensor_count = sensor_count
        self.stream_seconds = stream_seconds
        self.timeout_seconds = timeout_seconds
        self.identify_pause_seconds = 1.0
        self.warmup_packets_required = 10
        self.first_packet_timeout_seconds = 10.0
        self.warmup_timeout_seconds = 10.0

        self.manager = None
        self._done = threading.Event()
        self._lock = threading.Lock()

        self.connected_addresses = set()
        self.disconnected_addresses = set()
        self.stream_started = False
        self.stream_active = False
        self.measurement_active = False
        self.stream_stop_sent = False
        self.disconnect_sent = False
        self.awaiting_failure_disconnect = False
        self.failed = False
        self.failure_reason = ""
        self.warmup_gate_started = False

        self.stats_by_address: dict[str, SensorStats] = {}
        self.location_by_address: dict[str, str | None] = {}

    def run(self) -> int:
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

        print(f"Starting packet monitor with {self.sensor_count} sensor(s).")
        self.manager.discover_and_connect()

        completed = self._done.wait(timeout=self.timeout_seconds)
        if not completed:
            self._fail(f"Timed out after {self.timeout_seconds}s waiting for test completion.")

        self._shutdown_manager()
        self._print_summary()
        return 1 if self.failed else 0

    def _build_sensors(self):
        sensor_cls = resolve_installed_sensor_class("Movella DOT")
        if sensor_cls is None:
            raise RuntimeError("Movella DOT plugin is not installed")
        sensors = []
        for index in range(self.sensor_count):
            sensor = sensor_cls(None)
            location = DEFAULT_LOCATIONS[index] if index < len(DEFAULT_LOCATIONS) else None
            sensors.append(
                {
                    "sensor": sensor,
                    "meta": {
                        "location": location,
                    },
                }
            )
        return sensors

    def on_discover(self, payload):
        if isinstance(payload, dict) and payload.get("valid") is False:
            missing = payload.get("missing", [])
            self._fail(f"Discovery did not find all requested sensors. Missing: {missing}")
            return
        print(f"DISCOVERED: {payload}")

    def on_connected(self, payload):
        connected_time = time.monotonic()
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
                connected_count = len(self.connected_addresses)
                if connected_count > 0:
                    self.awaiting_failure_disconnect = True
                    self.disconnect_sent = True
                    reason = (
                        f"Only {connected_count} of {self.sensor_count} sensors connected."
                    )
                    self.failed = True
                    self.failure_reason = reason
                else:
                    reason = f"No sensors connected; expected {self.sensor_count}."
                    self.failed = True
                    self.failure_reason = reason
                    self._done.set()
                    print(f"FAILED: {reason}")
                    return
            else:
                reason = None

            if reason is not None and connected_count > 0:
                print(f"FAILED: {reason}")
                print("Disconnecting partially connected sensors.")
                self.manager.disconnect_all()
                return

            if len(self.connected_addresses) >= self.sensor_count and not self.stream_started:
                self.stream_started = True
                start_command_time = time.monotonic()
                for address in self.connected_addresses:
                    self.stats_by_address[address].stream_start_command_time = start_command_time
                print(f"Starting stream for {self.stream_seconds}s.")
                self.manager.start_all()

    def on_stream_started(self, payload):
        print(f"STREAM STARTED: {payload}")
        with self._lock:
            self.stream_active = True
            if self.warmup_gate_started:
                return
            self.warmup_gate_started = True
        threading.Thread(
            target=self._await_warmup_then_measure,
            daemon=True,
        ).start()

    def _stop_stream_after_delay(self):
        time.sleep(self.stream_seconds)
        with self._lock:
            if self.stream_stop_sent or self.manager is None:
                return
            self.stream_stop_sent = True
        print("Stopping stream now.")
        self.manager.stop_all()

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
        with self._lock:
            self.stream_active = False
            if self.disconnect_sent or self.manager is None:
                return
            self.disconnect_sent = True
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
            if not self.disconnect_sent and self.stream_started and self.stream_active:
                self.failed = True
                self.failure_reason = (
                    f"Unexpected disconnect during active stream: {sorted(disconnected_addresses)}"
                )
                self.awaiting_failure_disconnect = True
                self.disconnect_sent = True
                self.stream_active = False
                print(f"FAILED: {self.failure_reason}")
                print("Disconnecting remaining sensors.")
                self.manager.disconnect_all()
                return
            if self.awaiting_failure_disconnect:
                if len(self.disconnected_addresses) >= len(self.connected_addresses):
                    self._done.set()
                return
            if self.disconnect_sent and len(self.disconnected_addresses) >= self.sensor_count:
                self._done.set()

    def on_error(self, payload):
        self._fail(f"Sensor manager error: {payload}")

    def _fail(self, reason: str):
        with self._lock:
            if self.failed:
                return
            self.failed = True
            self.failure_reason = reason
        print(f"FAILED: {reason}")
        self._done.set()

    def _shutdown_manager(self):
        if self.manager is None:
            return
        try:
            self.manager.stop_all()
        except Exception:
            pass
        try:
            self.manager.disconnect_all()
        except Exception:
            pass
        time.sleep(1)
        try:
            self.manager.stop_manager()
        except Exception:
            pass

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

    def _await_warmup_then_measure(self):
        if self.manager is None:
            return

        addresses = sorted(self.connected_addresses)

        warmup_started_at = time.monotonic()
        print(
            "Waiting for warm-up gate:",
            f"{self.warmup_packets_required} packets per sensor.",
        )
        while True:
            with self._lock:
                missing_first_packet = [
                    address
                    for address in addresses
                    if self.stats_by_address[address].first_packet_time is None
                ]
                if missing_first_packet and (
                    time.monotonic() - warmup_started_at > self.first_packet_timeout_seconds
                ):
                    self.failed = True
                    self.failure_reason = (
                        "Sensors did not produce a first packet within "
                        f"{self.first_packet_timeout_seconds:.1f}s: {missing_first_packet}"
                    )
                    self.awaiting_failure_disconnect = True
                    self.disconnect_sent = True
                    self.stream_active = False
                    print(f"FAILED: {self.failure_reason}")
                    print("Disconnecting sensors after first-packet timeout.")
                    self.manager.disconnect_all()
                    return

                ready = all(
                    self.stats_by_address[address].startup_packets_received
                    >= self.warmup_packets_required
                    for address in addresses
                )
            if ready:
                break
            if time.monotonic() - warmup_started_at > self.warmup_timeout_seconds:
                with self._lock:
                    self.failed = True
                    self.failure_reason = (
                        "Warm-up gate timed out before all sensors reached "
                        f"{self.warmup_packets_required} packets."
                    )
                    self.awaiting_failure_disconnect = True
                    self.disconnect_sent = True
                    self.stream_active = False
                print(f"FAILED: {self.failure_reason}")
                print("Disconnecting sensors after warm-up timeout.")
                self.manager.disconnect_all()
                return
            time.sleep(0.05)

        with self._lock:
            for address in addresses:
                self.stats_by_address[address].reset_measurement()
            self.measurement_active = True

        print(
            "Warm-up gate complete. Starting official measurement window for",
            f"{self.stream_seconds}s.",
        )
        threading.Thread(target=self._stop_stream_after_delay, daemon=True).start()

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
                expected_rate_hz=expected_rate_hz,
            )
            self.stats_by_address[address] = stats
            return stats

        if stats.location is None and location is not None:
            stats.location = location
        if stats.expected_rate_hz is None and expected_rate_hz is not None:
            stats.expected_rate_hz = int(expected_rate_hz)
        return stats

    def _print_summary(self):
        print("")
        print("Packet monitor summary")
        print(f"Requested sensor count: {self.sensor_count}")
        print(f"Connected sensor count: {len(self.connected_addresses)}")
        print(f"Disconnected sensor count: {len(self.disconnected_addresses)}")
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
                f"time_to_first_packet_ms={time_to_first_packet_ms} "
                f"packets={stats.measurement_packets_received} "
                f"observed_rate_hz={stats.observed_rate_hz:.2f} "
                f"expected_rate_hz={stats.expected_rate_hz} "
                f"gap_events={stats.gap_events} "
                f"estimated_dropped_packets={stats.estimated_dropped_packets}"
            )

        if self.failure_reason:
            print(f"Failure reason: {self.failure_reason}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Standalone sensor-manager packet monitor for 1, 2, 4, 6, 7, or 8 Movella DOT sensors."
    )
    parser.add_argument(
        "--sensor-count",
        type=int,
        default=2,
        choices=[1, 2, 3, 4, 5, 6, 7, 8],
        help="How many sensors to instantiate and connect.",
    )
    parser.add_argument(
        "--stream-seconds",
        type=int,
        default=30,
        help="How long to stream before stopping.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=90,
        help="Overall timeout for the full discover/connect/stream/disconnect flow.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.timeout_seconds <= args.stream_seconds:
        raise SystemExit("--timeout-seconds must be greater than --stream-seconds")

    exit_code = PacketMonitorTest(
        sensor_count=args.sensor_count,
        stream_seconds=args.stream_seconds,
        timeout_seconds=args.timeout_seconds,
    ).run()
    raise SystemExit(exit_code)
