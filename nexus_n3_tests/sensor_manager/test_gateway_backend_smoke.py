import argparse
import os
import threading
import time

from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig
from nexus_n3.plugins.runtime.sensor_runtime import resolve_installed_sensor_class


DEFAULT_LOCATIONS = [
    "LOWER_BACK",
    "LEFT_ANKLE",
    "RIGHT_ANKLE",
    "LEFT_THIGH",
    "RIGH_THIGH",
    "CHEST",
    "HEAD",
    "UPPER_BACK",
]


class GatewayBackendSmokeTest:
    """Live sensor-manager smoke test for the BLE gateway backend."""

    def __init__(
        self,
        sensor_count: int,
        stream_seconds: float,
        timeout_seconds: float,
        ble_backend: str,
    ):
        self.sensor_count = sensor_count
        self.stream_seconds = stream_seconds
        self.timeout_seconds = timeout_seconds
        self.ble_backend = ble_backend

        self.manager = None
        self.done = threading.Event()
        self.lock = threading.Lock()

        self.failed = False
        self.failure_reason = ""
        self.discovered = []
        self.connected = []
        self.disconnected = set()
        self.stream_started = False
        self.stream_stopped = False
        self.sample_count = 0
        self.last_sample = None

    def run(self) -> int:
        os.environ["BLE_BACKEND"] = self.ble_backend
        config = BLERuntimeConfig.from_env()
        backend = os.environ.get("BLE_BACKEND", "").strip().lower()
        if backend not in {"gateway", "nexus_ble_gateway", "ble_gateway"}:
            print(
                "This smoke test requires the BLE gateway backend. "
                "Run with --ble-backend nexus_ble_gateway or set BLE_BACKEND accordingly."
            )
            return 2

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

        print(
            "Starting BLE gateway backend smoke test: "
            f"sensors={self.sensor_count} stream_seconds={self.stream_seconds:.1f} "
            f"backend={config.backend_label} port={config.gateway_serial_port}"
        )
        self.manager.discover_and_connect()

        completed = self.done.wait(timeout=self.timeout_seconds)
        if not completed and not self.failed:
            self._fail(f"Timed out after {self.timeout_seconds:.1f}s")

        self._shutdown_manager()

        if self.failed:
            print(f"FAILED: {self.failure_reason}")
            return 1

        print(
            "PASSED: "
            f"discovered={len(self.discovered)} connected={len(self.connected)} "
            f"samples={self.sample_count}"
        )
        return 0

    def _build_sensors(self):
        sensor_cls = resolve_installed_sensor_class("Movella DOT")
        if sensor_cls is None:
            raise RuntimeError("Movella DOT plugin is not installed")
        sensors = []
        for index in range(self.sensor_count):
            sensor = sensor_cls(None)
            location = DEFAULT_LOCATIONS[index] if index < len(DEFAULT_LOCATIONS) else None
            sensors.append({"sensor": sensor, "meta": {"location": location}})
        return sensors

    def _shutdown_manager(self):
        if not self.manager:
            return
        try:
            self.manager.stop_manager()
        except Exception as exc:
            print(f"[WARN] stop_manager raised: {exc}")

    def _fail(self, reason: str):
        with self.lock:
            if self.failed:
                return
            self.failed = True
            self.failure_reason = reason
            self.done.set()

    def on_discover(self, payload):
        if isinstance(payload, dict) and payload.get("valid") is False:
            self._fail(f"Discovery missing sensors: {payload.get('missing', [])}")
            return
        with self.lock:
            self.discovered = list(payload or [])
        print(f"DISCOVERED: {len(self.discovered)} sensor(s)")

    def on_connected(self, payload):
        with self.lock:
            self.connected = list(payload or [])
            connected_count = len(self.connected)
        print(f"CONNECTED: {connected_count}/{self.sensor_count}")
        if connected_count < self.sensor_count:
            self._fail(f"Expected {self.sensor_count} connected sensors, got {connected_count}")
            return
        self.manager.start_all()

    def on_stream_started(self, payload):
        with self.lock:
            if self.stream_started:
                return
            self.stream_started = True
        print(f"STREAM STARTED: {payload}")
        threading.Thread(target=self._stop_stream_after_delay, daemon=True).start()

    def _stop_stream_after_delay(self):
        time.sleep(self.stream_seconds)
        if self.manager and not self.failed:
            print("Stopping stream...")
            self.manager.stop_all()

    def on_data(self, payload):
        with self.lock:
            self.sample_count += 1
            self.last_sample = payload
        if self.sample_count == 1:
            print(f"FIRST SAMPLE: address={getattr(payload, 'address', None)}")

    def on_stream_stopped(self, payload):
        with self.lock:
            self.stream_stopped = True
            sample_count = self.sample_count
        print(f"STREAM STOPPED: {payload}")
        if sample_count <= 0:
            self._fail("No data samples received during 5 second stream window")
            return
        print("Disconnecting sensors...")
        self.manager.disconnect_all()

    def on_disconnected(self, payload):
        addresses = []
        if isinstance(payload, list):
            addresses = [str(item) for item in payload if item]
        elif isinstance(payload, dict):
            address = payload.get("address")
            if address:
                addresses = [str(address)]

        with self.lock:
            for address in addresses:
                self.disconnected.add(address)
            disconnected_count = len(self.disconnected)
        print(f"DISCONNECTED: {disconnected_count}")
        if disconnected_count >= self.sensor_count:
            self.done.set()

    def on_error(self, payload):
        self._fail(f"Sensor manager error: {payload}")


def build_parser():
    parser = argparse.ArgumentParser(description="BLE gateway backend smoke test")
    parser.add_argument(
        "--ble-backend",
        choices=["bleak", "nexus_ble_gateway"],
        default="nexus_ble_gateway",
    )
    parser.add_argument("--sensor-count", type=int, default=1)
    parser.add_argument("--stream-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    return parser


def main():
    args = build_parser().parse_args()
    test = GatewayBackendSmokeTest(
        sensor_count=args.sensor_count,
        stream_seconds=args.stream_seconds,
        timeout_seconds=args.timeout_seconds,
        ble_backend=args.ble_backend,
    )
    raise SystemExit(test.run())


if __name__ == "__main__":
    main()
