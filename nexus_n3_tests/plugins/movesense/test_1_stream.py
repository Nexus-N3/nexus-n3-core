import importlib
import time
import sys
from pathlib import Path


# Allow importing the plugin package without installation.
REPO_ROOT = Path(__file__).resolve().parents[4]
OS_ROOT = Path(__file__).resolve().parents[3]
PLUGIN_PATH = REPO_ROOT / "nexus-n3-sensors-plugins" / "nexus-n3-sensor-movesense"
if str(OS_ROOT) not in sys.path:
    sys.path.insert(0, str(OS_ROOT))
if str(PLUGIN_PATH) not in sys.path:
    sys.path.insert(0, str(PLUGIN_PATH))

# Ensure we load the local plugin, not the installed package.
for module_name in ("movesense_sensor", "movesense_sensor.sensor", "movesense_sensor.parser"):
    if module_name in sys.modules:
        del sys.modules[module_name]
importlib.invalidate_caches()

import movesense_sensor.parser as p
print("parser file:", p.__file__, flush=True)

from nexus_n3.sensor_manager.SensorManager import SensorManager
from movesense_sensor.sensor import MovesenseSensor


def main():
    sensor = MovesenseSensor(None)
    sensor.attributes.update({
        "STREAMS": [ "HR"],  # add ECG HR here to stream both
    })

    manager = SensorManager()
    manager.init_sensor_manager([
        {"sensor": sensor, "meta": {"location": "CHEST"}}
    ])

    def on_discover(payload):
        print(f"Discover: {payload}")

    def on_connected(payload):
        print(f"Connected: {payload}")

    hr_count = 0
    ecg_count = 0

    def on_data(payload):
        nonlocal hr_count, ecg_count
        sample_type = getattr(payload, "sample_type", None)
        if sample_type == "hr":
            hr_count += 1
            if hr_count <= 10 or hr_count % 10 == 0:
                print(f"[TEST] HR samples={hr_count}")
            return
        # Keep a minimal signal that data is flowing without spamming logs.
        if sample_type == "ecg":
            ecg_count += 1
            if ecg_count == 1:
                print("[TEST] ECG data flowing...")

    def on_error(payload):
        print(f"Error: {payload}")

    manager.register_listener("on_discover", on_discover)
    manager.register_listener("on_connected", on_connected)
    manager.register_listener("on_data", on_data)
    manager.register_listener("on_error", on_error)

    manager.discover()
    time.sleep(8)

    if not sensor.address:
        print("No Movesense device discovered; aborting test.")
        manager.stop_manager()
        return

    manager.connect_all()
    time.sleep(6)

    manager.start_all()
    time.sleep(10)

    manager.stop_all()
    time.sleep(2)

    manager.disconnect_all()
    time.sleep(3)

    print(f"[TEST] HR samples total={hr_count}")
    print(f"[TEST] ECG samples total={ecg_count}")
    manager.stop_manager()


if __name__ == "__main__":
    main()
