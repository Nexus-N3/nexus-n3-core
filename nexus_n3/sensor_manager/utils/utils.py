"""Utility helpers for BLE device discovery and validation."""

import math
import numpy as np
from nexus_n3.sensor_manager.types.devices import DevicesValid

import os
import logging
from typing import Dict, List, Tuple

# ---------------- LOGGING ----------------
# Suppress overly verbose loggers when packaged
print(f"setting all loggers to WARNING level")
for name in logging.root.manager.loggerDict:
    logging.getLogger(name).setLevel(logging.WARNING)


# ---------------- OS CHECK ----------------
# Only attempt DBus import on POSIX systems (Linux)
DBUS_AVAILABLE = False
if os.name == "posix":
    try:
        import dbus
        DBUS_AVAILABLE = True
        print("DBus module successfully imported.")
    except ImportError:
        print("DBus module is not installed; DBus-dependent helpers disabled.")
else:
    print("Not running on Linux, skipping DBus import.")


# ---------------- DEVICE REMOVAL ----------------
def remove_all_devices():
    """
    Remove all paired or cached BLE devices via DBus on Linux.
    
    Uses the system bus and BlueZ ObjectManager to enumerate
    all devices and remove them.
    
    Prints success/failure for each device.
    """
    if not DBUS_AVAILABLE:
        print("DBus is not available; skipping remove_all_devices.")
        return
    bus = dbus.SystemBus()
    obj_manager = bus.get_object("org.bluez", "/")
    manager = dbus.Interface(obj_manager, "org.freedesktop.DBus.ObjectManager")

    devices_removed = 0

    for path, interfaces in manager.GetManagedObjects().items():
        if "org.bluez.Device1" in interfaces:
            device = bus.get_object("org.bluez", path)
            adapter_path = "/".join(path.split("/")[:4])  # e.g., /org/bluez/hci0
            adapter = bus.get_object("org.bluez", adapter_path)
            adapter_interface = dbus.Interface(adapter, "org.bluez.Adapter1")

            try:
                adapter_interface.RemoveDevice(path)
                print(f"Removed {path}, cache cleared.")
                devices_removed += 1
            except dbus.exceptions.DBusException as e:
                print(f"Failed to remove {path}: {e}")

    if devices_removed == 0:
        print("No devices found to remove.")


def remove_devices(devices):
    """
    Remove a specified list of devices via DBus on Linux.
    
    Args:
        devices (list): List of device objects with an 'address' attribute
    
    Notes:
        Assumes adapter path is /org/bluez/hci0.
        Prints success/failure for each device.
    """
    if not DBUS_AVAILABLE:
        print("DBus is not available; skipping remove_devices.")
        return
    bus = dbus.SystemBus()
    adapter_path = "/org/bluez/hci0"  # Assuming hci0 is the adapter
    obj = bus.get_object("org.bluez", adapter_path)
    adapter = dbus.Interface(obj, "org.bluez.Adapter1") 

    for d in devices:
        try:
            adapter.RemoveDevice(f"{adapter_path}/dev_{d.address.replace(':', '_')}")
            print(f"Removed device {d.address}, cache cleared.")
        except dbus.exceptions.DBusException as e:
            print(f"Error: {e}")


# ---------------- DEVICE MATCHING ----------------
def match_devices(names, devices):
    """
    Match discovered devices by their local names.

    Supports exact matching and prefix matching (e.g., "Movesense" prefix).

    Args:
        names (list[str]): Names of devices to match
        devices (dict): Dictionary of devices returned from discover_devices
                        {addr: (device, advertisement_data)}

    Returns:
        list of tuples: [(device, advertisement_data, matched_name), ...]
    """
    matching_devices = []
    used_addresses = set()

    for name in names:
        matched = None
        for addr, (device, adv_data) in devices.items():
            if addr in used_addresses or not adv_data.local_name:
                continue
            if adv_data.local_name == name or adv_data.local_name.startswith(name):
                matched = (device, adv_data, name)
                used_addresses.add(addr)
                break
        if matched:
            matching_devices.append(matched)

    return matching_devices


def validate_matched_devices(sensors: list, matched_devices: list):
    """
    Validate that discovered BLE devices satisfy required sensors.
    
    Args:
        sensors (list): List of pre-instantiated sensor objects (sensor.name used)
        matched_devices (list): List of tuples from BLEAdapter.discover_devices
                                [(BLEDevice, AdvertisementData), ...]
    
    Returns:
        DevicesValid: Named tuple with fields:
            - valid (bool): True if all required sensors are found
            - missing (list): List of missing sensor types with counts
            - found (int): Total number of discovered devices that match
    """
    # Count required instances per sensor type
    required_counts = {}
    for s in sensors:
        required_counts[s.name] = required_counts.get(s.name, 0) + 1

    # Count discovered instances per sensor type
    found_counts = {}
    print(f"validating matched device") 
    for entry in matched_devices:
        if len(entry) == 3:
            _, adv_data, matched_name = entry
            name = matched_name
        else:
            _, adv_data = entry
            name = adv_data.local_name
        found_counts[name] = found_counts.get(name, 0) + 1

    missing = []
    for name, required in required_counts.items():
        found = found_counts.get(name, 0)
        if found < required:
            missing.append(f"{name} missing {required - found}")

    total_found = sum(min(required_counts.get(name, 0), found_counts.get(name, 0)) for name in required_counts)

    return DevicesValid(valid=(len(missing) == 0), missing=missing, found=total_found)


# ---------------- SENSOR CLASS HELPERS ----------------
def build_battery_name_map(sensor_classes) -> Dict[str, type]:
    """
    Build a name->class map for battery-capable sensors.

    Includes sensor_type.local_name, spec sensor.name, and spec aliases.
    """
    name_map: Dict[str, type] = {}
    for cls in sensor_classes:
        try:
            spec = cls.load_raw_spec()
        except Exception:
            spec = {}
        names = set()
        sensor_type = getattr(cls, "sensor_type", None)
        if sensor_type and getattr(sensor_type, "local_name", None):
            names.add(sensor_type.local_name)
        spec_name = spec.get("sensor", {}).get("name")
        if spec_name:
            names.add(spec_name)
        for alias in spec.get("aliases", []) or []:
            if alias:
                names.add(alias)
        for name in names:
            name_map[name] = cls
    return name_map


def match_sensor_name(local_name: str, names_sorted: List[str], names_sorted_lower: List[Tuple[str, str]]):
    """Match BLE local name to a known sensor name (exact or prefix)."""
    if not local_name:
        return None
    for name in names_sorted:
        if local_name == name or local_name.startswith(name):
            return name
    local_lower = local_name.lower()
    for name, lower_name in names_sorted_lower:
        if local_lower == lower_name or local_lower.startswith(lower_name):
            return name
    return None


def instantiate_sensor_class(cls):
    """Instantiate a sensor class with a best-effort constructor signature."""
    try:
        return cls(getattr(cls, "sensor_type", None))
    except TypeError:
        try:
            return cls()
        except Exception:
            return None
