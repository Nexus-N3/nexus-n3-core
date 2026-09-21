

# X-IMU3 wifi adapter tests

The goal of these tests is to establish a lifcycle to then move into a wifi adapter and
create a plugin for X-IMU3

The adapter should work across at least linux and windows.
There are known issues with macos that require manual intervention.

## Tests

These diagnostics expect the production topology to be provisioned already:
`br-sensor` owns the sensor-network IPv4 address, while the external Wi-Fi AP
and tagged VLAN 20 interfaces are bridge ports. Core validates this topology;
it does not create it or provide DHCP.

scan for -imu3 device and restore the ap
```bash
PYTHONPATH=".:../nexus-n3-plugin-catalog/sensors/nexus-n3-sensor-x-imu3/src:../nexus-n3-plugin-tooling/packages/sdk/src" \
NEXUS_N3_ENV_FILE=config/runtime.env \
python -u nexus_n3/sensor_manager/tests/wifi_adapter/discover.py
```

This diagnostic uses the production NetworkManager backend and X-IMU3 plugin
classifier. It does not configure the sensor and restores the Nexus AP before
exiting.

After `reset.py` has placed the sensor in AP mode and `discover.py` has found
it, exercise the production provisioning, discovery, UDP connection, and
disconnect path:

```bash
PYTHONPATH=".:../nexus-n3-plugin-catalog/sensors/nexus-n3-sensor-x-imu3/src:../nexus-n3-plugin-tooling/packages/sdk/src" \
NEXUS_N3_ENV_FILE=config/runtime.env \
python -u nexus_n3/sensor_manager/tests/wifi_adapter/lifecycle.py
```

Once the sensor is connected to the Nexus AP, exercise the same production
discovery/connection path plus the plugin's vendor-backed 50 Hz inertial and
quaternion callbacks. The diagnostic joins the two message types by the
X-IMU3 microsecond timestamp and reports complete Nexus IMU samples:

```bash
PYTHONPATH=".:../nexus-n3-plugin-catalog/sensors/nexus-n3-sensor-x-imu3/src:../nexus-n3-plugin-tooling/packages/sdk/src" \
NEXUS_N3_ENV_FILE=config/runtime.env \
NEXUS_WIFI_ALLOW_NETWORK_STACK_RESTART=1 \
python -u nexus_n3/sensor_manager/tests/wifi_adapter/stream.py --seconds 10
```

The recovery opt-in is harmless when normal AP restoration succeeds. It is
needed on the mt76x2u adapter only when the production backend must invoke the
installed fixed-purpose recovery service. Recovery restores the AP as a
`br-sensor` member and returns the bridge IPv4 configuration.

The mt76x2u adapter may require the previously proven network-stack recovery
after the x-IMU3 provisioning AP disappears. Install the fixed-purpose root
service once, granting this development user permission to start only that
unit:

```bash
sudo deployment/systemd/install_wifi_recovery.sh "$USER" EE
```

Then opt in to recovery for the diagnostic run; no cached sudo credential is
required:

```bash
PYTHONPATH=".:../nexus-n3-plugin-catalog/sensors/nexus-n3-sensor-x-imu3/src:../nexus-n3-plugin-tooling/packages/sdk/src" \
NEXUS_N3_ENV_FILE=config/runtime.env \
NEXUS_WIFI_ALLOW_NETWORK_STACK_RESTART=1 \
python -u nexus_n3/sensor_manager/tests/wifi_adapter/lifecycle.py
```
## resources

- [Vendor Python examples](https://github.com/xioTechnologies/x-IMU3-Software/tree/main/Examples/Python)
- [Vendor connection/callback example](https://github.com/xioTechnologies/x-IMU3-Software/blob/main/Examples/Python/connection.py)
- [Vendor multiple UDP connections and commands example](https://github.com/xioTechnologies/x-IMU3-Software/blob/main/Examples/Python/multiple_connections.py)
