

# X-IMU3 wifi adapter tests

The goal of these tests is to establish a lifcycle to then move into a wifi adapter and
create a plugin for X-IMU3

The adapter should work across at least linux and windows.
There are known issues with macos that require manual intervention.

## Tests

Use an external wifi adapter as an access point.

scan for -imu3 device and restore the ap
```bash
test_ap_scan.py 
pytest tests/wifi_adapter/test_ap_scan.py -s
```
discover the x-imu3 and connect to it, disconnect and restore the ap
```bash
test_ap_connect.py
```
connect and send a hamless command over udp - this uses the ximu3 api (requies pip install)
```bash
test_sensor_udp.py
```

Provision a x-imu3 device to connect to the ap
```bash
export NEXUS_SENSOR_AP_PASSWORD='your-password'
sudo -v

export NEXUS_TEST_ALLOW_NETWORK_STACK_RESTART=1
export NEXUS_AP_NORMAL_RESTORE_GRACE_SECONDS=5

pytest tests/wifi_adapter/test_ap_provision.py -s
```



## resources

- python examples
https://github.com/xioTechnologies/x-IMU3-Software/tree/main/Examples/Python

