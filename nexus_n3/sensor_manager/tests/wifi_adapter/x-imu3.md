

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
```
discover the x-imu3 and connect to it, disconnect and restore the ap
```bash
test_ap_connect.py
```
connect and send a hamless command over udp - this uses the ximu3 api (requies pip install)
```bash
test_sensor_udp.py
```



## resources

- python examples
https://github.com/xioTechnologies/x-IMU3-Software/tree/main/Examples/Python

