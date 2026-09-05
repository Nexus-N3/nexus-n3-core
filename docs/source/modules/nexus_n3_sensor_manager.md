# nexus_n3.sensor_manager

## Overview
`nexus_n3.sensor_manager` manages discovery, connection, streaming, and
standalone battery precheck for plugin-backed sensor instances. It keeps an
asyncio loop in a background thread and exposes a stable facade API through
`SensorManager`.

On Linux, startup clears stale BLE device state before discovery to reduce
reconnect failures from prior sessions.

Internally, responsibilities are split into services:
- `SensorController`: command routing/dispatch map
- `AdapterPool`: adapter creation and sensor grouping
- `DiscoveryService`: discovery/matching/address assignment
- `ConnectionService`: connect/disconnect/setup
- `StreamingService`: stream start/stop orchestration
- `PollingStreamService`: optional `request_sample()` fallback path
- `BatteryPrecheckService`: standalone pre-init battery workflow

## Key Classes and APIs
- `SensorManager(system_event_bus=None, error_cb=None)`
  - `register_listener(event, callback)` / `get_listener(event)` / `unregister_listener(event)`
  - `init_sensor_manager(sensors_to_init)`
    - Accepts sensor instances or `{sensor, meta}` entries; supports location metadata
  - `discover()` / `discover_for_subject(sensors)`
  - `discover_and_connect()`
  - `connect_all()` / `connect_specific_sensors(addresses)`
  - `disconnect_all()` / `disconnect_addresses(addresses)`
  - `start_all()` / `start_specific_sensors(addresses)`
  - `stop_all()` / `stop_specific_sensors(addresses)`
  - `identify(address)`
  - `get_connected_sensors()` / `get_connected_sensor_by_address(address)`
  - `check_battery_preinit(sensor_classes, scan_timeout=5.0, read_timeout=10.0)`
  - `stop_manager()`
- Adapters
  - `BLEAdapter` (Bleak host BLE backend)
  - `GatewayBLEAdapter` (Nexus BLE gateway backend over USB serial)
  - `WifiAdapter` (shared Wi-Fi sensor adapter)
  - `USBCameraAdapter` (V4L2 discovery)

## Wi-Fi Sensor Adapter

Sensors declare `adapter: WIFI` and provide vendor-specific Wi-Fi behavior
through their installed sensor plugin. Core owns the shared host network and
the plugin owns the sensor protocol. This keeps NetworkManager D-Bus objects,
host interface management, and recovery permissions out of plugins, while
keeping vendor discovery, provisioning commands, connections, and sample
parsing out of Core.

The Linux implementation uses NetworkManager's system D-Bus API through
`dbus-fast`; it does not parse `nmcli` output. One `WifiAdapter` is pooled for
all Wi-Fi sensors. Multiple requested sensors are reconciled by stable identity,
and missing sensors are provisioned sequentially through an exclusive session.
The multi-sensor path is covered with fake drivers even when only one physical
sensor of a model is available for live testing.

### Discovery and provisioning lifecycle

1. Initialize the configured interface and validate the saved Nexus sensor AP.
2. Ask each plugin driver for sensors already announcing on the Nexus subnet.
3. If a requested identity is missing, stop AP hosting temporarily and perform
   a fresh scan.
4. Let plugin drivers classify candidate provisioning APs.
5. Connect to one candidate with a volatile NetworkManager profile and have the
   owning plugin identify it before configuration.
6. Pass the configured Nexus SSID, password, and channel to the plugin's
   provision method.
7. Remove volatile state, restore the Nexus AP, and wait for the stable sensor
   identity to announce on the Nexus subnet.
8. Cache the discovered device and its plugin driver for normal connection,
   streaming, and disconnection.

Cleanup is shielded from cancellation and restores the host AP on every exit
path. Provisioning does not expose AP credentials in errors or diagnostics.

### X-IMU3 plugin lifecycle

The `nexus-n3-sensor-x-imu3` plugin uses the vendor `ximu3` package for network
announcements, configuration commands, UDP connections, and data callbacks.
Its lifecycle is:

- discover connected sensors from vendor network announcements
- identify a sensor while connected to its open provisioning AP
- configure it as a client of the Nexus sensor AP and apply the settings
- rediscover and verify the stable serial identity with a UDP ping
- configure matched inertial and quaternion message rates during setup
- enable UDP data messages at stream start and disable them at stream stop
- remove native callbacks and close the vendor connection at disconnect

The sampling rate is selected when a new session is set up; it is not changed
mid-stream. Inertial and quaternion messages are joined by the vendor's
microsecond timestamp. The plugin converts vendor acceleration from g to the SDK
contract's m/s² before emitting `IMUSample`; gyroscope values remain degrees/s.
Environmental gravity passed to an algorithm remains an algorithm input and is
not used as the unit-conversion constant.

### Wi-Fi runtime configuration

The main settings in `runtime.env` are:

```text
NEXUS_SENSOR_NETWORK_ENABLED=1
NEXUS_WIFI_BACKEND=linux-networkmanager
NEXUS_SENSOR_INTERFACE=wlx00c0cabaa751
NEXUS_SENSOR_CONNECTION=nexus-n3-sensor-ap
NEXUS_SENSOR_AP_SSID=nexus-n3-sensors
NEXUS_SENSOR_AP_PASSWORD=<secret>
NEXUS_SENSOR_AP_CHANNEL=36
NEXUS_SENSOR_AP_EXPECTED_CIDR=10.42.0.1/24
NEXUS_WIFI_PROVISIONING_CONNECTION=nexus-n3-sensor-provision
NEXUS_WIFI_DISCOVERY_TIMEOUT_S=20
NEXUS_WIFI_CONNECT_TIMEOUT_S=30
NEXUS_WIFI_PROVISIONING_JOIN_TIMEOUT_S=90
NEXUS_WIFI_ALLOW_NETWORK_STACK_RESTART=1
NEXUS_NETWORK_STACK_RESTART_TIMEOUT_SECONDS=30
NEXUS_WIFI_REGULATORY_DOMAIN=EE
```

`NEXUS_SENSOR_AP_PASSWORD` must be supplied through the protected runtime env;
it must not be committed to the repository.

## BLE Backends
BLE sensors still declare `adapter: BLE`, but Nexus N3 Core now supports two
runtime-selectable BLE backends behind that single adapter family.

### `bleak`
- Uses the existing local host BLE path through `BLEAdapter`
- Best for simplified deployments, development, and smaller test setups
- Remains a supported runtime backend

### `nexus_ble_gateway`
- Uses `GatewayBLEAdapter` with a shared `GatewaySerialClient`
- Moves BLE transport work to the Nexus BLE gateway over USB serial
- Preserves the existing plugin contract while adding packet-level gateway
  diagnostics and recovery support
- Primary production backend for the main gateway-based system
- Works through the host serial port layer rather than the host BLE stack
- Can be used on Windows when `GATEWAY_SERIAL_PORT` is set to a Windows serial
  port such as `COM3`
- This is the preferred BLE path on Windows development machines

### Runtime Selection
Backend selection is made once at server startup and does not require plugin or
sensor-spec changes.

- CLI:
  - `python nexus_n3_server.py --ble-backend bleak`
  - `python nexus_n3_server.py --ble-backend nexus_ble_gateway`
  - `nexus-n3-core --ble-backend bleak`
  - `nexus-n3-core --ble-backend nexus_ble_gateway`
- Shared runtime env file:
  - `config/runtime.env` locally, typically created from `config/runtime-example.env`
  - `/etc/nexus-n3/runtime.env` in deployed systems
- Main gateway transport settings:
  - `GATEWAY_SERIAL_PORT`
  - `GATEWAY_BAUDRATE`
  - `GATEWAY_PROTOCOL_VERSION`
  - `GATEWAY_CONNECT_TIMEOUT_S`
  - `GATEWAY_SUBSCRIBE_TIMEOUT_S`
  - `GATEWAY_WRITE_TIMEOUT_S`
  - `GATEWAY_READ_TIMEOUT_S`

Examples:

- Linux: `GATEWAY_SERIAL_PORT=/dev/serial/by-id/...`
- Windows: `GATEWAY_SERIAL_PORT=COM3`

The selected BLE backend is exposed in runtime status and the admin UI. When
the gateway backend is selected, the admin UI also reports whether the gateway
serial device is currently available.

`bleak` remains available mainly for development/testing. Its host-platform
behavior is owned by Bleak itself.

### Gateway timing and diagnostics

For binary gateway stream frames, core preserves the gateway timestamp and adds
a host monotonic receive timestamp before forwarding the sample through the
sensor callback. Plugins that opt into timing metadata can propagate it with the
sample; plugins that use the existing two-argument callback continue to work.

Gateway diagnostic snapshots contain:

- `parser`: host serial parser checksum failures, resynchronisation events and
  dropped bytes, plus partial JSON/frame waits
- `transport`: gateway-reported control and stream transport counters
- `ble_rx`: gateway-reported per-sensor notification receive counters
- `notification_drop_count`: the latest gateway notification-drop total
- `sensors`: the adapter's current sensor connection state

The host-owned `parser` counters are reset immediately before the first stream
start of each new recording session. A user may therefore start another session
without reinitializing core and still receive session-specific checksum and
resynchronisation totals. Automatic startup retries within that recording do not
reset the counters. This operation clears counters only: it does not issue a
gateway `reset_session`, disconnect sensors, or alter gateway firmware state.

Gateway-reported `transport`, `ble_rx`, and notification-drop fields have their
own gateway-side lifetime/reset semantics and should not be interpreted as the
same host parser-counter scope.

### Wi-Fi diagnostics

At stream stop, `AdapterPool.collect_diagnostics()` returns a `WIFI` payload
alongside any `BLE` payload. It contains:

- adapter lifecycle counters, bounded recent errors, capabilities, active
  subnet, and current discovered/connected identities
- NetworkManager AP activation, scan, temporary-profile, restoration, and
  fixed-service recovery state and counters
- optional per-sensor diagnostics retrieved from the isolated plugin host

The X-IMU3 plugin reports inertial and quaternion callback counts, complete
timestamp-paired samples, pending and evicted message halves, callback failures,
timestamp gaps, estimated missing and out-of-order samples, configured and
observed sampling rates, and current connection/stream state.

Core resets adapter and plugin counters at the beginning of each recording
session without changing connections. Plugins that do not implement the
optional diagnostics hooks remain compatible.

## Message Flow
- `Core._init_sensor_manager()` resolves installed sensor plugin classes and passes sensor instances and metadata
- Commands are queued to the manager loop and dispatched by `SensorController`
- Discovery -> adapter scan -> name matching -> address assignment -> callbacks to `Core`
- Connect -> adapter connect -> sensor setup hook
- Streaming -> sensor stream hooks (`start_stream`/`stop_stream`) -> `on_data`
- New recording session -> reset adapter/plugin diagnostics -> stream start hooks
- Polling fallback (`request_sample`) remains available but optional
- Battery check runs as a standalone pre-init BLE flow and returns
  `{"sensors": [...], "errors": {...}}`

The manager remains transport-generic. Sensor protocol logic, parsing, and any
optional `consume_input` behavior live inside the installed sensor plugins.

## Key Files
- `nexus_n3.sensor_manager/SensorManager.py`
- `nexus_n3.sensor_manager/sensor_controller.py`
- `nexus_n3.sensor_manager/adapter_pool.py`
- `nexus_n3.sensor_manager/discovery_service.py`
- `nexus_n3.sensor_manager/connection_service.py`
- `nexus_n3.sensor_manager/streaming_service.py`
- `nexus_n3.sensor_manager/polling_stream_service.py`
- `nexus_n3.sensor_manager/battery_precheck_service.py`
- `nexus_n3.sensor_manager/adapters/ble_adapter.py`
- `nexus_n3.sensor_manager/adapters/gateway_ble_adapter.py`
- `nexus_n3.sensor_manager/adapters/gateway_ble_client.py`
- `nexus_n3.sensor_manager/adapters/wifi_adapter.py`
- `nexus_n3.sensor_manager/adapters/wifi/backends/networkmanager_dbus.py`
- `nexus_n3.sensor_manager/adapters/wifi/backends/linux_networkmanager.py`
- `nexus_n3.sensor_manager/ble_runtime_config.py`
- `nexus_n3.sensor_manager/adapters/usb_camera_adapter.py`
