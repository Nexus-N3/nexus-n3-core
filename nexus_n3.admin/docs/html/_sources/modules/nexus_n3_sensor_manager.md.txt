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
  - `USBCameraAdapter` (V4L2 discovery)

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

## Message Flow
- `Core._init_sensor_manager()` resolves installed sensor plugin classes and passes sensor instances and metadata
- Commands are queued to the manager loop and dispatched by `SensorController`
- Discovery -> adapter scan -> name matching -> address assignment -> callbacks to `Core`
- Connect -> adapter connect -> sensor setup hook
- Streaming -> sensor stream hooks (`start_stream`/`stop_stream`) -> `on_data`
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
- `nexus_n3.sensor_manager/ble_runtime_config.py`
- `nexus_n3.sensor_manager/adapters/usb_camera_adapter.py`
