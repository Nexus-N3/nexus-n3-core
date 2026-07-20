# Runtime Config

This directory is the single source of truth for `nexus-n3-core` runtime
environment variables during local development.

The runtime loads configuration in this order:

1. `NEXUS_N3_ENV_FILE` if set  (clarify what this is)
2. `config/runtime.env`
3. `/etc/nexus-n3/runtime.env`

`config/runtime.env` is the local development baseline. Production deployments
should render the same variable set into `/etc/nexus-n3/runtime.env`.

## Host vs Docker Paths

Some path variables differ between host execution and Docker execution.

- When running `python nexus_n3_server.py ...` directly on the host, use host paths.
- When running the Docker compose stack, use container paths.

Example:

- Host development plugin tree might be:
  `/home/mike/Desktop/apps/dev/nexus-n3-project/nexus-n3-plugin-catalog`
- In Docker, that same host directory is mounted as:
  `/workspace/nexus-n3-plugin-catalog`

So `NEXUS_N3_PLUGIN_CATALOG_ROOT=/workspace/nexus-n3-plugin-catalog` is correct for the Docker
container, but not for a direct host run.

## Variables

### Core

`NEXUS_N3_NEIA_APPS_CATALOG_URL`
- Local NEIA catalog endpoint used by admin and device-info surfaces.

`NEXUS_ADMIN_DISPLAY_PROFILE`
- Optional display profile label for the admin UI, for example `1920x1080`.

`NEXUS_PERF_LOG`
- Enables optional performance logging when set to `1`, `true`, `yes`, or `on`.

### Plugin Runtime

`NEXUS_N3_PLUGIN_ROOT`
- Installed plugin root used by the runtime.
- This is where packaged plugins are installed and discovered.
- Example production value: `/opt/nexus-n3-plugins`.

`NEXUS_N3_PLUGIN_USE_SYSTEM_SITE_PACKAGES`
- When enabled, plugin runtime virtual environments may inherit base
  site-packages.
- Useful for development and constrained devices during migration.

`NEXUS_N3_BOOTSTRAP_PLUGINS`
- Startup bootstrap switch for local/developer runs.
- When set to `1`, `true`, `yes`, or `on`, `nexus_n3_server.py` will build and
  install the selected dev plugins before the server starts.
- When set to `0` or left empty, startup does nothing.

`NEXUS_N3_BOOTSTRAP_PLUGIN_LIST`
- Comma-separated list of plugin directory names or `plugin_id` values to
  bootstrap when `NEXUS_N3_BOOTSTRAP_PLUGINS` is enabled.
- Example: `movella-dot,standard-loading-intensity`
- If bootstrap is enabled and this list is empty, startup now fails fast so the
  install set is explicit.

`NEXUS_N3_PLUGIN_CATALOG_ROOT`
- Source tree used by the dev bootstrap flow when `nexus_n3_server.py` prepares
  plugins before startup.
- This should point at the directory that contains `sensors/` and
  `algorithms/`.
- Host example:
  `/home/mike/Desktop/apps/dev/nexus-n3-project/nexus-n3-plugin-catalog`
- Docker example:
  `/workspace/nexus-n3-plugin-catalog`

### BLE Backend

`BLE_BACKEND`
- BLE runtime backend selector.
- Supported values currently normalize to `bleak` or `nexus_ble_gateway`.

`GATEWAY_SERIAL_PORT`
- Serial device path for the Nexus BLE gateway.
- Required when `BLE_BACKEND` is the gateway backend.

`GATEWAY_BAUDRATE`
- Serial baudrate for the Nexus BLE gateway transport.

`GATEWAY_PROTOCOL_VERSION`
- BLE gateway protocol version expected by the runtime.

`GATEWAY_CONNECT_TIMEOUT_S`
- Timeout in seconds for gateway connect/setup operations.

`GATEWAY_SUBSCRIBE_TIMEOUT_S`
- Timeout in seconds for subscription/notification setup.

`GATEWAY_WRITE_TIMEOUT_S`
- Timeout in seconds for outbound gateway writes.

`GATEWAY_READ_TIMEOUT_S`
- Timeout in seconds for inbound gateway reads.

### Local Gateway Transport

`ZEROMQ_CMD_BIND`
- Bind address for the local command socket.

`ZEROMQ_EVENT_BIND`
- Bind address for the local event socket.

### Azure Bridge

`AZURE_IOT_CONNECTION_STRING`
- Azure IoT Hub device connection string.

`AZURE_IOT_DEVICE_ID`
- Azure IoT Hub device identifier.

`AZURE_IOT_CUSTOMER_ID`
- Customer identifier sent with cloud payloads.

`AZURE_IOT_SITE_ID`
- Site identifier sent with cloud payloads.

`AZURE_IOT_SITE_NAME`
- Human-readable site name sent with cloud payloads.

`AZURE_IOT_SITE`
- Legacy/simple site label still used by some bridge paths.

`AZURE_IOT_UPLOAD_CONTAINER`
- Cloud upload container/bucket name for session outputs.

`AZURE_BRIDGE_LOCAL_CMD_ADDR`
- Local command socket address the Azure bridge publishes into.

`AZURE_BRIDGE_LOCAL_EVT_ADDR`
- Local event socket address the Azure bridge subscribes to.

`AZURE_BRIDGE_REMOTE_CONTROL_ENABLED`
- Enables remote control commands from Azure when truthy.

`AZURE_BRIDGE_STATE_FILE`
- Local state file used by the Azure bridge to persist bridge state.

`AZURE_BRIDGE_USE_WEBSOCKETS`
- Enables Azure IoT websocket transport when truthy.

`AZURE_BRIDGE_KEEP_ALIVE`
- Keep-alive interval for Azure IoT transport.

`AZURE_BRIDGE_CONNECTION_RETRY_INTERVAL`
- Retry interval in seconds for Azure bridge reconnection.

`AZURE_BRIDGE_UPLOAD_RETRY_INTERVAL`
- Retry interval in seconds for upload retries.

### Robot Runtime

`NEXUS_N3_ROBOT_CONFIG`
- Path to the robot YAML configuration file.

## Practical Guidance

- To auto-install selected dev plugins before `python nexus_n3_server.py`, set:
  `NEXUS_N3_BOOTSTRAP_PLUGINS=1` and
  `NEXUS_N3_BOOTSTRAP_PLUGIN_LIST=<comma-separated plugin ids>`
- To disable that behavior entirely, set `NEXUS_N3_BOOTSTRAP_PLUGINS=0`.
- For host development, review every path variable and convert container paths
  to host paths.
- For Docker development, use the mounted container paths from
  `deployment/docker/docker-compose.dev.yml`.
- For deployed systems, keep `/etc/nexus-n3/runtime.env` as the single editable
  runtime configuration file.
