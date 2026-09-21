# Usage

## Product Shape

Nexus N3 Core is the runtime layer for:

- gateway and command/event handling
- sensor discovery, connection, and streaming
- plugin installation, discovery, and isolated execution
- subject/session orchestration
- generic raw and computed file output
- optional Azure bridge integration

Sensor and algorithm implementations are no longer expected to live in the core
repository. They are delivered as installed plugins under the configured plugin
root.

## Install

Create a virtual environment and install the core runtime from a source
checkout:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

For a packaged install from PyPI or TestPyPI, use:

```bash
python -m pip install nexus-n3-core
```

## Runtime Environment

Shared runtime configuration lives in:

- `config/runtime-example.env` as the tracked template for local development
- `config/runtime.env` as the local untracked copy
- `/etc/nexus-n3/runtime.env` for deployed systems

For an installed package, place your runtime configuration at:

```bash
/etc/nexus-n3/runtime.env
```

Or point the runtime at a different file with:

```bash
export NEXUS_N3_ENV_FILE=/path/to/runtime.env
```

To copy the current local development config into the standard deployed path:

```bash
sudo mkdir -p /etc/nexus-n3
sudo cp /home/mike/Desktop/apps/dev/rs-nexus-project/nexus-n3-core/config/runtime.env /etc/nexus-n3/runtime.env
```

This file is the source of truth for:

- plugin root and optional dev bootstrap list
- BLE backend and gateway serial settings
- Wi-Fi sensor interface, AP, provisioning, and recovery settings
- local ZeroMQ gateway bindings
- Azure bridge settings
- admin/runtime options

The server now supports a runtime-env-first startup path, so local development
can use:

```bash
python nexus_n3_server.py
```

if the required values are already set in your local `config/runtime.env`.

After installing from PyPI or TestPyPI, use:

```bash
nexus-n3-core
```

## Run The Full System

Minimal local run:

```bash
python nexus_n3_server.py
```

If no site is configured in `runtime.env` and you do not pass `--site`, the
runtime uses `local` as the default site label for output paths.

Installed-package run:

```bash
nexus-n3-core
```

On non-Linux development hosts, including Windows laptops, the runtime uses the
local `nexus_n3_outputs/` path only. The removable USB hot-disk workflow is
disabled automatically there.

Other host-setup features such as access-point mode and kiosk setup are Linux
deployment concerns and are not part of the Windows development/runtime path.

That means a Windows development run should already:

- use local file output
- skip Linux hot-disk behavior
- avoid Linux host-provisioning features such as AP mode and kiosk setup

Standalone runtime with admin UI:

```bash
python nexus_n3_server.py --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
```

Installed-package equivalent:

```bash
nexus-n3-core --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
```

Standalone runtime with Azure bridge:

```bash
python nexus_n3_server.py --role standalone --bridge azure_bridge --azure-bridge-remote-control --admin --admin-host 0.0.0.0 --admin-port 9000
```

Worker node:

```bash
python nexus_n3_server.py \
  --role worker \
  --node-id worker_A \
  --customer-id <customer-id> \
  --site <site> \
  --site-id <site-id> \
  --site-name "<site name>"
```

Master and workers in one distributed deployment must use matching customer and
site identity. Ansible supplies these flags from the worker host variables.

AI node:

```bash
python nexus_n3_server.py --role ai --node-id ai_A
```

Master node:

```bash
python nexus_n3_server.py --role master --mdns-hostname nexus-n3-master --admin --admin-host 0.0.0.0 --admin-port 9000
```

In distributed mode the master participates in subject execution. Subjects are
assigned to capable master/worker nodes by current sensor load, with the master
preferred when loads are equal. A shared stop is archived by the master only
after every participating node emits `stream_drained` for the same stop and
session.

The runtime uses the internal `zeromq_gateway`. `--gateway` remains accepted,
but `zeromq_gateway` is the only supported value.

## Plugins

### Current Model

Plugins are installed into `NEXUS_N3_PLUGIN_ROOT` and discovered from the
runtime catalog there.

Production/operator-facing artifacts are:

- `.rsnxplugin` bundles only

The core runtime then:

- validates installed plugin catalog entries
- discovers supported sensors and algorithms
- starts isolated algorithm and sensor hosts from installed plugin versions

Clients and calling applications should treat the supported sensor/algorithm
inventory as live runtime state. Before creating or updating a session
configuration, query the runtime for the current supported plugin inventory and
build the session from that result.

Subject/session initialization is a validation and binding step. It checks that
the requested sensors and algorithms are already installed and compatible, but
it does not act as the discovery source for selectable plugin options.

### Developer Workflow

High-level flow:

1. scaffold or edit a plugin in `nexus-n3-plugin-catalog/`
2. build a `.rsnxplugin` bundle with `nexus-n3-plugin-tooling`
3. install that bundle from `nexus-n3-core`, for example:

   ```bash
   python -m nexus_n3.plugins install \
     /path/to/nexus-n3-plugin-catalog/plugin-builds/sensors/nexus-n3-sensor-movesense-0.1.2.rsnxplugin \
     --plugin-root /opt/nexus-n3-plugins
   ```
4. run `python nexus_n3_server.py`

For local developer convenience, the core runtime also supports:

- `NEXUS_N3_BOOTSTRAP_PLUGINS`
- `NEXUS_N3_BOOTSTRAP_PLUGIN_LIST`
- `NEXUS_N3_PLUGIN_CATALOG_ROOT`

That allows the selected dev plugins to be built and installed before startup,
or installed independently with:

```bash
python -m nexus_n3.plugins install-dev-list
```

### Plugin Tooling

`nexus-n3-plugin-tooling` is the build-side repository. Its job is to:

- scaffold sensor and algorithm plugin repos
- build `.rsnxplugin` bundles
- provide focused development harnesses

Installation and runtime discovery are owned by Nexus N3 Core through
`nexus_n3.plugins`.

## BLE Backends

The sensor manager supports two runtime-selectable BLE backends behind the same
plugin-facing BLE adapter contract:

- `bleak`
- `nexus_ble_gateway`

Select the backend at startup if needed:

```bash
python nexus_n3_server.py --ble-backend bleak
python nexus_n3_server.py --ble-backend nexus_ble_gateway
```

Installed-package equivalents:

```bash
nexus-n3-core --ble-backend bleak
nexus-n3-core --ble-backend nexus_ble_gateway
```

The backend choice does not require sensor plugin or sensor spec changes.

When using `nexus_ble_gateway`, transport settings are loaded from the runtime
env file:

```text
BLE_BACKEND=nexus_ble_gateway
GATEWAY_SERIAL_PORT=/dev/serial/by-id/...   # Linux example
GATEWAY_BAUDRATE=1000000
GATEWAY_PROTOCOL_VERSION=1
GATEWAY_CONNECT_TIMEOUT_S=15.0
GATEWAY_SUBSCRIBE_TIMEOUT_S=5.0
GATEWAY_WRITE_TIMEOUT_S=5.0
GATEWAY_READ_TIMEOUT_S=5.0
```

On Windows, use a serial port name such as:

```text
GATEWAY_SERIAL_PORT=COM3
```

For Windows development, `nexus_ble_gateway` is the preferred BLE path because
it uses the serial gateway rather than host BLE stack integration.

## Wi-Fi Sensors

Wi-Fi sensors use one shared Core adapter and vendor-specific installed sensor
plugins. On Linux, the production backend inspects NetworkManager through
`dbus-fast` on the system bus. Provision the sensor bridge, its VLAN port, its
Wi-Fi AP port, and the runtime settings before enabling the sensor network; see
`modules/nexus_n3_sensor_manager.md` for the complete lifecycle and variable
list.

Normal operation keeps the Wi-Fi AP and tagged VLAN attached to `br-sensor`,
which owns the sensor-network IPv4 address. Discovery first checks for
already-connected sensors without disrupting that AP. Provisioning occurs only
for a requested deficit and temporarily switches the configured radio to a
sensor's provisioning AP inside an exclusive, cleanup-protected session; the
saved AP is then restored as a bridge port.

The X-IMU3 plugin uses stable serial numbers as sensor addresses. Its sample
rate is applied during setup for a new session. The plugin emits canonical
`IMUSample` units: acceleration in m/s², angular velocity in degrees/s, and
timestamps in microseconds.

## File Output

The core writes data generically through the file manager rather than through
sensor-specific storage code.

Outputs are organized under:

```text
nexus_n3_outputs/<site>/sessions/<session_name>_<timestamp>/
```

That session tree contains:

- raw CSV sample files
- real-time NDJSON result streams
- intermediate NDJSON files
- consolidated NDJSON files
- structured session diagnostics

When a session is fully finalized, the session directory is zipped locally and
the directory tree is removed.

On Linux standalone and master nodes, the optional hot-disk workflow can switch
that active output path onto the managed removable disk mount. On non-Linux
hosts, file output remains local-only.

## Diagnostics

Every recording session contains structured diagnostics under a directory
owned by the runtime node:

```text
<node-id>-diagnostics/session_diagnostics.json
<node-id>-diagnostics/session_diagnostics.jsonl
```

The master uses `master-diagnostics`, workers use their configured node ID, and
standalone mode uses `standalone-diagnostics`.

These files are created independently of the optional pipeline-debug switch and
are included in the finalized session archive. The JSON file is the current
session summary. The JSONL file is the time-ordered event record and includes
stream lifecycle events, compute-performance records, BLE and Wi-Fi transport
diagnostics, and errors. The summary field `official_stream` begins as `pending`
and is finalized as `passed` or `failed`.

The latest transport snapshot is stored at:

```text
latest_gateway_diagnostics.diagnostics.BLE
latest_gateway_diagnostics.diagnostics.WIFI
```

The `WIFI` entry combines shared adapter state, NetworkManager backend state,
and optional per-sensor plugin counters. Despite the historical summary field
name, it is a transport-diagnostics container and may contain both adapter
families.

Core emits one `compute_performance` event for every compute result. Important
fields include:

- `algorithm_execution_ms`: exact execution time only when
  `execution_measurement_exact` is `true`
- `result_callback_ms`: fallback duration of the result-producing
  `algorithm.on_sample()` callback; this is not presented as algorithm execution
  time
- `result_interval_ms`: interval between successive results for the same sensor
  and algorithm; absent from the first result
- `expected_result_interval_ms` and `cadence_drift_ms`: expected cadence and
  deviation when the algorithm exposes its window duration
- `queue_wait_ms`: time between compute enqueue and dispatch
- `compute_enqueue_to_result_ms`: enqueue-to-result latency
- `trigger_sample_to_result_ms`: host receipt of the result-producing sample to
  result receipt; it is not the duration of the complete sample window
- `samples_since_previous_result`: samples dispatched since the preceding result
- `trigger_sample`: available source, gateway, host, core, and normalized session
  timestamps for the sample that produced the result

`compute_result` remains the application-facing algorithm output. Timing fields
are deliberately emitted separately through `compute_performance`, so plugin
result schemas do not need to change.

### Optional pipeline debugging

Enable low-overhead pipeline diagnostics only when needed:

```bash
python nexus_n3_server.py --diagnostics
```

This writes:

```text
nexus_n3_outputs/<site>/sessions/<session_name>_<timestamp>/<node-id>-diagnostics/pipeline_debug.ndjson
```

`pipeline_debug.ndjson` is an additional debugging artifact. It is not required
for the structured session diagnostics or compute-performance events described
above.

Set `NEXUS_PERF_LOG=1` to enable periodic performance logging.

## Deployment Paths

See:

- `deployment/guides/ANSIBLE_DEPLOYMENT.md`
- `deployment/guides/DOCKER_DEPLOYMENT.md`
- `deployment/guides/DISTRIBUTED_DEPLOYMENT.md`
- `deployment/guides/MANUAL_DEPLOYMENT.md`
- `deployment/guides/SYSTEMD_DEPLOYMENT.md`
