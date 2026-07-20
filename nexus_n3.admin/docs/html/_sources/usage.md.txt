# Usage

## Product Shape

NexusN3 Edge Core is the runtime layer for:

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

Create a virtual environment and install the core runtime:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

For packaged installs, build a wheel with:

```bash
python -m build
```

## Runtime Environment

Shared runtime configuration lives in:

- `config/runtime.env` for local development
- `/etc/nexus-n3/runtime.env` for deployed systems

This file is the source of truth for:

- plugin root and optional dev bootstrap list
- BLE backend and gateway serial settings
- local ZeroMQ gateway bindings
- Azure bridge settings
- admin/runtime options

The server now supports a runtime-env-first startup path, so local development
can use:

```bash
python nexus_n3_server.py
```

if the required values are already set in `config/runtime.env`.

## Run The Full System

Minimal local run:

```bash
python nexus_n3_server.py
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

Standalone runtime with Azure bridge:

```bash
python nexus_n3_server.py --role standalone --bridge azure_bridge --azure-bridge-remote-control --admin --admin-host 0.0.0.0 --admin-port 9000
```

Worker node:

```bash
python nexus_n3_server.py --role worker --node-id worker_A
```

AI node:

```bash
python nexus_n3_server.py --role ai --node-id ai_A
```

Master node:

```bash
python nexus_n3_server.py --role master --mdns-hostname nexus-n3-master --admin --admin-host 0.0.0.0 --admin-port 9000
```

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
     /home/mike/Desktop/apps/dev/nexus-n3-project/nexus-n3-plugin-catalog/plugin-builds/sensors/nexus-n3-sensor-movesense-0.1.2.rsnxplugin \
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

Installation and runtime discovery are owned by NexusN3 Edge Core through
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

## File Output

The core writes data generically through the file manager rather than through
sensor-specific storage code.

Outputs are organized under:

```text
nexus_n3_outputs/<site>/<session_label>/session_<timestamp>/
```

That session tree contains:

- raw CSV sample files
- real-time NDJSON result streams
- intermediate NDJSON files
- consolidated NDJSON files
- diagnostics events when enabled

When a session is fully finalized, the session directory is zipped locally and
the directory tree is removed.

On Linux standalone and master nodes, the optional hot-disk workflow can switch
that active output path onto the managed removable disk mount. On non-Linux
hosts, file output remains local-only.

## Diagnostics

Enable low-overhead pipeline diagnostics only when needed:

```bash
python nexus_n3_server.py --diagnostics
```

This writes:

```text
nexus_n3_outputs/<site>/<session_label>/session_<timestamp>/diagnostics/pipeline_debug.ndjson
```

Set `NEXUS_PERF_LOG=1` to enable periodic performance logging.

## Deployment Paths

See:

- `deployment/guides/ANSIBLE_DEPLOYMENT.md`
- `deployment/guides/DOCKER_DEPLOYMENT.md`
- `deployment/guides/DISTRIBUTED_DEPLOYMENT.md`
- `deployment/guides/MANUAL_DEPLOYMENT.md`
- `deployment/guides/SYSTEMD_DEPLOYMENT.md`
