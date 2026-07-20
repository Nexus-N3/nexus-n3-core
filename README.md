# NexusN3 Edge Core

NexusN3 Edge Core is the runtime layer for sensor acquisition, plugin-backed
processing, session orchestration, and edge deployment.

The repository name is currently `nexus-n3-core`, but the product/runtime name
described by this codebase is NexusN3 Edge Core.

## Overview

The runtime provides:

- ZeroMQ-based local command/event transport
- sensor discovery, connection, and streaming through `nexus_n3.sensor_manager`
- plugin installation, cataloging, and isolated runtime hosts through `nexus_n3.plugins`
- compute orchestration for algorithm plugins
- generic raw and computed file output
- optional Azure bridge integration
- standalone, master, worker, and AI node roles

Sensors and algorithms are no longer intended to live in the core repository as
built-in runtime implementations. They are delivered as installed plugins.

## Repositories

- core runtime: this repository
- plugin build/scaffold tooling: `nexus-n3-plugin-tooling`
- local development plugin workspace: `nexus-n3-plugin-catalog/`

## Runtime Configuration

Shared runtime configuration lives in:

- `config/runtime.env` for local development
- `/etc/nexus-n3/runtime.env` for deployed systems

This file controls:

- plugin root and optional dev bootstrap behavior
- BLE backend and gateway serial settings
- local ZeroMQ bindings
- Azure bridge settings
- admin/runtime settings

Because startup now reads the runtime env file automatically, a configured local
environment can start with:

```bash
python nexus_n3_server.py
```

## Running The Full System

Minimal local run:

```bash
python nexus_n3_server.py
```

On non-Linux development hosts, including Windows laptops, the runtime stays on
the local `nexus_n3_outputs/` path. The removable USB hot-disk workflow is
disabled automatically there.

Other host-setup features such as access-point mode and kiosk setup are Linux
deployment concerns and are not part of the Windows development/runtime path.

That means a Windows development run should already:

- use local file output
- skip Linux hot-disk behavior
- avoid Linux host-provisioning features such as AP mode and kiosk setup

Standalone with admin UI:

```bash
python nexus_n3_server.py --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
```

Standalone with Azure bridge:

```bash
python nexus_n3_server.py --role standalone --bridge azure_bridge --azure-bridge-remote-control --admin --admin-host 0.0.0.0 --admin-port 9000
```

Master:

```bash
python nexus_n3_server.py --role master --mdns-hostname nexus-n3-master --admin --admin-host 0.0.0.0 --admin-port 9000
```

Worker:

```bash
python nexus_n3_server.py --role worker --node-id worker_A
```

AI node:

```bash
python nexus_n3_server.py --role ai --node-id ai_A
```

`zeromq_gateway` is the supported local gateway. The server still accepts
`--gateway`, but `zeromq_gateway` is the only supported value.

## Plugin Model

### Production Artifact

The only production/operator-facing plugin artifact is:

- `.rsnxplugin`

The runtime installs these bundles into `NEXUS_N3_PLUGIN_ROOT`, records them in
the plugin catalog, and discovers sensor/algorithm support from that installed
state.

### Developer Workflow

High-level plugin workflow:

1. create or edit a plugin in `nexus-n3-plugin-catalog/`
2. build a `.rsnxplugin` bundle with `nexus-n3-plugin-tooling`
3. install it from `nexus-n3-core`, for example:

   ```bash
   python -m nexus_n3.plugins install \
     /home/mike/Desktop/apps/dev/nexus-n3-project/nexus-n3-plugin-catalog/plugin-builds/sensors/nexus-n3-sensor-movesense-0.1.2.rsnxplugin \
     --plugin-root /opt/nexus-n3-plugins
   ```
4. start NexusN3 Edge Core

Examples:

```bash
python -m nexus_n3.plugins show-dev-list
python -m nexus_n3.plugins install-dev-list
```

The runtime can also bootstrap a configured plugin list before startup through:

- `NEXUS_N3_BOOTSTRAP_PLUGINS`
- `NEXUS_N3_BOOTSTRAP_PLUGIN_LIST`
- `NEXUS_N3_PLUGIN_CATALOG_ROOT`

### Plugin Tooling

`nexus-n3-plugin-tooling` is responsible for:

- scaffolding sensor and algorithm plugins
- building `.rsnxplugin` bundles
- local development harnesses

NexusN3 Edge Core is responsible for:

- installing bundles
- maintaining plugin catalogs
- runtime discovery
- isolated sensor/algorithm host execution

## Sensor Manager

The sensor manager remains transport-generic. Installed sensor plugins provide
protocol logic and runtime hooks, while `nexus_n3.sensor_manager` handles:

- discovery
- connection lifecycle
- stream start/stop
- adapter pooling
- battery precheck
- BLE backend selection

Supported BLE backends:

- `bleak`
- `nexus_ble_gateway`

Select the backend with `BLE_BACKEND` in `runtime.env` or `--ble-backend` at
startup.

When using `nexus_ble_gateway`, set `GATEWAY_SERIAL_PORT` for the host OS:

- Linux example: `/dev/serial/by-id/...`
- Windows example: `COM3`

On Windows, this is the preferred BLE path because it uses the serial gateway
rather than host BLE stack integration.

## File Output

All runtime output is written through the generic file manager path under:

```text
nexus_n3_outputs/<site>/<session_label>/session_<timestamp>/
```

This includes:

- raw CSV sensor data
- real-time NDJSON compute results
- intermediate NDJSON results
- consolidated NDJSON results
- diagnostics NDJSON when enabled

On Linux standalone and master nodes, the optional hot-disk workflow can switch
the active output path to the managed removable disk mount. On non-Linux hosts,
file output remains local-only.

When a session is finalized, the session directory is zipped locally and the
source directory is removed.

## Deployment

Deployment documentation lives under `deployment/guides/`:

- `deployment/guides/MANUAL_DEPLOYMENT.md`
- `deployment/guides/SYSTEMD_DEPLOYMENT.md`
- `deployment/guides/ANSIBLE_DEPLOYMENT.md`
- `deployment/guides/DOCKER_DEPLOYMENT.md`
- `deployment/guides/DISTRIBUTED_DEPLOYMENT.md`

Use the manual guide when you want to:

- create a host-side runtime virtual environment
- install the built core wheel
- install built `.rsnxplugin` bundles
- configure `runtime.env`
- start the system without Ansible or Docker

## Documentation

Sphinx source documentation lives under `docs/source/`.

To build and sync docs into the admin package:

```bash
cd docs
make html
cd ..
bash scripts/sync_docs.sh
```
