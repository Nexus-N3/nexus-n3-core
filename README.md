# Nexus N3 Core

Nexus N3 Core is the runtime layer for sensor acquisition, plugin-backed
processing, session orchestration, and host deployment.

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
- plugin build and scaffold tooling: `nexus-n3-plugin-tooling`
- plugin development workspace: `nexus-n3-plugin-catalog/`

## Installation

Use an isolated Python environment. A system-wide install can pick up unrelated
global packages and break imports before Nexus N3 Core starts.

Recommended local install with `venv`:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install nexus-n3-core
```

Alternative install with `pipx`:

```bash
pipx install nexus-n3-core
```

Runtime configuration typically lives in:

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

This file controls:

- plugin root and optional dev bootstrap behavior
- BLE backend and gateway serial settings
- local ZeroMQ bindings
- Azure bridge settings
- admin/runtime settings

Because startup reads the runtime env file automatically, a configured local
environment can start with:

```bash
python nexus_n3_server.py
```

After installing from PyPI or TestPyPI, use:

```bash
nexus-n3-core
```

The installed console command forwards the same flags as the source entry point.
For example:

```bash
nexus-n3-core --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
```

## Running The Runtime

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

On non-Linux development hosts, including Windows laptops, the runtime stays on
the local `nexus_n3_outputs/` path. The removable USB hot-disk workflow is
disabled automatically there.

Other host-setup features such as access-point mode and kiosk setup are Linux
host concerns and are not part of the Windows development/runtime path.

That means a Windows development run should already:

- use local file output
- skip Linux hot-disk behavior
- avoid Linux host-provisioning features such as AP mode and kiosk setup

Standalone with admin UI:

```bash
python nexus_n3_server.py --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
```

Installed-package equivalent:

```bash
nexus-n3-core --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
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

## Sample Session Client

For a user-editable example that drives a live session against a running
server, use:

```bash
python run_sample_session.py --stream 60
```

This script is intended as a starting point rather than a fixed regression
test. Edit `SAMPLE_SUBJECTS` in [run_sample_session.py](/home/mike/Desktop/apps/dev/rs-nexus-project/nexus-n3-core/run_sample_session.py)
to match your own sensors, locations, and compute setup.

Useful staged runs:

```bash
python run_sample_session.py --discover
python run_sample_session.py --connect
python run_sample_session.py --identify
python run_sample_session.py --stream 60
```

## Plugin Model

### Runtime Artifact

The plugin artifact consumed by the runtime is:

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
     /path/to/nexus-n3-plugin-catalog/plugin-builds/sensors/nexus-n3-sensor-movesense-0.1.2.rsnxplugin \
     --plugin-root /opt/nexus-n3-plugins
   ```
4. start Nexus N3 Core

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

Nexus N3 Core is responsible for:

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

## Troubleshooting

If startup fails with an error like
`ImportError: cannot import name 'appengine' from 'urllib3.contrib'`, the
Python environment is mixing an old `requests-toolbelt` install with
`urllib3 2.x`. This is an environment conflict, not a Nexus N3 Core CLI issue.

Use a fresh `venv` or `pipx` environment instead of a system-wide install.

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

## Deployment And Operations

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
- start the runtime without Ansible or Docker

## Project Scope

This repository focuses on the runtime and its plugin execution model.

It does not try to bundle all sensor logic directly into the core package.
Sensor and algorithm implementations are expected to be delivered as plugins,
installed through `nexus_n3.plugins`, and discovered at runtime.

This separation keeps the runtime smaller, makes host responsibilities clearer,
and supports independent plugin development and release workflows.

## Documentation

Sphinx source documentation lives under `docs/source/`.

To build and sync docs into the admin package:

```bash
cd docs
make html
cd ..
bash scripts/sync_docs.sh
```
