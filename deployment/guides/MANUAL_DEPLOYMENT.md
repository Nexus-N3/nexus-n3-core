# Manual Deployment

This guide describes a full manual deployment flow for NexusN3 Edge Core
without Ansible or Docker.

It covers:

- building the core wheel
- building `.rsnxplugin` bundles with `nexus-n3-plugin-tooling`
- copying artifacts to the target host
- installing the runtime and plugins manually

It does not require:

- plugin source trees on the target
- `nexus-n3-plugin-tooling` on the target

## 1. Prepare The Build Machine

The build machine should contain:

- this repository
- `nexus-n3-plugin-tooling`
- `nexus-n3-plugin-catalog/`

Recommended sibling layout:

```text
nexus-n3-project/
  nexus-n3-core/
  nexus-n3-plugin-tooling/
  nexus-n3-plugin-catalog/
```

## 2. Build The Core Wheel

From the core repository:

```bash
cd /path/to/nexus-n3-core
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip build
python -m build
```

This produces:

```text
dist/nexus_n3_core-<version>-py3-none-any.whl
```

Also retain:

- `nexus_n3_server.py`

The current deployment model still launches the runtime through that script.

## 3. Install Plugin Tooling On The Build Machine

From the tooling repository:

```bash
cd /path/to/nexus-n3-plugin-tooling
./install.sh
source .venv/bin/activate
nexusn3-plugin --help
```

This installs the plugin CLI and SDK into the tooling virtual environment.

## 4. Build The Required `.rsnxplugin` Bundles

Create persistent output directories for retained bundles:

```bash
mkdir -p /path/to/nexus-n3-core/plugin-builds/sensors
mkdir -p /path/to/nexus-n3-core/plugin-builds/algorithms
```

Build the reference sensor plugin:

```bash
cd /path/to/nexus-n3-plugin-tooling
source .venv/bin/activate

nexusn3-plugin build \
  --plugin-root /path/to/nexus-n3-plugin-catalog/sensors/nexus-n3-sensor-movella-dot \
  --output-dir /path/to/nexus-n3-core/plugin-builds/sensors
```

nexusn3-plugin build \
  --plugin-root /path/to/nexus-n3-plugin-catalog/sensors/nexus-n3-sensor-movella-dot \
  --output-dir /path/to/nexus-n3-plugin-catalog/plugin-builds/sensors

Build the reference algorithm plugin:

```bash
cd /path/to/nexus-n3-plugin-tooling
source .venv/bin/activate

nexusn3-plugin build \
  --plugin-root /path/to/nexus-n3-plugin-catalog/algorithms/nexus-n3-algorithm-standard-loading-intensity \
  --output-dir /path/to/nexus-n3-core/plugin-builds/algorithms
```

nexusn3-plugin build \
  --plugin-root /path/to/nexus-n3-plugin-catalog/algorithms/nexus-n3-algorithm-standard-loading-intensity \
  --output-dir /path/to/nexus-n3-plugin-catalog/plugin-builds/algorithms

this is by default now as it makes sense. it doesnt make sense to depend on system packages. that defeats the object.
For offline-complete bundles, include third-party dependency wheels explicitly:

```bash
nexusn3-plugin build \
  --plugin-root /path/to/nexus-n3-plugin-catalog/algorithms/nexus-n3-algorithm-standard-loading-intensity \
  --output-dir /path/to/nexus-n3-core/plugin-builds/algorithms \
  --artifact /path/to/wheels/numpy-<version>.whl \
  --artifact /path/to/wheels/scipy-<version>.whl
```

Important:

- `nexusn3-plugin build` does not auto-fetch third-party dependency wheels - it does now.
- use a persistent `--output-dir`, not `/tmp`, if you want to retain the built
  bundles for transfer/deployment

## 5. Prepare Artifacts For Transfer

On the build machine, prepare:

- `dist/nexus_n3_core-<version>-py3-none-any.whl`
- `nexus_n3_server.py`
- the required sensor plugin `.rsnxplugin` bundles
- the required algorithm plugin `.rsnxplugin` bundles

Recommended layout on the build machine:

```text
dist/
plugin-builds/
  sensors/
  algorithms/
```

Copy the wheel, `nexus_n3_server.py`, and bundles to the target host by your
preferred method:

- `scp`
- mounted USB disk
- other file transfer path

## 6. Create Target Directories

On the target host:

```bash
sudo mkdir -p /opt/nexusn3-edge-core
sudo mkdir -p /opt/nexus-n3-plugins
sudo mkdir -p /etc/nexus-n3
sudo mkdir -p /var/lib/nexus-n3
```

If you want local output under a dedicated path:

```bash
sudo mkdir -p /exports/nexus_n3_data/nexus_n3_outputs
```

## 7. Install The Core Runtime

Create the runtime virtual environment:

```bash
cd /opt/nexusn3-edge-core
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install /path/to/nexus_n3_core-<version>-py3-none-any.whl
```

Copy the runtime entry script into the install root:

```bash
cp /path/to/nexus_n3_server.py /opt/nexusn3-edge-core/nexus_n3_server.py
```

## 8. Install Plugin Bundles

Install each required plugin bundle into the plugin root:

```bash
cd /opt/nexusn3-edge-core
source .venv/bin/activate

python -m nexus_n3.plugins install /path/to/nexus-n3-sensor-movella-dot-<version>.rsnxplugin --plugin-root /opt/nexus-n3-plugins

SENSOR EXAMPLE
python -m nexus_n3.plugins install /path/to/nexus-n3-plugin-catalog/plugin-builds/sensors/nexus-n3-sensor-movella-dot-0.1.0.rsnxplugin --plugin-root /opt/nexus-n3-plugins


python -m nexus_n3.plugins install /path/to/nexus-n3-algorithm-standard-loading-intensity-<version>.rsnxplugin --plugin-root /opt/nexus-n3-plugins

ALGO EXAMPLE

python -m nexus_n3.plugins install /path/to/nexus-n3-plugin-catalog/plugin-builds/algorithms/nexus-n3-algorithm-standard-loading-intensity-0.1.0.rsnxplugin --plugin-root /opt/nexus-n3-plugins


```

Repeat for any additional bundles required by the node role.

Role guidance:

- standalone: sensor plugins and algorithm plugins
- master: sensor plugins and algorithm plugins
- worker: sensor plugins and algorithm plugins
- ai: algorithm plugins only

## 9. Create The Runtime Environment File

Create `/etc/nexus-n3/runtime.env`.

Start from the repo example:

```bash
cp config/runtime-example.env /etc/nexus-n3/runtime.env
```

Then set at minimum:

```text
NEXUS_N3_PLUGIN_ROOT=/opt/nexus-n3-plugins
BLE_BACKEND=nexus_ble_gateway
GATEWAY_SERIAL_PORT=/dev/serial/by-id/...
ZEROMQ_CMD_BIND=tcp://*:5555
ZEROMQ_EVENT_BIND=tcp://*:5556
```

Also set:

- Azure variables if using the Azure bridge
- `NEXUS_N3_BOOTSTRAP_PLUGINS=0` for production/manual deployment

Manual production deployment should install built bundles explicitly. It should
not rely on dev bootstrap.

## 10. Optional USB Mount Scripts

If the host uses the removable USB data-disk path, the repository includes
manual helper scripts:

- `scripts/usb_disk_add_or_remount.sh`
- `scripts/usb_disk_safe_unplug.sh`

These can be copied to the target host and invoked manually when preparing or
removing the USB-backed output storage.

Typical usage:

```bash
sudo bash scripts/usb_disk_add_or_remount.sh
sudo bash scripts/usb_disk_safe_unplug.sh
```

Ansible deployments install templated copies under `/usr/local/bin`, but for a
manual deployment you can use the repository versions directly.

## 11. Start The Runtime Manually

Run from the installed runtime environment:

```bash
cd /opt/nexusn3-edge-core
source .venv/bin/activate
python nexus_n3_server.py --admin # uses the defaults which is likely ok for most
python nexus_n3_server.py --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000
```

If the runtime env file is present and correct, the server can also be started
with the simplified form:

```bash
python nexus_n3_server.py
```

## 12. Verify The Deployment

Recommended checks:

```bash
python -m nexus_n3.plugins show-root
python -m nexus_n3.plugins show-dev-list --json
bash scripts/health_check.sh
```

At startup, the runtime now logs the detected installed plugin inventory. Check
for a line similar to:

```text
[PLUGINS] root=/opt/nexus-n3-plugins sensors=... algorithms=...
```

Also verify:

- admin UI reachable on port `9000`
- BLE backend status is correct
- expected sensor and algorithm plugins appear in the startup summary

## 13. Optional Systemd Service

For a persistent host deployment, create a service that runs the installed
runtime inside `/opt/nexusn3-edge-core/.venv` and points at
`/etc/nexus-n3/runtime.env`.

At minimum, ensure the service:

- runs the runtime venv Python
- loads `/etc/nexus-n3/runtime.env`
- starts `nexus_n3_server.py`
- restarts on failure

The Ansible deployment is the reference for a managed systemd setup. Use that
if you want the full provisioned path.

## 14. Updating Plugins Later

To deploy a new plugin version later:

1. copy the new `.rsnxplugin` bundle to the host
2. run `python -m nexus_n3.plugins install ... --plugin-root /opt/nexus-n3-plugins`
3. restart the runtime if required by the current operational flow

The future admin-app upload path is expected to use the same bundle installer
mechanism.
