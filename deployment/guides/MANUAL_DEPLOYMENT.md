# Manual Deployment

This guide describes a direct host deployment of `nexus-n3-core` without
Ansible or Docker.

It covers:

- building the core wheel
- building target-specific `.rsnxplugin` bundles
- copying artifacts to the target host
- installing the runtime and plugins manually
- starting the runtime with the installed `nexus-n3-core` CLI

It does not require:

- plugin source trees on the target
- `nexus-n3-plugin-tooling` on the target

## 1. Prepare The Build Machine

The build machine should contain:

- `nexus-n3-core/`
- `nexus-n3-plugin-tooling/`
- `nexus-n3-plugin-catalog/`

Recommended sibling layout:

```text
rs-nexus-project/
  nexus-n3-core/
  nexus-n3-plugin-tooling/
  nexus-n3-plugin-catalog/
```

## 2. Build The Core Wheel

From the core repository:

```bash
cd /path/to/rs-nexus-project/nexus-n3-core
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip build
python -m build
```

This produces:

```text
dist/nexus_n3_core-<version>-py3-none-any.whl
```

## 3. Install Plugin Tooling On The Build Machine

From the tooling repository:

```bash
cd /path/to/rs-nexus-project/nexus-n3-plugin-tooling
./install.sh
nexus-n3-plugin --help
```

The standard operator command is `nexus-n3-plugin`.

## 4. Build Target-Specific Plugin Bundles

For a Raspberry Pi deployment target, build into:

```bash
cd /path/to/rs-nexus-project/nexus-n3-plugin-catalog
mkdir -p plugin-builds/sensors/rpi plugin-builds/algorithms/rpi
```

Build sensor bundles:

```bash
nexus-n3-plugin build \
  --plugin-root sensors/nexus-n3-sensor-movella-dot \
  --output-dir plugin-builds/sensors/rpi \
  --target rpi

nexus-n3-plugin build \
  --plugin-root sensors/nexus-n3-sensor-movesense \
  --output-dir plugin-builds/sensors/rpi \
  --target rpi
```

Build algorithm bundles:

```bash
nexus-n3-plugin build \
  --plugin-root algorithms/nexus-n3-algorithm-pass-through \
  --output-dir plugin-builds/algorithms/rpi \
  --target rpi

nexus-n3-plugin build \
  --plugin-root algorithms/nexus-n3-algorithm-generic-data-summary \
  --output-dir plugin-builds/algorithms/rpi \
  --target rpi

nexus-n3-plugin build \
  --plugin-root algorithms/nexus-n3-algorithm-standard-loading-intensity \
  --output-dir plugin-builds/algorithms/rpi \
  --target rpi

nexus-n3-plugin build \
  --plugin-root algorithms/nexus-n3-algorithm-ecg-rhythm \
  --output-dir plugin-builds/algorithms/rpi \
  --target rpi
```

Verify the artifacts:

```bash
find plugin-builds -type f -name '*-rpi.rsnxplugin' | sort
```

Notes:

- the default build now aims to create a dependency-complete target bundle
- if the build machine has connectivity, target wheels are fetched during the
  build
- if the build machine is offline, provide a target wheelhouse and use the
  tooling options documented in `nexus-n3-plugin-tooling`

## 5. Prepare Artifacts For Transfer

On the build machine, prepare:

- `nexus-n3-core/dist/nexus_n3_core-<version>-py3-none-any.whl`
- the required sensor plugin `*-rpi.rsnxplugin` bundles
- the required algorithm plugin `*-rpi.rsnxplugin` bundles

Recommended layout:

```text
nexus-n3-core/dist/
nexus-n3-plugin-catalog/plugin-builds/
  sensors/
    rpi/
  algorithms/
    rpi/
```

Copy the wheel and bundles to the target host by your preferred method:

- `scp`
- mounted USB disk
- other file transfer path

## 6. Create Target Directories

On the target host:

```bash
sudo mkdir -p /opt/nexus-n3-core
sudo mkdir -p /opt/nexus-n3-plugins
sudo mkdir -p /etc/nexus-n3
```

## 7. Install The Core Runtime

Copy the wheel to the target host, then create the runtime environment:

```bash
cd /opt/nexus-n3-core
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install /path/to/nexus_n3_core-<version>-py3-none-any.whl
```

The installed runtime command is:

```bash
/opt/nexus-n3-core/venv/bin/nexus-n3-core
```

## 8. Install Plugin Bundles

Install each required bundle into the plugin root:

```bash
/opt/nexus-n3-core/venv/bin/python -m nexus_n3.plugins install \
  /path/to/nexus-n3-plugin-catalog/plugin-builds/sensors/rpi/nexus-n3-sensor-movella-dot-<version>-rpi.rsnxplugin \
  --plugin-root /opt/nexus-n3-plugins

/opt/nexus-n3-core/venv/bin/python -m nexus_n3.plugins install \
  /path/to/nexus-n3-plugin-catalog/plugin-builds/algorithms/rpi/nexus-n3-algorithm-standard-loading-intensity-<version>-rpi.rsnxplugin \
  --plugin-root /opt/nexus-n3-plugins
```

Repeat for any additional bundles required by the node role.

Role guidance:

- `standalone`: sensor plugins and algorithm plugins
- `master`: sensor plugins and algorithm plugins
- `worker`: sensor plugins and algorithm plugins
- `ai`: algorithm plugins only

## 9. Fix Runtime Ownership

The service user must be able to read:

- `/etc/nexus-n3/runtime.env`
- `/opt/nexus-n3-plugins`
- installed plugin manifests and runtime virtual environments

If the runtime will run as `rsnexus`:

```bash
sudo chgrp rsnexus /etc/nexus-n3/runtime.env
sudo chmod 640 /etc/nexus-n3/runtime.env
sudo chown -R rsnexus:rsnexus /opt/nexus-n3-plugins
```

If another service user will be used, replace `rsnexus` accordingly.

## 10. Create The Runtime Environment File

Create `/etc/nexus-n3/runtime.env`.

At minimum, set:

```text
NEXUS_N3_PLUGIN_ROOT=/opt/nexus-n3-plugins
NEXUS_N3_PLUGIN_USE_SYSTEM_SITE_PACKAGES=0
BLE_BACKEND=nexus_ble_gateway
GATEWAY_SERIAL_PORT=/dev/serial/by-id/...
ZEROMQ_CMD_BIND=tcp://*:5555
ZEROMQ_EVENT_BIND=tcp://*:5556
```

Also set:

- Azure variables if using the Azure bridge
- any site/customer metadata required for the deployment
- `NEXUS_N3_BOOTSTRAP_PLUGINS=0` for production/manual deployment

Manual production deployment should install built bundles explicitly. It should
not rely on dev bootstrap.

## 11. Start The Runtime Manually

Run the installed CLI:

```bash
/opt/nexus-n3-core/venv/bin/nexus-n3-core \
  --role standalone \
  --admin \
  --admin-host 0.0.0.0 \
  --admin-port 9000
```

If the host vars or runtime env require it, also pass:

- `--gateway zeromq_gateway`
- `--ble-backend nexus_ble_gateway`
- `--bridge azure_bridge`
- `--azure-bridge-remote-control`

## 12. Verify The Deployment

Recommended checks:

```bash
/opt/nexus-n3-core/venv/bin/python -m nexus_n3.plugins show-root
/opt/nexus-n3-core/venv/bin/python -m nexus_n3.plugins show-dev-list --json
/opt/nexus-n3-core/venv/bin/python -m nexus_n3.plugins show-catalog --json
```

Also verify:

- admin UI reachable on port `9000`
- expected sensor and algorithm plugins appear in the startup summary
- gateway ports are available to clients if required:
  - `tcp://<host-ip>:5555`
  - `tcp://<host-ip>:5556`

## 13. Optional Systemd Service

For a persistent host deployment, create a service that:

- runs `/opt/nexus-n3-core/venv/bin/nexus-n3-core`
- loads `/etc/nexus-n3/runtime.env`
- runs as a user that can read the env file and plugin root
- restarts on failure

Use [SYSTEMD_DEPLOYMENT.md](./SYSTEMD_DEPLOYMENT.md) for the user-service path
or [ANSIBLE_DEPLOYMENT.md](./ANSIBLE_DEPLOYMENT.md) for the managed
system-level path.

## 14. Updating Plugins Later

To deploy a new plugin version later:

1. copy the new target-specific `.rsnxplugin` bundle to the host
2. run `python -m nexus_n3.plugins install ... --plugin-root /opt/nexus-n3-plugins`
3. restart the runtime if required by the operational flow

The future admin-app upload path is expected to use the same bundle installer
mechanism.
