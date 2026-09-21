# Ansible Deployment

Ansible deployments target linux only.  Windows and MAC OS are for testing and development only. 

This guide covers deployment of `nexus-n3-core` with Ansible using:

- the built `nexus-n3-core` wheel
- built `.rsnxplugin` bundles

It does not deploy plugin source trees or `nexus-n3-plugin-tooling`.

The Ansible flow is now split into two explicit phases:

- host provisioning on Linux targets only
- software deployment of the built wheel and plugin bundles

Windows is treated as a development environment, not an Ansible deployment
target.

## Artifact Model

Ansible now expects these local artifacts on the control machine:

- `dist/nexus_n3_core-<version>-py3-none-any.whl`
- sensor plugin bundles in `../nexus-n3-plugin-catalog/plugin-builds/sensors/<target>/`
- algorithm plugin bundles in `../nexus-n3-plugin-catalog/plugin-builds/algorithms/<target>/`

The default bundle roots can be overridden in host or group vars:

- `nexus_plugin_bundle_target`
- `nexus_sensor_plugin_bundle_root`
- `nexus_algorithm_plugin_bundle_root`

For Raspberry Pi deployments, the intended build output is:

- `../nexus-n3-plugin-catalog/plugin-builds/sensors/rpi/*.rsnxplugin`
- `../nexus-n3-plugin-catalog/plugin-builds/algorithms/rpi/*.rsnxplugin`

The standard bundle build command is:

```bash
nexus-n3-plugin build \
  --plugin-root sensors/nexus-n3-sensor-movesense \
  --output-dir plugin-builds/sensors/rpi \
  --target rpi
```

Run that from the `nexus-n3-plugin-catalog/` repository root. For algorithms,
replace the `--plugin-root` and `--output-dir` paths accordingly.

### Python ABI and Target-Specific Bundle Directories

Dependency-complete plugin bundles are specific to the target operating system,
CPU architecture, Python implementation, Python version, and ABI whenever they
contain native wheels. For example, a bundle containing `cp310` wheels cannot be
installed by a Nexus N3 plugin runtime using Python 3.12 (`cp312`). The installer
validates declared target metadata and pip also rejects incompatible wheels.

The relevant interpreter is `nexus_plugin_installer_python`, which defaults to
the Python executable inside the deployed Nexus Core virtualenv. Installing an
additional Python version on the host does not change an existing Core virtualenv
or make an incompatible bundle usable. Rebuild the bundle for the Python version
used by the target runtime instead.

The build machine and deployment target do not need to use the same Python
version. The plugin tooling can cross-build an x86_64/Python 3.12 bundle while
running on an x86_64/Python 3.10 development machine:

```bash
nexus-n3-plugin build \
  --plugin-root sensors/nexus-n3-sensor-movesense \
  --output-dir plugin-builds/sensors/x86_64-py312 \
  --target local \
  --target-platform manylinux2014_x86_64 \
  --target-python-version 3.12 \
  --target-implementation cp \
  --target-abi cp312
```

Use the equivalent family directory for algorithms:

```bash
nexus-n3-plugin build \
  --plugin-root algorithms/nexus-n3-algorithm-ecg-rhythm \
  --output-dir plugin-builds/algorithms/x86_64-py312 \
  --target local \
  --target-platform manylinux2014_x86_64 \
  --target-python-version 3.12 \
  --target-implementation cp \
  --target-abi cp312
```

Keep separate bundle directories when deployments use different Python ABIs:

```text
plugin-builds/
  sensors/
    x86_64-py310/
    x86_64-py312/
  algorithms/
    x86_64-py310/
    x86_64-py312/
```

Select the matching directory in host or group vars:

```yaml
nexus_plugin_bundle_target: x86_64-py312
```

`nexus_plugin_bundle_target` is an Ansible directory selector; it does not have
to match one of the plugin tool's `--target` preset names. Both the sensor and
algorithm target directories must exist when their respective installation
switches are enabled. Setting the variable to an empty string selects bundles
directly from `plugin-builds/sensors/` and `plugin-builds/algorithms/`, but that
layout should only be used when a single target ABI needs to be supported.

Do not use `--slim` for offline Ansible deployment. A deployment bundle must
include all required third-party wheels, including transitive SDK dependencies
such as PyYAML. Installing those packages globally on the target does not satisfy
an isolated plugin virtualenv.

## Role-Based Plugin Deployment

Plugin deployment is role-based.

- `standalone`: sensor plugins and algorithm plugins
- `master`: sensor plugins and algorithm plugins
- `worker`: sensor plugins and algorithm plugins
- `ai`: algorithm plugins only

This matches the intended architecture:

- nodes that physically access sensors need sensor plugins
- nodes that execute compute need algorithm plugins
- AI nodes do not need sensor plugins

## Current Distributed Runtime Constraints

Distributed runtime now validates master and worker subject assignments against
node plugin capabilities, and AI nodes use the plugin runtime for algorithm
execution.

AI nodes still do not manage session files or write to the shared disk. They
remain compute-only in the current implementation.

## Inventory Model

The shipped inventory uses these groups:

- `master`
- `workers`
- `ai_nodes`

For a standalone deployment, the host may still live in the `master` group, but
its `nexus_role` should be `standalone`.

## Phase 1: Host Provisioning

Host provisioning configures the machine itself and is intended only for Linux
targets. That includes items such as:

- package dependencies
- kiosk setup
- USB hot-disk helpers
- access point setup
- shared storage setup

Use these playbooks first when preparing a new Linux host:

Distributed host provisioning:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/provision_distributed.yml
```

One node type only:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/provision_master.yml
ansible-playbook -i inventory.ini playbooks/provision_workers.yml
ansible-playbook -i inventory.ini playbooks/provision_ai_nodes.yml
```

Specific host:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/provision_master.yml -e nexus_provision_hosts=nexus-n3-master
```

The legacy `site.yml` entry point is now host-provisioning-only and no longer
deploys the application release.

If a host is not Linux, these provisioning playbooks fail early by design.

## Important Variables

Core deployment variables:

- `nexus_release_local_path`
- `nexus_runtime_env_remote_path`
- `nexus_plugin_root`
- `nexus_plugin_bundle_staging_root`

Distributed identity variables:

- `nexus_customer_id`
- `nexus_site`
- `nexus_site_id`
- `nexus_site_name`
- `nexus_node_id` (unique per worker)

The customer and site values must match the master. WorkerNode receives them
from the rendered command line rather than loading them independently. For a
worker, the service command includes:

```text
--customer-id <customer-id> --site <site> --site-id <site-id> \
--site-name "<site name>" --role worker --node-id <worker-node-id>
```

Bundle variables:

- `nexus_plugin_bundle_target`
- `nexus_sensor_plugin_bundle_root`
- `nexus_algorithm_plugin_bundle_root`
- `nexus_sensor_plugin_bundles`
- `nexus_algorithm_plugin_bundles`

Role switches:

- `nexus_install_sensor_plugins`
- `nexus_install_algorithm_plugins`

If the explicit bundle lists are empty, Ansible reads the manifests in the
corresponding bundle root and selects only the newest version of each plugin
ID. Historical bundles may remain in `plugin-builds/` without being installed.
An explicit bundle list must contain at most one version of each plugin ID.

By default, `nexus_plugin_prune_inactive_versions: true` makes the deployed
plugin root converge on those selected versions. Ansible installs missing
versions, explicitly activates each desired version, and only then removes its
inactive superseded versions. Set the variable to `false` to retain inactive
versions for manual rollback.

## Runtime Ownership Model

The deployed service runs as `{{ ansible_user }}` for the target host, which is
`rsnexus` in the shipped Raspberry Pi inventory.

That means the deployment must leave these paths readable by the service user:

- `/etc/nexus-n3/runtime.env`
- `/opt/nexus-n3-plugins`
- all installed plugin manifests, wheels, and runtime virtual environments under
  `/opt/nexus-n3-plugins/installed/`

The release role now enforces that by:

- writing `runtime.env` as `root:<service-group>` with mode `0640`
- normalizing ownership of the plugin root recursively to the service
  user/group after bundle installation

## Phase 2: Software Deployment

Software deployment is separate from host provisioning. These playbooks install
the built wheel, runtime env, admin assets, and plugin bundles onto hosts that
have already been prepared. The installed `nexus-n3-core` CLI entry point is
what the deployed systemd service runs.

## Standalone Deployment

1. Build the core wheel.
2. Build the required sensor and algorithm `.rsnxplugin` bundles.
3. Place the wheel in `dist/`.
4. Place bundles in:
   - `../nexus-n3-plugin-catalog/plugin-builds/sensors/rpi/`
   - `../nexus-n3-plugin-catalog/plugin-builds/algorithms/rpi/`
5. Set the standalone host vars, especially:
   - `nexus_role: standalone`
   - `nexus_plugin_bundle_target: rpi`
   - `nexus_ble_backend`
   - `nexus_ble_gateway_serial_port` if using the BLE gateway
   - Azure variables if the Azure bridge is enabled
6. Run:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml -e nexus_deploy_hosts=master
```

If you want to target a specific host directly:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml -e nexus_deploy_hosts=nexus-n3-master
```

### Core-Only Reinstall On Raspberry Pi

If you rebuilt `nexus-n3-core` and only want to reinstall the wheel onto the
target Raspberry Pi without reinstalling any sensor or algorithm plugin
bundles, disable both plugin installation switches:

the nexus_release_local_path in nexus_release/defaults.main.yml must be updated with the new release number

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  -e nexus_install_sensor_plugins=false \
  -e nexus_install_algorithm_plugins=false
```

That still:

- uploads the new wheel from `dist/`
- force-reinstalls `nexus-n3-core` into the target virtualenv
- updates runtime env and admin assets
- refreshes the systemd unit if needed
- restarts the `nexus-n3` service

That does not:

- copy `.rsnxplugin` bundles
- install missing sensor plugins
- install missing algorithm plugins

## Running Specific Parts With Tags

The release role supports these tags:

- `core_release`: the main release path
- `release_artifact`: local wheel checks and wheel upload
- `venv`: virtualenv creation
- `core_install`: install/reinstall the `nexus-n3-core` wheel
- `runtime_env`: runtime env validation and `runtime.env` templating
- `admin_assets`: admin UI asset upload
- `service`: systemd unit install and service enable/start
- `plugins`: all plugin bundle tasks
- `plugin_bundles`: plugin bundle staging/install tasks
- `sensor_plugins`: sensor plugin bundle tasks only
- `algorithm_plugins`: algorithm plugin bundle tasks only
- `filesystem`: install/output/log/root directory creation
- `firewall`: ufw-related rules

Examples:

Only reinstall the core wheel and restart the service:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --tags core_install,service
```

Only update the runtime env and systemd unit:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --tags runtime_env,service
```

Only deploy plugin bundles:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --tags plugin_bundles
```

Only deploy sensor plugin bundles:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --tags sensor_plugins
```

Skip plugin bundle work while still running the rest of the release role:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --skip-tags plugins
```

Preview which tagged tasks would run:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --tags core_install,service \
  --list-tasks
```

## Choosing Playbooks

Use playbook selection to choose the deployment scope first, then use tags to
limit which parts of that playbook execute.

- `playbooks/deploy_release.yml`: one release deployment target or group
- `playbooks/deploy_master.yml`: master group only
- `playbooks/deploy_workers.yml`: worker group only
- `playbooks/deploy_ai_nodes.yml`: AI node group only
- `playbooks/deploy_distributed.yml`: master, workers, and AI nodes together

Examples:

Deploy only worker nodes:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_workers.yml
```

Deploy only AI nodes, but only run the service-related tasks:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_ai_nodes.yml --tags service
```

### Example Raspberry Pi Build Sequence

From `nexus-n3-plugin-catalog/`:

```bash
mkdir -p plugin-builds/sensors/rpi plugin-builds/algorithms/rpi

nexus-n3-plugin build \
  --plugin-root sensors/nexus-n3-sensor-movella-dot \
  --output-dir plugin-builds/sensors/rpi \
  --target rpi

nexus-n3-plugin build \
  --plugin-root sensors/nexus-n3-sensor-movesense \
  --output-dir plugin-builds/sensors/rpi \
  --target rpi

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

Then verify the artifacts exist before running Ansible:

```bash
find plugin-builds -type f -name '*-rpi.rsnxplugin' | sort
```

## Distributed Deployment

For distributed deployments:

- master and workers should receive both sensor and algorithm bundles
- AI nodes should receive algorithm bundles only

Run:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_distributed.yml
```

This deploys the release role to:

- `master`
- `workers`
- `ai_nodes`

## Deploy One Node Type

Use these playbooks when you want to provision one node type without touching
the rest of the system.

Master nodes:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_master.yml
```

Worker nodes:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_workers.yml
```

AI nodes:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_ai_nodes.yml
```

You can also target a specific host:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_workers.yml -e nexus_deploy_hosts=nexus-n3-worker-02
```

That is the standard path for cases like adding a new worker to an existing
system where the master is already deployed.

After deployment, inspect the complete unit and command with:

```bash
ansible <worker-node-id> -b -m command -a 'systemctl cat nexus-n3'
ansible <worker-node-id> -b -m command \
  -a 'systemctl show nexus-n3.service --property=ExecStart --value'
```

The worker service does not enable the admin server. It still installs the
shared release payload used by the generic deployment role.

## What The Role Does

The `nexus_release` role:

- uploads `runtime.env`
- creates the runtime virtual environment
- installs the `nexus-n3-core` wheel
- uploads admin UI assets
- stages `.rsnxplugin` bundles on the target
- installs bundles into `NEXUS_N3_PLUGIN_ROOT` using `python -m nexus_n3.plugins install`
- skips bundle installation if the exact plugin version is already installed
- explicitly activates the selected version of each plugin
- removes inactive superseded versions when `nexus_plugin_prune_inactive_versions` is enabled
- normalizes plugin-root ownership for runtime readability
- installs and restarts the systemd service when required

## Quick Plugin Rollout

For plugin-only deployment without redeploying the core wheel, use:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_plugin_bundles.yml
```

That playbook installs role-appropriate plugin bundles only:

- master and workers get sensor and algorithm bundles
- AI nodes get algorithm bundles only

To deploy only the Standard Loading Intensity algorithm update while leaving
sensor plugins untouched:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_plugin_bundles.yml \
  -e nexus_install_sensor_plugins=false \
  -e '{"nexus_algorithm_plugin_bundles":["nexus-n3-algorithm-standard-loading-intensity-0.1.2-rpi.rsnxplugin"]}'
```

That installs and activates `0.1.2`, then removes inactive installed versions
of that plugin, including `0.1.0`. It does not alter installed sensor plugins.

You can target a subset of nodes:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_plugin_bundles.yml -e nexus_deploy_hosts=workers
```

or a single node:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_plugin_bundles.yml -e nexus_deploy_hosts=nexus-n3-worker-02
```

For a quick targeted plugin rollout outside a full Ansible playbook flow, use:

- [rollout_plugin_bundle.sh](../rollout_plugin_bundle.sh)

That script copies one built `.rsnxplugin` bundle to selected nodes and runs the
existing installer locally on those targets.

Because it accepts a direct `--bundle` path, the source artifact can come from:

- a local development machine
- a mounted USB disk
- a directory populated by a future admin-app upload flow

That is intentional. The deployment source may vary, but the installer path on
the node should stay the same.

## Minimal Recovery Runs

When only one deployment step needs to be corrected, it is not necessary to
wait for a full end-to-end rollout.

Useful restart points:

```bash
cd deployment/ansible

ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --start-at-task "Upload shared runtime env file"
```

That is useful when only `runtime.env` ownership/content needs to be refreshed.

```bash
cd deployment/ansible

ansible-playbook -i inventory.ini playbooks/deploy_release.yml \
  -e nexus_deploy_hosts=nexus-n3-master \
  --start-at-task "Install role-appropriate plugin bundles"
```

That is useful when only plugin installation or plugin-root ownership needs to
be corrected.

If only the unit file changed, rerun the release deploy and then verify:

```bash
sudo sed -n '1,20p' /etc/systemd/system/nexus-n3.service
sudo systemctl daemon-reload
sudo systemctl restart nexus-n3
systemctl status nexus-n3 --no-pager
```

The `ExecStart=` line must remain separate from `Restart=on-failure`.

## Troubleshooting

When the `nexus_sensor_access_point` role is enabled, it installs the
root-owned `nexus-n3-wifi-recovery.service` and an exact sudoers rule scoped to
`nexus_sensor_ap_service_user` (the Ansible connection user by default). The
rule authorizes only a non-interactive restart of that exact unit. Set the
runtime opt-in
`NEXUS_WIFI_ALLOW_NETWORK_STACK_RESTART=1` only where restarting the sensor
Wi-Fi stack is operationally acceptable.

Common failure signatures seen during Raspberry Pi rollout on July 21, 2026:

- `unrecognized arguments: --azure-bridge-remote-controlRestart=on-failure`
  means the systemd unit file is malformed and `Restart=on-failure` has been
  concatenated onto `ExecStart`.
- `PermissionError: [Errno 13] Permission denied: '/etc/nexus-n3/runtime.env'`
  means the runtime env file is not readable by the service user.
- `PermissionError: [Errno 13] Permission denied: '/opt/nexus-n3-plugins/...'`
  means the installed plugin tree is not readable by the service user.

Useful checks on the target:

```bash
sudo sed -n '1,20p' /etc/systemd/system/nexus-n3.service
ls -l /etc/nexus-n3/runtime.env
find /opt/nexus-n3-plugins -maxdepth 3 \( -type d -o -type f \) | head
systemctl status nexus-n3 --no-pager
journalctl -u nexus-n3 -n 100 -l --no-pager
```

## Notes

- Production deployment should use built bundles only.
- Do not point Ansible at `nexus-n3-plugin-catalog/` source trees for production rollout.
- The master is not intended to act as a runtime source of plugin code for
  workers or AI nodes.
- The `deploy_*` playbooks are the software rollout path.
- The `provision_*` playbooks and `site.yml` are the Linux host-preparation path.
