# Ansible Deployment

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
- sensor plugin bundles in `plugin-builds/sensors/`
- algorithm plugin bundles in `plugin-builds/algorithms/`

The default bundle roots can be overridden in host or group vars:

- `nexus_sensor_plugin_bundle_root`
- `nexus_algorithm_plugin_bundle_root`

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
- `nexus_server_local_path`
- `nexus_runtime_env_remote_path`
- `nexus_plugin_root`
- `nexus_plugin_bundle_staging_root`

Bundle variables:

- `nexus_sensor_plugin_bundle_root`
- `nexus_algorithm_plugin_bundle_root`
- `nexus_sensor_plugin_bundles`
- `nexus_algorithm_plugin_bundles`

Role switches:

- `nexus_install_sensor_plugins`
- `nexus_install_algorithm_plugins`

If the explicit bundle lists are empty, Ansible auto-discovers all
`.rsnxplugin` files in the corresponding bundle root.

## Phase 2: Software Deployment

Software deployment is separate from host provisioning. These playbooks install
the built wheel, runtime env, admin assets, and plugin bundles onto hosts that
have already been prepared.

## Standalone Deployment

1. Build the core wheel.
2. Build the required sensor and algorithm `.rsnxplugin` bundles.
3. Place the wheel in `dist/`.
4. Place bundles in:
   - `plugin-builds/sensors/`
   - `plugin-builds/algorithms/`
5. Set the standalone host vars, especially:
   - `nexus_role: standalone`
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

## What The Role Does

The `nexus_release` role:

- uploads `runtime.env`
- creates the runtime virtual environment
- installs the `nexus-n3-core` wheel
- uploads admin UI assets
- stages `.rsnxplugin` bundles on the target
- installs bundles into `NEXUS_N3_PLUGIN_ROOT` using `python -m nexus_n3.plugins install`
- skips bundle installation if the exact plugin version is already installed
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

- [rollout_plugin_bundle.sh](/home/mike/Desktop/apps/dev/nexus-n3-project/nexus-n3-core/deployment/rollout_plugin_bundle.sh)

That script copies one built `.rsnxplugin` bundle to selected nodes and runs the
existing installer locally on those targets.

Because it accepts a direct `--bundle` path, the source artifact can come from:

- a local development machine
- a mounted USB disk
- a directory populated by a future admin-app upload flow

That is intentional. The deployment source may vary, but the installer path on
the node should stay the same.

## Notes

- Production deployment should use built bundles only.
- Do not point Ansible at `nexus-n3-plugin-catalog/` source trees for production rollout.
- The master is not intended to act as a runtime source of plugin code for
  workers or AI nodes.
- The `deploy_*` playbooks are the software rollout path.
- The `provision_*` playbooks and `site.yml` are the Linux host-preparation path.
