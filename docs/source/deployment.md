# Deployment

Nexus N3 Core supports these deployment paths:

## Manual Deployment

For a step-by-step host-side deployment without Ansible or Docker, see:

- `deployment/guides/MANUAL_DEPLOYMENT.md`

This covers:

- creating the runtime virtual environment
- installing the built core wheel
- installing built `.rsnxplugin` bundles
- configuring `runtime.env`
- optional Linux-only USB mount script setup
- optional systemd setup
- Linux Wi-Fi sensor AP and fixed-purpose recovery setup

The removable USB hot-disk workflow is a Linux host feature for deployed edge
systems such as Raspberry Pi and other edge computers. On laptops and Windows
hosts, the runtime falls back to local output storage and does not expose the
Linux USB mount/unmount controls.

## Systemd Deployment

For a direct host deployment using the user systemd units under
`deployment/systemd/`, see:

- `deployment/guides/SYSTEMD_DEPLOYMENT.md`

Wi-Fi recovery is deliberately non-interactive. The deployment installs a
root-owned `nexus-n3-wifi-recovery.service` and a sudoers rule that permits the
Nexus service identity to run only this fixed command:

```text
sudo -n /usr/bin/systemctl restart nexus-n3-wifi-recovery.service
```

For a development host, install it with:

```bash
sudo deployment/systemd/install_wifi_recovery.sh "$USER" EE
```

The second argument is the regulatory domain. Production installations should
use the `nexus_sensor_access_point` Ansible role. No PolicyKit desktop agent or
interactive authentication is required at runtime.

## Ansible Deployment

For role-based deployment of standalone, master, worker, and AI nodes, see:

- `deployment/guides/ANSIBLE_DEPLOYMENT.md`

## Docker Deployment

For containerized standalone deployment, see:

- `deployment/guides/DOCKER_DEPLOYMENT.md`

## Distributed Deployment

For deployment identity, capability-based subject assignment, shared session
storage, per-node diagnostics, the distributed drain barrier, and plugin rules
across master, worker, and AI nodes, see:

- `deployment/guides/DISTRIBUTED_DEPLOYMENT.md`
