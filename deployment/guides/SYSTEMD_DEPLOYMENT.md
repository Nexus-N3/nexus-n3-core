# Systemd Deployment

This guide covers a direct host deployment using systemd without Ansible or
Docker.

Use this path when:

- you want the runtime to start automatically on a host
- `/etc/nexus-n3/runtime.env` is the configuration source of truth
- plugins are already installed into `/opt/nexus-n3-plugins`
- the installed runtime wheel already exists under `/opt/nexus-n3-core/venv`

This path is lighter than the Ansible deployment, but less managed.

## Runtime Model

The current systemd model is:

- start the installed `nexus-n3-core` CLI from `/opt/nexus-n3-core/venv/bin/`
- load `/etc/nexus-n3/runtime.env`
- run as a user that can read both the env file and plugin root

It should not depend on:

- `nexus_n3_server.py`
- a live repo checkout under the service user home directory
- plugin source trees on the host

## Prerequisites

Before enabling the service, ensure:

1. `/opt/nexus-n3-core/venv/bin/nexus-n3-core` exists.
2. `/etc/nexus-n3/runtime.env` exists and is correct for the host role.
3. `/opt/nexus-n3-plugins` contains the required installed plugins.
4. the chosen service user can read:
   - `/etc/nexus-n3/runtime.env`
   - `/opt/nexus-n3-plugins`
   - installed plugin manifests and runtime virtual environments

For the Raspberry Pi deployment documented on July 21, 2026, the runtime user
is typically `rsnexus`.

## Required Permissions

If the service user is `rsnexus`, the minimum expected shape is:

```bash
sudo chgrp rsnexus /etc/nexus-n3/runtime.env
sudo chmod 640 /etc/nexus-n3/runtime.env
sudo chown -R rsnexus:rsnexus /opt/nexus-n3-plugins
```

Expected env file mode:

```text
-rw-r----- 1 root rsnexus /etc/nexus-n3/runtime.env
```

## Example System Service

Create `/etc/systemd/system/nexus-n3.service`:

```ini
[Unit]
Description=Nexus N3 Core (standalone)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=rsnexus
Group=rsnexus
WorkingDirectory=/opt/nexus-n3-core
Environment=PYTHONUNBUFFERED=1
Environment=NEXUS_N3_ENV_FILE=/etc/nexus-n3/runtime.env
EnvironmentFile=-/etc/nexus-n3/runtime.env
ExecStart=/opt/nexus-n3-core/venv/bin/nexus-n3-core --gateway zeromq_gateway --site lunar --customer-id customer-dlr --site-id lunar_facility_cologne --site-name "Lunar Cologne" --use-async --role standalone --admin --admin-host 0.0.0.0 --admin-port 9000 --ble-backend nexus_ble_gateway --bridge azure_bridge --azure-bridge-remote-control
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Adjust the CLI flags for the actual host role and deployment settings.

## Install And Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable nexus-n3
sudo systemctl restart nexus-n3
systemctl status nexus-n3 --no-pager
```

## Verification

Useful checks:

```bash
sudo sed -n '1,20p' /etc/systemd/system/nexus-n3.service
ls -l /etc/nexus-n3/runtime.env
systemctl status nexus-n3 --no-pager
journalctl -u nexus-n3 -n 100 -l --no-pager
```

The `ExecStart=` line must remain separate from `Restart=on-failure`.

## Common Failure Signatures

- `unrecognized arguments: --azure-bridge-remote-controlRestart=on-failure`
  means the service file is malformed and `Restart=on-failure` has been
  concatenated onto `ExecStart`.
- `PermissionError: [Errno 13] Permission denied: '/etc/nexus-n3/runtime.env'`
  means the service user cannot read the env file.
- `PermissionError: [Errno 13] Permission denied: '/opt/nexus-n3-plugins/...'`
  means the service user cannot read the installed plugin tree.

## When To Prefer Another Guide

- Use [MANUAL_DEPLOYMENT.md](./MANUAL_DEPLOYMENT.md)
  when you want the full host-side install process from wheel and bundles.
- Use [ANSIBLE_DEPLOYMENT.md](./ANSIBLE_DEPLOYMENT.md)
  when you want a managed rollout with runtime env, plugin installation, and
  service installation handled together.
- Use [DOCKER_DEPLOYMENT.md](./DOCKER_DEPLOYMENT.md)
  for containerized runtime or dev deployment.
