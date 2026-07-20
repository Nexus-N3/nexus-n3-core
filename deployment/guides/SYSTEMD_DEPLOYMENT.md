# Systemd Deployment

This guide covers a direct host deployment using the user-level systemd units
under `deployment/systemd/`.

Use this path when:

- you want the runtime to start automatically on a host without Docker
- you want `/etc/nexus-n3/runtime.env` to remain the configuration source of truth
- plugins are already installed into `/opt/nexus-n3-plugins`

This path is lighter than the Ansible deployment, but less managed. It assumes
you already have:

- a working Python runtime on the host
- the core repository checked out at a stable path
- `/etc/nexus-n3/runtime.env`
- `/opt/nexus-n3-plugins`

## Runtime Model

The shipped systemd units now follow the same runtime-env-first model as the
other deployment shapes.

They:

- set `NEXUS_N3_ENV_FILE=/etc/nexus-n3/runtime.env`
- load `EnvironmentFile=-/etc/nexus-n3/runtime.env`
- start `nexus_n3_server.py` with only role-specific arguments

They do not hard-code:

- gateway selection
- site/customer metadata
- plugin root
- BLE backend settings

Those values should come from `runtime.env`.

## Unit Files

Available unit files:

- `deployment/systemd/nexus_n3_standalone.service`
- `deployment/systemd/nexus_n3_master.service`
- `deployment/systemd/nexus_n3_worker@.service`

Helper scripts:

- `deployment/systemd/setup_standalone.sh`
- `deployment/systemd/setup_services.sh`
- `deployment/systemd/stop_services.sh`

## Prerequisites

Before enabling the units, ensure:

1. `/etc/nexus-n3/runtime.env` exists and is correct for the host role.
2. `/opt/nexus-n3-plugins` contains the required installed plugins.
3. the repository path used by the unit files exists:

```text
%h/Desktop/apps/dev/nexus-n3-project/nexus-n3-core
```

If the checkout lives elsewhere, update the unit files before enabling them.

## Standalone Setup

From `deployment/systemd/`:

```bash
bash setup_standalone.sh
```

That copies the standalone unit into the user systemd directory, reloads the
user daemon, enables the service, and starts it immediately.

## Master + Worker Setup

From `deployment/systemd/`:

```bash
bash setup_services.sh
```

That currently enables:

- `nexus_n3_master.service`
- `nexus_n3_worker@worker_A.service`

If you want other worker names, edit `setup_services.sh` or enable additional
instances manually:

```bash
systemctl --user enable --now nexus_n3_worker@worker_B.service
```

## Stop And Disable

To stop and disable the shipped units:

```bash
bash stop_services.sh
```

## Operational Notes

- This deployment shape expects plugins to be installed ahead of time.
- New plugins become available for future discovery and future session setup,
  but clients should query the live supported plugin inventory before building
  a new session configuration.
- The units are user services, not system-wide `/etc/systemd/system` services.

## When To Prefer Another Guide

- Use [MANUAL_DEPLOYMENT.md](./MANUAL_DEPLOYMENT.md)
  when you want the full host-side install process from wheel and bundles.
- Use [ANSIBLE_DEPLOYMENT.md](./ANSIBLE_DEPLOYMENT.md)
  when you want a managed multi-node rollout with service installation.
- Use [DOCKER_DEPLOYMENT.md](./DOCKER_DEPLOYMENT.md)
  for containerized runtime or dev deployment.
