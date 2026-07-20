# Deployment

NexusN3 Edge Core supports these deployment paths:

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

The removable USB hot-disk workflow is a Linux host feature for deployed edge
systems such as Raspberry Pi and other edge computers. On laptops and Windows
hosts, the runtime falls back to local output storage and does not expose the
Linux USB mount/unmount controls.

## Systemd Deployment

For a direct host deployment using the user systemd units under
`deployment/systemd/`, see:

- `deployment/guides/SYSTEMD_DEPLOYMENT.md`

## Ansible Deployment

For role-based deployment of standalone, master, worker, and AI nodes, see:

- `deployment/guides/ANSIBLE_DEPLOYMENT.md`

## Docker Deployment

For containerized standalone deployment, see:

- `deployment/guides/DOCKER_DEPLOYMENT.md`

## Distributed Deployment

For role expectations and plugin distribution rules across master, worker, and
AI nodes, see:

- `deployment/guides/DISTRIBUTED_DEPLOYMENT.md`
