# Operations User Guide

## Purpose

This guide is for operators and field maintainers running NexusN3 Edge Core on
deployed systems.

## First Checks

Use these first:

```bash
bash scripts/health_check.sh
```

Check:

- the service is running
- the admin UI is reachable
- the BLE backend is configured correctly
- the expected plugin inventory was detected at startup

## Service Operation

Typical runtime roles:

- `standalone`
- `master`
- `worker`
- `ai`

The runtime is normally started through `nexus_n3_server.py`, using
`/etc/nexus-n3/runtime.env` as its configuration source.

## Plugin Operations

Production systems should use built `.rsnxplugin` bundles only.

Do not deploy:

- `nexus-n3-plugin-catalog/`
- plugin source repos
- `nexus-n3-plugin-tooling`

Install bundles with:

```bash
python -m nexus_n3.plugins install /path/to/plugin.rsnxplugin --plugin-root /opt/nexus-n3-plugins
```

## Storage

Session data is written under the configured output root and finalized into zip
archives when a session is fully drained.

If using the removable USB disk workflow on a Linux edge host, the manual
helper scripts are:

```bash
sudo bash scripts/usb_disk_add_or_remount.sh
sudo bash scripts/usb_disk_safe_unplug.sh
```

On non-Linux hosts, including Windows laptops used for development or support,
the runtime keeps using the local output directory and does not expose these
USB management actions.

## References

See also:

- `deployment/guides/MANUAL_DEPLOYMENT.md`
- `deployment/guides/SYSTEMD_DEPLOYMENT.md`
- `deployment/guides/ANSIBLE_DEPLOYMENT.md`
- `deployment/guides/DOCKER_DEPLOYMENT.md`
