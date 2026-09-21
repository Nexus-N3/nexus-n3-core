# Operations User Guide

## Purpose

This guide is for operators and field maintainers running Nexus N3 Core on
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
- the Wi-Fi sensor AP is active when Wi-Fi sensors are configured

## Service Operation

Typical runtime roles:

- `standalone`
- `master`
- `worker`
- `ai`

The runtime is normally started through `nexus-n3-core`, using
`/etc/nexus-n3/runtime.env` as its configuration source.

If you need to use a non-standard config file location, set:

```bash
export NEXUS_N3_ENV_FILE=/path/to/runtime.env
```

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

Plugin versions are immutable after installation. Build and install a higher
version whenever plugin code or its bundled SDK changes.

## Wi-Fi Sensor Operation

The configured Wi-Fi interface normally hosts the Nexus sensor AP. Sensors that
are already provisioned reconnect to it automatically. Reset devices into their
vendor AP mode only when explicitly testing or performing provisioning.

Network-stack recovery requires the fixed-purpose systemd service and sudoers
rule described in the deployment guide. Runtime recovery uses `sudo -n`; an
authentication prompt indicates an incomplete or incorrect deployment and must
not be accepted as normal operation on a headless system.

## Storage

Session data is written under the configured output root and finalized into zip
archives when a session is fully drained.

Each archive contains node-specific diagnostics. The master writes under
`master-diagnostics/`, each worker writes under `<worker-node-id>-diagnostics/`,
and standalone mode writes under `standalone-diagnostics/`. Each directory
contains `session_diagnostics.json` for the final summary and
`session_diagnostics.jsonl` for ordered runtime events. These structured
diagnostics are written for every recording; they do not require the optional
`--diagnostics` pipeline-debug mode.

For mixed BLE/Wi-Fi sessions, inspect
`latest_gateway_diagnostics.diagnostics.BLE` and
`latest_gateway_diagnostics.diagnostics.WIFI` in the summary. The Wi-Fi entry
includes adapter/backend state and any diagnostics exported by the installed
sensor plugin.

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
- `deployment/guides/DISTRIBUTED_DEPLOYMENT.md`
- `deployment/guides/DOCKER_DEPLOYMENT.md`
