# Troubleshooting

## Admin UI Not Reachable

Check:

```bash
bash scripts/health_check.sh
```

Then confirm the runtime was started with:

- `--admin`
- the expected `--admin-host`
- the expected `--admin-port`

## Admin UI Shows an Old Core Version

Source checkouts read the release from the `[project]` version in
`pyproject.toml`. Installed deployments read the generated Python distribution
metadata. Both paths are centralized in `nexus_n3.core.version.get_core_version`
and are used by the admin UI, device information, gateway messages, Azure
reported properties, and plugin compatibility checks.

If a source run shows an old version, stop all existing server processes and
start `python nexus_n3_server.py --admin` from the intended checkout. If an
installed command shows an old version, rebuild and reinstall the Core package;
editing a source `pyproject.toml` elsewhere does not update an installed wheel.

## Plugin Not Detected

Check the plugin root:

```bash
python -m nexus_n3.plugins show-root
```

Confirm the bundle was installed successfully and that the startup log shows the
expected plugin inventory.

For local dev-list installs:

```bash
python -m nexus_n3.plugins show-dev-list --json
```

## BLE Gateway Not Available

Check:

- `BLE_BACKEND`
- `GATEWAY_SERIAL_PORT`
- device presence at the configured serial path

Examples:

- Linux: `/dev/serial/by-id/...`
- Windows: `COM3`

The admin/device-info surfaces should also report gateway readiness.

On Windows development machines, prefer `nexus_ble_gateway` over host BLE when
the gateway hardware is available.

## Wi-Fi Sensor Is Not Discovered

Check that `NEXUS_SENSOR_NETWORK_ENABLED=1`, the configured Wi-Fi, bridge, and
VLAN interfaces exist, and that their saved profiles are active. The AP and
VLAN profiles must be bridge ports of `NEXUS_SENSOR_BRIDGE_INTERFACE`; the
bridge must own `NEXUS_SENSOR_EXPECTED_CIDR`. The AP profile owns no IPv4
address.

An already provisioned X-IMU3 should announce on the Nexus sensor subnet. A
reset X-IMU3 instead exposes its open provisioning AP and must pass through the
provisioning lifecycle. `WifiCandidateNotFound` means no unclaimed AP matched
the requested sensor plugin during the fresh scan; it does not mean that a
sensor already connected to the Nexus AP failed its UDP ping.

Only one process should use the vendor network-announcement socket. Stop stale
live-test or Core processes before retrying if the vendor package reports
`RuntimeError: Address in use`.

## Wi-Fi AP Restoration or Authentication Failure

Direct recovery after provisioning requires the fixed-purpose service and
sudoers rule:

```bash
sudo deployment/systemd/install_wifi_recovery.sh "$USER" EE
```

Replace `EE` with the deployment's regulatory domain. Confirm that the service
exists and that the runtime identity received the generated sudoers rule. Core
runs only this non-interactive command:

```text
sudo -n /usr/bin/systemctl restart nexus-n3-wifi-recovery.service
```

A desktop authorization dialog is not a supported runtime path. It normally
indicates stale code, an incomplete recovery installation, or a runtime user
that does not match the installed sudoers rule. Restart Core after correcting
the installation. Inspect the `WIFI.backend` and `WIFI.adapter.errors` entries
in the session diagnostics for the failed recovery stage.

## Gateway Checksum or Parser Resynchronisation Reported

Inspect `<node-id>-diagnostics/session_diagnostics.json` or the
`sensor_diagnostics` records in
`<node-id>-diagnostics/session_diagnostics.jsonl` inside the session archive.
Use `master-diagnostics` for the master and `standalone-diagnostics` for a
standalone runtime.

The host parser counters are reset at the beginning of every recording session,
including when a new session is started without reinitializing core. A non-zero
`stream_checksum_failures` value therefore belongs to the current recording in
core version 0.1.12 or later.

One checksum failure normally also produces resynchronisation activity: the
parser drops data until it finds the next valid JSON or binary-frame boundary.
Consequently, one checksum failure plus one or more resynchronisation events may
describe a single corruption incident. `stream_resync_drop_bytes` shows how many
bytes were discarded.

Partial JSON/frame waits are not themselves checksum errors. They can occur when
a valid serial message arrives across multiple reads.

If failures recur, compare their event timestamps with gateway transport drops,
BLE receive counters, GATT timeout logs, and sensor sample-rate changes. Do not
assume a parser checksum failure identifies a particular sensor plugin.

## No Session Output

Verify:

- the configured output path exists and is writable
- USB storage is mounted if the deployment expects removable storage
- the runtime reached official stream start

## Deployment Mismatch

Use the deployment guide that matches the real deployment shape:

- `deployment/guides/MANUAL_DEPLOYMENT.md`
- `deployment/guides/SYSTEMD_DEPLOYMENT.md`
- `deployment/guides/ANSIBLE_DEPLOYMENT.md`
- `deployment/guides/DOCKER_DEPLOYMENT.md`
