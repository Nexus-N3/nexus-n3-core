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

## Gateway Checksum or Parser Resynchronisation Reported

Inspect `diagnostics/session_diagnostics.json` or the `sensor_diagnostics`
records in `diagnostics/session_diagnostics.jsonl` inside the session archive.

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
