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
