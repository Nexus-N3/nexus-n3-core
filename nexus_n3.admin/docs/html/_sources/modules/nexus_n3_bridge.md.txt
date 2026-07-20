# nexus_n3.bridge

## Overview
`nexus_n3.bridge` contains bridge discovery and instantiation helpers for
optional remote integrations used by `nexus_n3_server.py`.

It is the thin routing layer between CLI/runtime bridge selection and the
concrete bridge implementations such as:

- `nexus_n3.azure_bridge`

## Key Classes and APIs
- `discover_bridges()`
  - returns bridge metadata keyed by bridge name
  - includes display name, scope, and runtime-control support
- `create_bridge(bridge_name, *, site, customer_id=None, site_id=None, site_name=None, remote_control_enabled=False)`
  - creates the configured bridge service instance
  - applies deployment identity values to bridge environment defaults where needed

## Runtime Notes
- `nexus_n3_server.py --bridge <name>` uses `create_bridge(...)` to start a remote bridge in-process.
- Bridge metadata is surfaced in server-ready/device-info payloads so clients and admin surfaces can show supported remote transports.
- `azure_bridge` is implemented in `nexus_n3.azure_bridge`.

## Key Files
- `nexus_n3.bridge/bridge_registry.py`
- `nexus_n3.bridge/local_gateway_client.py`
