# nexus_n3.admin

## Overview
`nexus_n3.admin` provides a local FastAPI dashboard for inspecting system
status, logs, and outputs. It also supports restarting the server and
switching between gateway implementations.

## Key Classes and APIs
- `AdminState(project_root, role, site, gateway_name, ...)`
  - `server_status()` / `node_status()` / `uptime_seconds()`
- `create_app(state)`
  - `/` -> system status dashboard
  - `/logs` -> list available logs
  - `/outputs` -> browse outputs directory
  - `/server/restart` -> restart server
  - `/server/switch-gateway` -> restart with selected gateway

## UI Notes
- Gateway options show the transport scope (`local` or `remote`)
- Actions allow switching gateway and restarting the server

## Key Files
- `nexus_n3.admin/app.py`
- `nexus_n3.admin/templates/index.html`
- `nexus_n3.admin/static/admin.css`
