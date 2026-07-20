# nexus_n3.gateway

## Overview
`nexus_n3.gateway` handles external communication. A `GatewayInterface`
implementation receives client commands and publishes system events. The
`Server` wires the gateway to the `MessageHandler` and the `SystemEventBus`.
Gateway metadata is also surfaced to the admin and device-info layers.

Linux-only removable USB disk controls are wired into the server only when the
host supports the hot-disk workflow. On Windows and other non-Linux hosts, the
server still runs normally but stays on local output storage.

## Key Classes and APIs
- `Server(gateway, usb_disk_manager=None)`
  - `start()` -> starts gateway and emits `CMD_SYSTEM_SETUP`
  - `stop()`
- `MessageHandler(site, system_event_bus)`
  - `handle(msg)` -> dispatches to local core or master dispatcher
- `SystemEventBus`
  - `subscribe(cb)` / `emit(event)`
- `GatewayInterface` (contract)
  - `start(on_message)` / `publish_event(event)` / `publish_command(cmd)` / `stop()`
- Gateways
  - `ZeroMQGateway` (local PUB/SUB transport)

## Runtime Environment
- `config/runtime.env` in local development
- `/etc/nexus-n3/runtime.env` in deployed systems
- gateway-specific variables in that file:
  - `ZEROMQ_CMD_BIND` / `ZEROMQ_EVENT_BIND`

## Message Flow
- Client -> gateway -> `MessageHandler.handle()`
- `MessageHandler` -> `Core` (standalone/worker) OR `MasterNode` dispatcher
- `SystemEventBus.emit()` -> gateway publishes events to clients
- `EVT_SERVER_READY` includes supported sensors/algorithms/gateways/bridges when ready
- `EVT_DEVICE_INFO` exposes a richer device snapshot for control-center surfaces
- `CMD_SYSTEM_SETUP` creates the core runtime and emits plugin startup inventory logging

## Key Files
- `nexus_n3.gateway/server.py`
- `nexus_n3.gateway/messaging/message_handler.py`
- `nexus_n3.gateway/messaging/message_types.py`
- `nexus_n3.gateway/event_bus/system_event_bus.py`
- `nexus_n3.gateway/gateways/zeromq/zeromq_gateway.py`
