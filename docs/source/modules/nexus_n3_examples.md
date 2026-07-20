# nexus_n3_examples

## Overview
`nexus_n3_examples` contains client scripts showing how to control the gateway
and react to system events.

## Key Scripts and APIs
- `example_client.py`
  - Full-system flow: `CMD_IS_SERVER_READY` -> (optional `CMD_CHECK_BATTERY`) -> init -> discover -> connect ->
    stream -> stop -> disconnect
- `example_client_subject.py`
  - Per-subject flow using `CMD_DISCOVER_SENSORS_FOR_SUBJECTS` and
    `CMD_CONNECT_SUBJECTS`

## Message Flow (Typical)
1) Client sends `CMD_IS_SERVER_READY`
2) Server emits `EVT_SERVER_READY`
3) Client sends `CMD_INIT_SYSTEM` with subjects
4) Server emits `EVT_SYSTEM_INITIALIZED`
5) Client sends `CMD_DISCOVER_*` and then `CMD_CONNECT_*`
6) Client starts/stops streaming and receives events

## Key Files
- `nexus_n3_examples/example_client.py`
- `nexus_n3_examples/example_client_subject.py`
