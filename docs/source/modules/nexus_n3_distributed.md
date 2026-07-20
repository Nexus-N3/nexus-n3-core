# nexus_n3.distributed

## Overview
`nexus_n3.distributed` provides master/worker coordination. The master routes
commands and aggregates events; workers execute commands locally and forward
results back.

## Key Classes and APIs
- `MasterNode(registry, usb_disk_manager, router_port=6000, mdns_hostname=None)`
  - `start()` / `stop()`
  - `dispatch_command(msg, message_handler=None)`
  - `assign_subjects(subjects)`
- `WorkerNode(node_id, site, registry=None)`
  - `start()` / `stop()`
  - `send_event(event)`
- `AiComputeNode(node_id, compute_port=7001, capabilities=None)`
  - `start()` / `stop()`
- `NodeRegistry`
  - `register_node()` / `assign_subject()` / `get_subjects()`
  - `get_ai_nodes()` / `set_ai_nodes()`

## Message Flow
- Worker discovers master via mDNS and registers over ZMQ
- Master assigns subjects to nodes and routes `CMD_*` messages
- Workers execute locally via `MessageHandler` and emit events back
- Workers send lightweight heartbeats when idle to keep `last_seen` updated
- USB path changes -> master broadcasts `CMD_UPDATE_FILE_PATH` to workers
- AI compute nodes register with the master and expose a direct compute endpoint
- Master broadcasts AI registry snapshots to workers over the internal control channel

## Subject Assignment
- If **no workers** are registered, all subjects are assigned to the **master**
- If **one or more workers** are registered, subjects are assigned **round-robin to workers only**
- Result: in a master+worker setup, subjects typically land on workers, not the master

## Node Liveness
- Master updates its own `last_seen` so it stays `active` in the admin UI
- Workers auto-reconnect by re-discovering the master when the ZMQ connection goes quiet

## Key Files
- `nexus_n3.distributed/master_node.py`
- `nexus_n3.distributed/worker_node.py`
- `nexus_n3.distributed/ai_compute_node.py`
- `nexus_n3.distributed/registry.py`
