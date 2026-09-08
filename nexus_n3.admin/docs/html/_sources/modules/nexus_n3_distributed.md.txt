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
- `WorkerNode(node_id, site, customer_id=None, site_id=None, site_name=None, registry=None)`
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
- Stop commands carry a shared `stop_session_id`; each node reports its real
  `stream_drained` event after local file and diagnostics queues are closed
- Master archives only after every expected execution node has drained

## Subject Assignment
- Master and workers are both execution-capable assignment candidates
- Nodes must advertise every sensor and algorithm capability required by a subject
- Subjects are placed on the eligible node with the lowest assigned sensor count
- Equal loads prefer the master, followed by a stable node-ID tie break
- With one master, one worker, and two equivalent subjects, assignment normally
  places one subject on each node

## Identity And Storage

- Master and workers must share customer ID, site, site ID, and site name
- Workers receive these values through their service command and require a
  unique `--node-id`
- The master sends its worker-facing shared output path during registration and
  broadcasts later path changes
- Diagnostics are node-local within the shared session:
  `master-diagnostics/`, `<worker-node-id>-diagnostics/`, or
  `standalone-diagnostics/`

## Drain Barrier

- Every node closes its own raw, compute, and diagnostics writers before its
  successful drain acknowledgement
- Raw-write errors produce an error/partial shared archive after all nodes drain
- `drained=false` prevents archiving when a node failed before it was safe
- Finalization is bound to the acknowledged session timestamp and can be retried
  if the archive callback fails

## Node Liveness
- Master updates its own `last_seen` so it stays `active` in the admin UI
- Workers auto-reconnect by re-discovering the master when the ZMQ connection goes quiet

## Key Files
- `nexus_n3.distributed/master_node.py`
- `nexus_n3.distributed/worker_node.py`
- `nexus_n3.distributed/ai_compute_node.py`
- `nexus_n3.distributed/registry.py`
