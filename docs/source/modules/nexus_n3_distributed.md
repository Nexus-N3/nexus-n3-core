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
- Distributed start commands carry a shared `start_session_id`. Physical
  streaming and readiness checks remain node-local, but a ready node does not
  open official persistence until the coordinating client sends
  `start_official_stream`.
- The master forwards `start_official_stream` only after every execution node
  assigned to that start has emitted `stream_ready_for_official`.
- Standalone nodes do not use this barrier and transition directly from local
  readiness to `stream_official_started`.

## Official-Start Barrier

1. The master snapshots the execution nodes participating in a distributed
   start and injects `official_start_mode=coordinated` plus one shared
   `start_session_id` into their start commands.
2. Each node starts its physical sensors and runs its local startup gate.
3. A successful local gate enters `ready_for_official`; samples continue to be
   observed for the physical stream but are not persisted or computed.
4. After observing readiness from every required node, the coordinating client
   sends `start_official_stream` with the shared start ID.
5. The master validates the barrier and broadcasts the commit only to the
   snapshotted participants.
6. Each node activates persistence, establishes its node-local Timeline origin,
   and acknowledges with `stream_official_started`.

The commit is idempotent for its active start ID. Stale IDs and commits issued
before all participants are ready are rejected.

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
