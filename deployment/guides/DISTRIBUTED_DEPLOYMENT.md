# Distributed Deployment

## Runtime Roles

The distributed runtime has three node roles:

- `master`: accepts client commands, participates in edge execution, assigns
  subjects, coordinates workers, and owns shared-session finalization
- `worker`: runs edge sensor orchestration and compute for assigned subjects
- `ai`: provides optional remote compute and does not manage sensors or files

Master and worker nodes must have the sensor and algorithm plugins required by
the subjects assigned to them. AI nodes require algorithm plugins only. Plugins
are installed and executed locally; the master does not serve plugin code to
other nodes at runtime.

## Deployment Identity

Every master and worker in one distributed deployment must use the same:

- customer ID
- site
- site ID
- site name

Each worker must also have a unique node ID. `WorkerNode` receives this identity
from the server command line; it does not independently load site values.
Ansible renders these values from inventory variables into the worker service:

```yaml
nexus_customer_id: <customer-id>
nexus_site: <site>
nexus_site_id: <site-id>
nexus_site_name: "<site name>"
nexus_node_id: "{{ inventory_hostname }}"
```

The rendered worker command has this shape:

```text
/opt/nexus-n3-core/venv/bin/nexus-n3-core \
  --gateway zeromq_gateway \
  --site <site> \
  --customer-id <customer-id> \
  --site-id <site-id> \
  --site-name "<site name>" \
  --role worker \
  --node-id <inventory-hostname> \
  --ble-backend <backend>
```

The master may use its normal runtime defaults when those defaults contain the
same deployment identity:

```bash
python3 nexus_n3_server.py --role master --admin
```

## Discovery And Subject Assignment

Workers discover the master through `_nexusn3._tcp.local.` mDNS, connect to its
ZeroMQ ROUTER endpoint, and register their capabilities. The master itself is
also registered as an execution-capable node.

For each subject, the master:

1. determines the required sensor and algorithm plugins
2. filters out nodes without those capabilities or sufficient sensor capacity
3. chooses the eligible node with the lowest assigned sensor count
4. prefers the master when equally loaded, then uses node ID for a stable tie
   break

With one master, one worker, and two equivalent two-sensor subjects, this
normally assigns the first subject to the master and the second to the worker.
Discovery and connection can then be issued per subject so each node claims
only the sensors for its assigned subject.

## Shared Session Storage

The master provides its worker-facing network path in the registration response
and broadcasts later path changes with `CMD_UPDATE_FILE_PATH`. Workers mount or
access that shared location and write their assigned subject directories into
the same session tree. AI nodes do not write session files.

The master and workers must use the same site and session timestamp; otherwise
they will write different session trees. The shared storage permissions must
allow the worker service user to create and update files.

Diagnostics are isolated by runtime node inside the shared session:

```text
<session>/master-diagnostics/
<session>/<worker-node-id>-diagnostics/
```

A standalone runtime uses:

```text
<session>/standalone-diagnostics/
```

Each directory contains `session_diagnostics.json`,
`session_diagnostics.jsonl`, and, when `--diagnostics` is enabled,
`pipeline_debug.ndjson`.

## Distributed Stop And Finalization

A distributed stop carries one `stop_session_id` to every participating node.
Each node stops its assigned subjects, drains raw and compute writes, finalizes
its node-local diagnostics, and emits `stream_drained`. The master archives the
shared session only after every expected node has acknowledged that stop.

An acknowledged raw-write failure still completes the barrier and produces an
error/partial archive. A node that failed before actually draining reports
`drained=false`; the master will not archive an actively written session.
Archive callback failures remain retryable, and the acknowledged session
timestamp is checked before finalization so a late acknowledgement cannot
archive a newer session.

## Deploying A Worker

Build the release wheel first, then deploy only the desired worker:

```bash
cd deployment/ansible
ansible-playbook -i inventory.ini playbooks/deploy_workers.yml \
  -e nexus_deploy_hosts=<worker-node-id>
```

To reinstall only Core while retaining already installed plugins:

```bash
ansible-playbook -i inventory.ini playbooks/deploy_workers.yml \
  -e nexus_deploy_hosts=<worker-node-id> \
  -e nexus_install_sensor_plugins=false \
  -e nexus_install_algorithm_plugins=false
```

Other deployment entry points are:

- `playbooks/deploy_distributed.yml`: master, workers, and AI nodes
- `playbooks/deploy_master.yml`: master nodes only
- `playbooks/deploy_ai_nodes.yml`: AI nodes only
- `playbooks/deploy_plugin_bundles.yml`: plugin bundles only

## Two-Subject Hardware Test

Start the master, ensure exactly one worker is registered, and turn on four
Movella DOT sensors. Both execution nodes must advertise the Movella DOT sensor
and Standard Loading Intensity algorithm capabilities.

Run:

```bash
python3 nexus_n3_tests/core/test_4e_two_subject_distributed_session_30s.py
```

The test initializes two subjects, discovers and connects each subject
sequentially on its assigned node, streams both for 30 seconds, waits for
`stream_drained` from the master and worker, then disconnects. It also verifies
unique physical sensor ownership and observes Standard Loading Intensity output
for both subjects.
