# Distributed Deployment Notes

## Intended Plugin Model

In distributed mode, plugins are installed locally on the nodes that execute
them.

The master should not serve plugin code to other nodes at runtime.

Required role-based plugin presence:

- `master`: sensor plugins and algorithm plugins
- `worker`: sensor plugins and algorithm plugins
- `ai`: algorithm plugins only

## Why

Workers are execution-capable nodes. They can:

- manage assigned subjects
- connect to local sensors
- stream data
- write files to the shared disk path provided by the master
- execute required algorithms

AI nodes are different. They are compute-only and do not connect directly to
sensors.

## Current Code Reality

The current runtime now does two important things:

1. subject assignment checks master and worker node plugin capabilities before
   assigning a subject
2. AI nodes execute algorithms through the plugin runtime rather than the legacy
   built-in algorithm registry

This means distributed deployment and runtime execution are aligned to the
plugin model for master, worker, and AI roles.

## Shared Disk Behavior

- Workers do receive the master-provided network/shared output path during
  registration and use the normal local message handling path for file updates.
- AI nodes do not currently manage files or write to the shared disk.

## Recommended Operational Rule For Now

- keep plugin installation role-correct across the fleet
- use the standard bundle deployment paths so node capabilities reflect the
  actual installed plugins

## Standard Deployment Paths

The intended operational paths are now:

- full distributed deployment:
  `deployment/ansible/playbooks/deploy_distributed.yml`
- master-only deployment:
  `deployment/ansible/playbooks/deploy_master.yml`
- worker-only deployment:
  `deployment/ansible/playbooks/deploy_workers.yml`
- AI-only deployment:
  `deployment/ansible/playbooks/deploy_ai_nodes.yml`
- plugin-only deployment:
  `deployment/ansible/playbooks/deploy_plugin_bundles.yml`

This supports:

- bringing up a full distributed system
- adding a new worker to an existing system
- adding or refreshing AI nodes
- rolling out a new plugin bundle to selected nodes without redeploying the core
