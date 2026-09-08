# nexus_n3_tests

## Overview
`nexus_n3_tests` contains test scaffolding for core, distributed, sensor
manager, and data offload components.

## Areas Covered
- `nexus_n3_tests/core` -> core interface and subject workflows
- `nexus_n3_tests/distributed` -> master/worker discovery and routing
- `nexus_n3_tests/plugins` -> installer, catalog discovery, algorithm host, sensor host, and routing
- `nexus_n3_tests/sensor_manager` -> adapter paths
- `nexus_n3_tests/usb_camera` -> USB camera discovery/streaming
- `nexus_n3_tests/data_offload` -> USB and sink behaviors
- `nexus_n3_tests/azure_bridge` -> bridge behavior and upload paths

## Distributed Hardware Session

`core/test_4e_two_subject_distributed_session_30s.py` exercises one master and
one worker with two Movella DOT sensors per subject. It validates capability-
based assignment, sequential per-subject discovery and connection, unique
physical sensor ownership, compute output on both nodes, and the distributed
drain barrier before disconnecting.

Run it against a local master after exactly one worker has registered:

```bash
python3 nexus_n3_tests/core/test_4e_two_subject_distributed_session_30s.py
```

## Key Files
- `nexus_n3_tests/*`
