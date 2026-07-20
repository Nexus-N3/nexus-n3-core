# nexus_n3.compute_manager

## Overview
`nexus_n3.compute_manager` runs real-time algorithms and optional intermediate
and consolidation executors.

The public API remains `ComputeManager`, but internal responsibilities are split
for maintainability:
- `IntermediateStage`: buffered results + intermediate executor scheduling
- `ConsolidationStage`: consolidation executor registry and end-of-stream execution
- `ResultRouter`: per-result fanout to listeners and intermediate stage
- `RemoteComputeService`: AI node selection and delegation lifecycle
- `RemoteComputeClient`: ZeroMQ/pickle transport

## Key Classes and APIs
- `ComputeManager(system_event_bus=None, error_cb=None)`
  - `register_algorithm(address, algorithm)`
  - `has_algorithm(address)`
  - `register_intermediate_executor(algorithm_name, executor)`
  - `register_consolidation_executor(algorithm_name, executor)`
  - `register_result_listener(callback)`
  - `register_intermediate_result_listener(callback)`
  - `ingest_sample(sample)`
  - `get_results(algorithm_name, address=None, limit=None)`
  - `run_consolidation_for_subject(subject_id, algorithm_name, intermediate_records)`
  - `set_registry(registry)`
  - `delegate_compute(algorithm, samples)`
  - `on_remote_result(result, request_id=None)` (remote callback path)
  - `reset()`

## Message Flow
- `Core._on_discover()` registers algorithms/executors
- Samples -> queue -> algorithm `on_sample()`
- Result -> `on_algorithm_result()` -> `ResultRouter`
- `ResultRouter` -> compute result listener + `IntermediateStage`
- `IntermediateStage` optionally emits aggregated intermediate results
- On stop, core/orchestrator calls `run_consolidation_for_subject(...)`
  to execute optional algorithm-level consolidation executors
- If AI nodes are available and algorithm config allows delegation:
  - Algorithm asks `ComputeManager` to delegate execution
  - `RemoteComputeService` selects an endpoint from node registry and starts/reuses a `RemoteComputeClient`
  - `RemoteComputeClient` sends `RUN_ALGO` over ZeroMQ DEALER transport
  - Remote result is normalized locally then routed through same result path

## Runtime Notes
- `reset()` clears algorithms, intermediate/consolidation executors, buffered results, and queued samples;
  it does not stop the background worker thread.
- Performance logging can be enabled with `NEXUS_PERF_LOG=1`.

## Key Files
- `nexus_n3.compute_manager/compute_manager.py`
- `nexus_n3.compute_manager/intermediate_stage.py`
- `nexus_n3.compute_manager/consolidation_stage.py`
- `nexus_n3.compute_manager/result_router.py`
- `nexus_n3.compute_manager/remote_compute_service.py`
- `nexus_n3.compute_manager/remote_compute_client.py`
