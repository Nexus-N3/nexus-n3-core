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
  - `register_performance_listener(callback)`
  - `ingest_sample(sample, timing_metadata=None)`
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
- Result -> core-owned timing record -> performance listener
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
- Core emits one `compute_performance` event per compute result and appends the
  same payload to `diagnostics/session_diagnostics.jsonl`.
- The algorithm host wraps `execute_real_time()` at runtime when that method is
  available. Those records use `execution_measurement=wrapped_execute_real_time`
  and are exact for that method boundary. Plugins without that conventional
  method report `result_callback_ms` with
  `execution_measurement_exact=false`; callback duration is not presented as
  algorithm execution time.
- Result cadence is measured at `ComputeManager` receipt using the host
  monotonic clock. The first result has no interval; each later result reports
  its interval from the preceding result for the same algorithm and address.
- Trigger-sample metadata preserves available source, BLE gateway, host-receive,
  and normalized session timestamps without modifying the plugin result schema.
- `result_interval_ms` is the monotonic interval between consecutive results for
  the same algorithm and sensor address. It is absent for the first result.
  When `window_seconds` is available, `expected_result_interval_ms` and
  `cadence_drift_ms` make the expected runtime cadence directly checkable.
- `trigger_sample_to_result_ms` starts at host receipt of the sample that caused
  the result and therefore measures trigger-sample latency, not the time to
  collect the complete algorithm window. `queue_wait_ms` and
  `compute_enqueue_to_result_ms` isolate the core queue and dispatch portions.
- `samples_since_previous_result` reports how many samples core dispatched for
  the sensor since its preceding result. Timing instrumentation is owned by core
  and the core algorithm host; released plugins do not need to emit these fields.

## Key Files
- `nexus_n3.compute_manager/compute_manager.py`
- `nexus_n3.compute_manager/intermediate_stage.py`
- `nexus_n3.compute_manager/consolidation_stage.py`
- `nexus_n3.compute_manager/result_router.py`
- `nexus_n3.compute_manager/remote_compute_service.py`
- `nexus_n3.compute_manager/remote_compute_client.py`
