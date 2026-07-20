# nexus_n3.core

## Overview
`nexus_n3.core` provides the high-level orchestration layer. The `Core` class
initializes subjects and sensors, drives the sensor manager lifecycle, feeds
samples into the compute manager, and coordinates file output.

It also exposes runtime capability discovery from installed plugins through:

- `get_supported_sensors()`
- `get_supported_algorithms()`

## Key Classes and APIs
- `Core(site, system_event_bus=None)`
  - `init_core(subjects_config, init_label=None)`
  - `discover_sensors()` / `discover_sensors_for_subjects(subject_ids)`
  - `connect_all()` / `connect_subjects(subject_ids)` / `disconnect_*`
  - `identify_sensor(subject_id, location)`
  - `set_file_path(path)`
  - `start_stream(payload)` / `start_stream_for_subjects(payload)` / `stop_*`
    - `start_stream` supports `tag` (all subjects) or `tags` (per-subject map)
    - `start_stream_for_subjects` supports `tag`
  - `check_battery(scan_timeout=5.0, read_timeout=10.0)`
  - `get_supported_sensors()` / `get_supported_gateways()`
- `Subject(subject_id, sensor_configs)`
  - `add_sensor(sensor, meta_data)`
  - `ingest_sample(sample, file_manager)`
  - `ingest_result(result, file_manager)`
  - `ingest_intermediate_result(result, file_manager)`

### Orchestrators
`Core` delegates to coordination services under `nexus_n3.core/orchestrators`:

- `SubjectGraph`
- `SensorOrchestrator`
- `ComputeOrchestrator`
- `StorageOrchestrator`
- `EventAssembler`

Notes:
- `Core` interacts with `SensorManager` through `SensorOrchestrator` methods,
  including listener overrides used by battery check.
- `ComputeOrchestrator` owns algorithm registration policy and delegates compute
  runtime behavior to `ComputeManager`.

## Message Flow (Core-centric)
- `CMD_SYSTEM_SETUP` -> `Core(...)` created and file path set
- `CMD_INIT_SYSTEM` -> subjects are initialized and installed sensor plugin classes are instantiated
- `CMD_DISCOVER_*` -> sensor manager scans and assigns addresses
- `CMD_CONNECT_*` -> sensors connect; notifications configured
- `CMD_CHECK_BATTERY` -> standalone pre-init BLE battery scan, connect, read, disconnect
- `CMD_START_STREAM_*` -> file manager creates session paths; streaming begins
- Sensor data -> `Subject.ingest_sample()` -> `FileManager.write_block()`
- Compute results -> `Subject.ingest_result()` -> NDJSON output
- Intermediate results -> aggregated per subject+algorithm and written to NDJSON
- Stop stream -> pending samples drain -> consolidation runs per subject+algorithm -> consolidated NDJSON + `EVT_CONSOLIDATED_RESULT`

## Key Files
- `nexus_n3.core/core.py`
- `nexus_n3.core/subject.py`
