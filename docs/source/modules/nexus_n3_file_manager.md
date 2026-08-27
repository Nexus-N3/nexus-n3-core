# nexus_n3.file_manager

## Overview
`nexus_n3.file_manager` manages session directories and writes raw and computed
sensor data to disk. It also finalizes a session into a local zip archive after
stream stop.

This is the generic write path for the runtime. Sensors and algorithms do not
implement their own storage layout in the core; they emit samples/results and
the file manager persists them through the common session structure.

## Key Classes and APIs
- `FileManager(site, base_dir="nexus_n3_outputs")`
  - `set_base_path(path)`
  - `set_session_label(label)`
  - `start_stream(subject, session_index)`
  - `stop_stream(subject)`
  - `enqueue_block(entry, samples)`
  - `flush()`
  - `write_block(entry, samples)`
  - `write_computed_json(entry, result)`
  - `write_intermediate_json(subject_id, algorithm_name, result)`
  - `read_intermediate_json(subject_id, algorithm_name=None)`
  - `write_consolidated_json(subject_id, algorithm_name, result)`
  - `start_session_diagnostics(session_index, metadata=None)`
  - `append_session_diagnostics_event(session_index, event_type, payload=None)`
  - `enqueue_session_diagnostics_event(session_index, event_type, payload=None)`
  - `finalize_session_diagnostics(session_index, status, reason=None, summary_updates=None)`
  - `archive_session(session_index)`
- `session_archive`
  - `build_session_archive_name(...)`
  - `archive_session_directory(...)`

## Message Flow
- `Core.start_stream_*()` -> `FileManager.start_stream()` creates paths
- `Subject.ingest_sample()` -> `enqueue_block()` for raw CSV blocks
- FileManager background writer thread -> `write_block()` to CSV
- `Subject.ingest_result()` -> `write_computed_json()` to real-time NDJSON (per sensor)
- `Subject.ingest_intermediate_result()` -> `write_intermediate_json()` (per subject+algorithm)
- Stop-stream consolidation -> `write_consolidated_json()` (per subject+algorithm)
- Runtime events -> `session_diagnostics.jsonl`; current/final state ->
  `session_diagnostics.json`
- Stop-stream finalization -> `flush()` -> `archive_session()` -> zip archive under the session base directory

## Session Finalization
- Active outputs are written under:
  - `nexus_n3_outputs/<site>/sessions/<session_name>_<session_timestamp>/...`
- On stop/finalize, the session directory is zipped locally and the source
  directory is removed.
- Archive names follow:
  - `<session_name>_<session_timestamp>.zip`
  - Site context remains in the parent directory and Azure blob prefix.

## Generic Output Mechanisms

- raw samples -> CSV block writes
- real-time compute results -> NDJSON
- intermediate results -> NDJSON
- consolidated results -> NDJSON
- diagnostics events -> NDJSON

## Session Diagnostics

Structured diagnostics are created for every recording under the session's
`diagnostics/` directory:

- `session_diagnostics.json`: merged summary containing lifecycle state,
  `official_stream` (`pending`, then `passed` or `failed`), the latest gateway
  diagnostics, errors, stop state, and drain state
- `session_diagnostics.jsonl`: ordered diagnostic events, including one
  `compute_performance` record per compute result

Compute-performance writes use a background queue so the compute callback does
not wait for disk I/O. Finalization drains that queue and seals the diagnostics
state before the session directory is archived. Late events cannot recreate the
already archived session directory.

## Key Files
- `nexus_n3.file_manager/FileManager.py`
- `nexus_n3.file_manager/session_archive.py`
