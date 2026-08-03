# Client Message Flow

This guide describes the command/event flow and documents all message types
defined in `message_types.py`. It is intended for application teams building
a control client.

## Common Fields
- `type` (string): Message type constant from `nexus_n3.gateway.messaging.message_types`.
- `payload` (object): Optional data for the command or event.

## Readiness and Initialization
1) Client -> `CMD_IS_SERVER_READY`
   - Payload: none

2) Server -> `EVT_SERVER_READY`
   - Payload:
     - When ready: `{ "msg": "System Server Ready", "supported_sensors": [{ "name": "<sensor>", "locations": [...], "computations": [{ "name": "<algorithm>", "inputs": { ... } }] }], "supported_algorithms": [...], "supported_gateways": [...], "supported_bridges": [...] }`
     - When not ready: `{ "msg": "System Server NOT Ready" }`

3) Client -> `CMD_GET_DEVICE_INFO`
   - Payload: none

4) Server -> `EVT_DEVICE_INFO`
   - Payload:
     - `device`: control-center oriented runtime identity fields such as role, active bridge, software version, and optional IoT Hub device id
     - `admin_summary`: server status, uptime, remote-control flag, and USB mount state
     - `capabilities`: supported sensors, algorithms, gateways, and bridges
     - `plugin_inventory`: first-pass installed sensor/algorithm inventory plus summary counts

Clients should use `EVT_SERVER_READY` and/or `EVT_DEVICE_INFO` as the source of
truth for selectable sensors and algorithms before sending `CMD_INIT_SYSTEM`.
Do not build a new session from a cached or hard-coded plugin list when the
runtime plugin set may have changed.

5) Client -> `CMD_INIT_SYSTEM`
   - Payload:
     - `subjects` (required): list of subject configs
     - `init_label` (optional): session name (client should include any desired
       "who_" prefix)
   - The canonical session ID is `<init_label>_<session_timestamp>`.
   - If `init_label` is missing, the session name is `sys_session`.
   - `CMD_INIT_SYSTEM` validates the requested sensors and algorithms against
     the currently installed plugin catalog. It does not discover new plugin
     options for the client.

Example:
```json
{
  "type": "init_system",
  "payload": {
    "init_label": "Anna_bdc",
    "subjects": [
      {
        "subject_id": "subject1",
        "sensors": [
          {
            "local_name": "Movella DOT",
            "number_of": 2,
            "compute_algorithm": {
              "name": "standard_loading_intensity",
              "inputs": { "gravity": 9.80665 }
            },
            "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"]
          }
        ]
      }
    ]
  }
}
```

6) Server -> `EVT_SYSTEM_INITIALIZED`
   - Payload: status string (e.g., `"System initialised with 1 subject(s)"`)

## Discovery and Connection
7) Client -> `CMD_DISCOVER_SENSORS`
   - Payload: none

8) Server -> `EVT_SENSORS_DISCOVERED`
   - Payload: list of subjects with discovered sensors.

9) Client -> `CMD_DISCOVER_SENSORS_FOR_SUBJECTS`
   - Payload:
     - `subject_ids` (required): list of subject IDs

10) Server -> `EVT_SENSORS_DISCOVERED_FOR_SUBJECT`
   - Payload: discovery info for the specific subject.

11) Client -> `CMD_CONNECT_TO_ALL` or `CMD_CONNECT_SUBJECTS`
   - `CMD_CONNECT_SUBJECTS` payload:
     - `subject_ids` (required): list of subject IDs

12) Server -> `EVT_SENSOR_CONNECTED`

13) (Optional) Client -> `CMD_IDENTIFY_SENSOR`
   - Payload:
     - `subject_id` (required)
     - `location` (required)

## Start Streaming (Tagging)
14) Client -> `CMD_START_STREAM_FOR_ALL`
   - Payload (optional):
     - `tag`: apply to all subjects
     - `tags`: map of `subject_id -> tag` (per-subject override)

If tags are missing:
- Tag defaults to `sys`
- Tag directory becomes `sys_<ts>`

Example:
```json
{
  "type": "start_stream_for_all",
  "payload": { "tag": "run" }
}
```

Per-subject tags:
```json
{
  "type": "start_stream_for_all",
  "payload": {
    "tags": {
      "subject1": "run",
      "subject2": "walk"
    }
  }
}
```

15) Client -> `CMD_START_STREAM_FOR_SUBJECTS`
   - Payload:
     - `subject_ids` (required): list of subject IDs
     - `tag` (optional)

16) Server -> `EVT_STREAM_STARTED`

### File Output Structure
Canonical session directory:
```
nexus_n3_outputs/<site>/sessions/<session_name>_<session_ts>/
```

Per subject:
```
subjects/<subject_id>/activities/<tag>/raw/<location>_<sensor_id>.csv
subjects/<subject_id>/activities/<tag>/computed/real_time/<algorithm>/<location>_<sensor_id>.ndjson
subjects/<subject_id>/activities/<tag>/computed/intermediate/<algorithm>.ndjson
subjects/<subject_id>/activities/<tag>/computed/consolidated/<algorithm>.ndjson
```

Notes:
- The session timestamp appears once, in the canonical session ID.
- All dynamic path components are filesystem-safe.
- Sensor-address separators are removed in `sensor_id`.
- Real-time results are per subject + algorithm + sensor address/location.
- Intermediate and consolidated results are per subject + algorithm.
- Directory context identifies the activity, algorithm, and compute stage.

## Stop Streaming
15) Client -> `CMD_STOP_STREAM_FOR_ALL`
   - Payload: none

or

15) Client -> `CMD_STOP_STREAM_FOR_SUBJECTS`
   - Payload: `subject_ids` (required): list of subject IDs

16) Server -> `EVT_STREAM_STOPPED`

## Additional Commands
- `CMD_SYSTEM_SETUP`
  - Payload: `{ "file_path": <path or null>, "network_path": <path or null> }`
  - Typically issued by the server on startup.
- `CMD_UPDATE_FILE_PATH`
  - Payload: `{ "file_path": <path or null> }`
  - Broadcast when USB paths change.
- `CMD_DISCONNECT_ALL`
  - Payload: none
- `CMD_DISCONNECT_SUBJECTS`
  - Payload: `subject_ids` (required): list of subject IDs
- `CMD_CHECK_BATTERY`
  - Payload: `{ "scan_timeout": <seconds>, "read_timeout": <seconds> }`
  - Discovers BLE sensors with battery notify capability, reads battery, disconnects.

## Event Reference (from `message_types.py`)
- `EVT_SERVER_READY`
- `EVT_SYSTEM_INITIALIZED`
- `EVT_SENSORS_DISCOVERED`
- `EVT_SENSORS_DISCOVERED_FOR_SUBJECT`
- `EVT_SENSOR_CONNECTED`
- `EVT_SENSOR_DISCONNECTED`
- `EVT_STREAM_STARTED`
- `EVT_STREAM_STOPPED`
- `EVT_SENSOR_IDENTIFIED`
- `EVT_SENSOR_MANGER_INITIALISED`
- `EVT_BATTERY_UPDATE`
- `EVT_BATTERY_CHECK`
- `EVT_COMPUTE_RESULT`
- `EVT_INTERMEDIATE_RESULT`
- `EVT_CONSOLIDATED_RESULT`
- `EVT_ERROR`
- `EVT_USB_DISK_INSERTED`
- `EVT_USB_DISK_REMOVED`

## Errors
Any command can emit `EVT_ERROR` with a payload string or dict describing the
failure.

## Battery Events
`EVT_BATTERY_UPDATE` payload:
```json
{
  "address": "AA:BB:CC:DD:EE:FF",
  "battery_level": 57,
  "is_charging": false
}
```

`EVT_BATTERY_CHECK` payload:
```json
{
  "timestamp": "2026-02-06T12:34:56.789012",
  "results": [
    {
      "address": "AA:BB:CC:DD:EE:FF",
      "name": "Movella DOT",
      "status": "ok",
      "battery_level": 57,
      "is_charging": false
    }
  ]
}
```

## Intermediate Result Payload
Intermediate executor results are normalized as:
```json
{
  "algorithm_name": "standard_loading_intensity",
  "stage": "intermediate_time",
  "results": [
    { "address": "AA:BB:CC", "data": { "...": "..." } }
  ]
}
```
The `data` object is algorithm-specific, but the `results` list is consistent
across executors. Some algorithms may add comparison entries to `results`
using `kind: "comparison"` and a `pair` field.

If an executor supports comparisons, it may also return comparison entries:
```json
{
  "results": [
    {
      "subject_id": "subject1",
      "kind": "comparison",
      "pair": ["LEFT_ANKLE", "RIGHT_ANKLE"],
      "data": { "0-3": { "x": 105.0, "y": 98.2, "z": 101.2, "mag": 100.4 } }
    }
  ]
}
```

## Consolidated Result Payload
Consolidation executor results are normalized as:
```json
{
  "subject_id": "subject1",
  "algorithm_name": "standard_loading_intensity",
  "stage": "consolidated_time",
  "results": [
    {
      "kind": "sensor_summary",
      "address": "AA:BB:CC",
      "window_count": 24,
      "data": {
        "0-3": {
          "x": { "count": 24, "mean": 0.12, "min": 0.01, "max": 0.31, "p50": 0.11, "p95": 0.29 }
        }
      }
    }
  ]
}
```
