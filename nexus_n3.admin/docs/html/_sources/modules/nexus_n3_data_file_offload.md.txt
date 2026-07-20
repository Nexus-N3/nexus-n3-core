# nexus_n3.data_file_offload

## Overview
`nexus_n3.data_file_offload` handles external storage detection and file
handoff. Current implementation focuses on USB detection; other sinks are
placeholders.

The removable USB hot-disk workflow is Linux-host-only. On non-Linux hosts, the
manager stays on the local fallback path and does not attempt USB mount
detection or control.

## Key Classes and APIs
- `USBDiskManager(fallback_dir="nexus_n3_outputs", poll_interval=2)`
  - `local_path` -> local or USB output path
  - `network_path` -> SMB path for workers when USB present
  - `supports_hotdisk` -> whether Linux USB hot-disk behavior is enabled
  - `register_callback(event, callback)`
  - `stop()`

## Message Flow
- `USBDiskManager` detects mount availability and write access
- On insert/remove -> callbacks -> `Server` emits `CMD_UPDATE_FILE_PATH`
- Master forwards `network_path` to workers

## Key Files
- `nexus_n3.data_file_offload/sinks/usb.py`
- `nexus_n3.data_file_offload/scanner.py` (empty placeholder)
- `nexus_n3.data_file_offload/triggers.py` (empty placeholder)
- `nexus_n3.data_file_offload/sinks/ftp.py` (empty placeholder)
- `nexus_n3.data_file_offload/sinks/azure_blob.py` (empty placeholder)
