# nexus_n3.azure_bridge

## Overview
`nexus_n3.azure_bridge` provides the optional Azure IoT Hub integration layer
for Nexus N3 Core. It connects the local runtime to IoT Hub for:

- direct method handling
- reported-property updates
- session archive upload after stream finalization
- remote-control enable/disable state

The bridge can run standalone or in-process via
`nexus_n3_server.py --bridge azure_bridge` or
`nexus-n3-core --bridge azure_bridge`.

## Key Classes and APIs
- `AzureBridgeService`
  - `start()` -> connect to IoT Hub, subscribe to local events, process pending uploads
  - `stop()`
  - `status()` -> bridge runtime status for admin and device-info surfaces
  - `set_remote_control_enabled(enabled)` -> toggle control-mode at runtime
- `AzureBridgeConfig`
  - loads bridge env/config including device identity, IoT Hub settings, and upload retry interval
- `AzureDeviceClient`
  - wraps IoT Hub device client operations
  - direct method responses
  - reported properties
  - file-upload storage negotiation
- `IoTHubFileUploader`
  - uploads local session archives through the IoT Hub file-upload flow
- `BridgeStateStore`
  - persists bridge runtime state such as control mode

## Message Flow
- Local runtime emits `EVT_STREAM_DRAINED` after stop/finalize.
- `AzureBridgeService` inspects the payload.
- When the payload is successful and all local streams are stopped, the bridge:
  - reads archive metadata from the event payload
  - queues the session archive for upload
  - retries upload on failure according to bridge config
- On success, the bridge:
  - records last-upload status
  - updates reported properties
  - requests USB safe-unmount when appropriate

For cloud-to-device control:
- IoT Hub direct method -> `AzureBridgeService`
- If remote control is enabled and the method is allowed, the bridge forwards the command to the local runtime
- Read-only methods remain available when remote control is disabled

## Runtime Notes
- Integrated bridge mode is intended for `standalone` and `master`.
- The bridge listens for `EVT_STREAM_DRAINED`, so archive upload depends on the local stop/finalize flow completing successfully.
- Remote control can be enabled at startup with `--azure-bridge-remote-control` or toggled at runtime from the admin UI.
- Session archives are uploaded only after the runtime has finalized and archived the local session output.

## Key Files
- `nexus_n3.azure_bridge/bridge.py`
- `nexus_n3.azure_bridge/config.py`
- `nexus_n3.azure_bridge/azure_device_client.py`
- `nexus_n3.azure_bridge/file_upload.py`
- `nexus_n3.azure_bridge/state_store.py`
