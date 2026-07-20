"""
Message types for Nexus N3 Core gateway communication.

This module defines all **commands** that clients can send to the system
and all **events** that the system emits back. These constants are used
by `Server`, `MessageHandler`, gateways, and clients to ensure consistent
message passing.
"""

# =======================
# Client-to-System Commands
# =======================

#: Command: Check if the system server is ready and return all supported sensors / gateways
CMD_IS_SERVER_READY = "is_server_ready"

#: Command: Setup the system (initialize core components)
CMD_SYSTEM_SETUP = "system_setup"

#: Command: Initialize the system with subjects and configuration (optional init_label)
CMD_INIT_SYSTEM = "init_system"

#: Command: Discover all sensors in the system
CMD_DISCOVER_SENSORS = "discover_sensors"

#: Command: Discover sensors for specific subjects
CMD_DISCOVER_SENSORS_FOR_SUBJECTS = "discover_sensors_for_subjects"

#: Command: Connect all sensors in the system
CMD_CONNECT_TO_ALL = "connect_all"

#: Command: Connect sensors for specific subjects
CMD_CONNECT_SUBJECTS = "connect_subjects"

#: Command: Disconnect all sensors
CMD_DISCONNECT_ALL = "disconnect_all"

#: Command: Disconnect sensors for specific subjects
CMD_DISCONNECT_SUBJECTS = "disconnect_subjects"

#: Command: Identify a sensor by subject ID and body location
CMD_IDENTIFY_SENSOR = "identify_sensor"

#: Command: Discover BLE battery-capable sensors, read battery, then disconnect
CMD_CHECK_BATTERY = "check_battery"

#: Command: Start streaming sensor data (optional tag or tags map)
CMD_START_STREAM_FOR_ALL = "start_stream_for_all"

#: Command: Start streaming for subjects (optional tag or tags map)
CMD_START_STREAM_FOR_SUBJECTS = "start_stream_for_subjects"

#: Command: Stop streaming sensor data
CMD_STOP_STREAM_FOR_ALL = "stop_stream_for_all"

#: Command: Stop streaming for subjects
CMD_STOP_STREAM_FOR_SUBJECTS = "stop_stream_for_subjects"

#: Command: Update file path (usb disk or local)
CMD_UPDATE_FILE_PATH = "update_file_path"

#: Command: Manually mount/remount the USB disk on standalone/master nodes
CMD_USB_MOUNT = "usb_mount"

#: Command: Safely unmount the USB disk on standalone/master nodes
CMD_USB_SAFE_UNMOUNT = "usb_safe_unmount"

#: Command: Request the current USB disk status
CMD_GET_USB_STATUS = "get_usb_status"

#: Command: Request a device snapshot for remote control-center surfaces
CMD_GET_DEVICE_INFO = "get_device_info"

#: Command: Forward a Control Center message to local NEIA consumers
CMD_FORWARD_CONTROL_CENTER_MESSAGE = "forward_control_center_message"

#: Command: robot motion
CMD_ROBOT_MOTION = "robot_motion"

#: Command: stop robot
CMD_ROBOT_STOP = "robot_stop"


# =======================
# System-to-Client Events
# =======================

#: Event: Server is ready
EVT_SERVER_READY = "server_ready"

#: Event: System has been initialized
EVT_SYSTEM_INITIALIZED = "system_initialized"

#: Event: All sensors have been discovered
EVT_SENSORS_DISCOVERED = "sensors_discovered"

#: Event: Sensors discovered for a specific subject
EVT_SENSORS_DISCOVERED_FOR_SUBJECT = "sensors_discovered_for_subject"

#: Event: A sensor has connected
EVT_SENSOR_CONNECTED = "sensor_connected"

#: Event: A sensor has disconnected
EVT_SENSOR_DISCONNECTED = "sensor_disconnected"

#: Event: Streaming of sensor data started
EVT_STREAM_STARTED = "stream_started"

#: Event: Streaming of sensor data stopped
EVT_STREAM_STOPPED = "stream_stopped"

#: Event: Local stop cleanup and file draining are complete
EVT_STREAM_DRAINED = "stream_drained"

#: Event: Stream warmup has started but is not yet official
EVT_STREAM_WARMUP_STARTED = "stream_warmup_started"

#: Event: Stream warmup status update
EVT_STREAM_WARMUP_STATUS = "stream_warmup_status"

#: Event: Startup gate failed this attempt and a retry will be attempted
EVT_STREAM_STARTUP_RETRY = "stream_startup_retry"

#: Event: Startup gate passed and official capture has begun
EVT_STREAM_OFFICIAL_STARTED = "stream_official_started"

#: Event: Startup gate failed and no further retries remain
EVT_STREAM_STARTUP_FAILED = "stream_startup_failed"

#: Event: A sensor has been identified
EVT_SENSOR_IDENTIFIED = "sensor_identified"

#: Event: Sensor manager has been initialized
EVT_SENSOR_MANGER_INITIALISED = "sensor_manager_initalised"

#: Event: Battery status update from a sensor
EVT_BATTERY_UPDATE = "battery_update"

#: Event: Battery check results for discovered sensors
EVT_BATTERY_CHECK = "battery_check"

#: Event: Transport and packet-level diagnostics snapshot/update
EVT_SENSOR_DIAGNOSTICS = "sensor_diagnostics"

#: Event: Compute Result
EVT_COMPUTE_RESULT = "compute_result"

#: Event: Intermediate Result
EVT_INTERMEDIATE_RESULT = "intermediate_result"

#: Event: Consolidated Result
EVT_CONSOLIDATED_RESULT = "consolidated_result"

#: Event: An error occurred
EVT_ERROR = "error"

#: Event: USB Disk Inserted
EVT_USB_DISK_INSERTED = "usb_disk_inserted"

#: Event: USB Disk Removed
EVT_USB_DISK_REMOVED = "usb_disk_removed"

#: Event: USB disk status update
EVT_USB_STATUS = "usb_status"

#: Event: Device information snapshot for control-center surfaces
EVT_DEVICE_INFO = "device_info"

#: Event: Forwarded Control Center message for local NEIA consumers
EVT_CONTROL_CENTER_MESSAGE = "control_center_message"

#: Event: Robot Status
EVT_ROBOT_STATUS = "robot_status"
