from nexus_n3.azure_bridge.bridge import AzureBridgeService
from nexus_n3.azure_bridge.config import AzureBridgeConfig
from nexus_n3.gateway.messaging import message_types as mt

import threading
import time


class FakeAzureClient:
    def __init__(self):
        self.telemetry = []
        self.reported = []
        self.method_responses = []
        self.connected = True
        self.connect_calls = 0
        self.shutdown_calls = 0
        self.method_handler = None
        self.twin_patch_handler = None
        self.connection_state_handler = None
        self.background_exception_handler = None

    def connect(self):
        self.connect_calls += 1
        self.connected = True

    def shutdown(self):
        self.shutdown_calls += 1
        self.connected = False

    def set_method_handler(self, handler):
        self.method_handler = handler

    def set_twin_patch_handler(self, handler):
        self.twin_patch_handler = handler

    def set_connection_state_handler(self, handler):
        self.connection_state_handler = handler

    def set_background_exception_handler(self, handler):
        self.background_exception_handler = handler

    def send_telemetry(self, payload):
        self.telemetry.append(payload)

    def patch_reported_properties(self, payload):
        self.reported.append(payload)

    def send_method_response(self, request, status, payload):
        self.method_responses.append({
            "request": request,
            "status": status,
            "payload": payload,
        })

    def get_storage_info_for_blob(self, blob_name):
        return {
            "hostName": "examplestorage.blob.core.windows.net",
            "containerName": "uploads",
            "blobName": blob_name,
            "sasToken": "?sig=abc",
            "correlationId": "corr-1",
        }

    def notify_blob_upload_status(self, correlation_id, is_success, status_code, status_description):
        return None


class FakeLocalClient:
    def __init__(self):
        self.commands = []
        self.started = False
        self.closed = False

    def start(self, handler):
        self.started = True
        self.handler = handler

    def send_command(self, command):
        self.commands.append(command)

    def close(self):
        self.closed = True


class FakeRequest:
    def __init__(self, name, payload=None):
        self.name = name
        self.payload = payload or {}


def test_local_event_is_forwarded_to_cloud_telemetry():
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()

    service._handle_local_event(
        {
            "type": "stream_started",
            "payload": {"stream_tag": "baseline"},
        }
    )

    assert len(service.azure_client.telemetry) == 1
    telemetry = service.azure_client.telemetry[0]
    assert telemetry["type"] == "stream_started"
    assert telemetry["payload"] == {"stream_tag": "baseline"}
    assert telemetry["device_id"] == "d1"
    assert telemetry["site"] == "test-site"
    assert "timestamp" in telemetry


def test_safe_read_method_returns_correlated_payload_and_still_forwards_telemetry():
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.local_client = FakeLocalClient()

    request = FakeRequest(mt.CMD_IS_SERVER_READY)

    worker = threading.Thread(target=service._handle_method_request, args=(request,), daemon=True)
    worker.start()

    deadline = time.time() + 2
    while not service.local_client.commands and time.time() < deadline:
        time.sleep(0.01)

    assert service.local_client.commands
    command = service.local_client.commands[0]
    correlation_id = command["payload"]["correlation_id"]

    service._handle_local_event(
        {
            "type": mt.EVT_SERVER_READY,
            "payload": {
                "msg": "System Server Ready",
                "site": "test-site",
                "supported_sensors": [],
                "supported_algorithms": ["standard_loading_intensity"],
                "supported_gateways": ["zeromq_gateway"],
                "supported_bridges": ["azure_bridge"],
                "correlation_id": correlation_id,
            },
        }
    )

    worker.join(timeout=2)
    assert not worker.is_alive()

    assert len(service.azure_client.telemetry) == 1
    assert service.azure_client.telemetry[0]["type"] == mt.EVT_SERVER_READY

    assert len(service.azure_client.method_responses) == 1
    response = service.azure_client.method_responses[0]
    assert response["status"] == 200
    assert response["payload"]["correlation_id"] == correlation_id
    assert response["payload"]["event_type"] == mt.EVT_SERVER_READY
    assert response["payload"]["payload"]["msg"] == "System Server Ready"


def test_control_center_direct_method_forwards_only_neia_targeted_messages():
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
        remote_control_enabled=True,
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.local_client = FakeLocalClient()

    request = FakeRequest(
        mt.EVT_CONTROL_CENTER_MESSAGE,
        {
            "type": "subject_catalog_update",
            "target": "neia",
            "payload": {"groups": []},
        },
    )

    service._handle_method_request(request)

    assert len(service.local_client.commands) == 1
    command = service.local_client.commands[0]
    assert command["type"] == mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE
    assert command["payload"]["message"]["type"] == "subject_catalog_update"
    assert command["payload"]["message"]["target"] == "neia"

    assert len(service.azure_client.method_responses) == 1
    response = service.azure_client.method_responses[0]
    assert response["status"] == 202
    assert response["payload"]["command_type"] == mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE


def test_control_center_direct_method_rejects_non_neia_targeted_messages():
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
        remote_control_enabled=True,
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.local_client = FakeLocalClient()

    request = FakeRequest(
        mt.EVT_CONTROL_CENTER_MESSAGE,
        {
            "type": "subject_catalog_update",
            "target": "nexus-n3-core",
            "payload": {"groups": []},
        },
    )

    service._handle_method_request(request)

    assert service.local_client.commands == []
    assert len(service.azure_client.method_responses) == 1
    response = service.azure_client.method_responses[0]
    assert response["status"] == 400
    assert response["payload"]["message"] == "Control Center message is not targeted for NEIA"


def test_ensure_cloud_connection_reconnects_and_rebinds_handlers():
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.azure_client.connected = False

    assert service._ensure_cloud_connection(force=True) is True
    assert service.azure_client.connect_calls == 1
    assert service.azure_client.shutdown_calls == 1
    assert service.azure_client.method_handler == service._handle_method_request
    assert service.azure_client.twin_patch_handler == service._handle_twin_patch
    assert service.azure_client.connection_state_handler == service._handle_connection_state_change
    assert service.azure_client.background_exception_handler == service._handle_background_exception


def test_connection_state_change_requests_reconnect_when_disconnected():
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.azure_client.connected = False

    service._handle_connection_state_change()

    assert service._reconnect_requested is True


def test_stream_drained_retries_missing_archive_then_uploads_and_unmounts(monkeypatch, tmp_path):
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
        upload_retry_interval=1,
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.local_client = FakeLocalClient()
    service._local_client_started = True

    archive_path = tmp_path / "session.zip"
    payload = {
        "status": "ok",
        "all_local_streams_stopped": True,
        "session_archive_path": str(archive_path),
        "session_archive_name": archive_path.name,
        "session_timestamp": "20260417_120000",
        "base_root": "/exports/nexus_n3_data/nexus_n3_outputs",
    }

    uploads = []

    class FakeUploader:
        def __init__(self, azure_client):
            self.azure_client = azure_client

        def upload_file(self, file_path, *, blob_name=None):
            uploads.append((file_path, blob_name))
            return type(
                "Result",
                (),
                {
                    "success": True,
                    "status_code": 200,
                    "status_description": "Uploaded session.zip",
                    "blob_name": blob_name,
                    "container_name": "uploads",
                },
            )()

    monkeypatch.setattr("nexus_n3.azure_bridge.bridge.IoTHubFileUploader", FakeUploader)

    service._handle_stream_drained(payload)
    service._process_pending_uploads(now=0.0)

    assert uploads == []
    assert len(service._pending_uploads) == 1

    archive_path.write_text("session-bytes", encoding="utf-8")
    service._process_pending_uploads(now=2.0)

    assert uploads == [
        (
            str(archive_path),
            "unknown-customer/test-site/d1/session.zip",
        )
    ]
    assert len(service._pending_uploads) == 0
    assert service.local_client.commands[-1]["type"] == mt.CMD_USB_SAFE_UNMOUNT


def test_failed_upload_is_retried_until_success(monkeypatch, tmp_path):
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
        upload_retry_interval=1,
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.local_client = FakeLocalClient()
    service._local_client_started = True

    archive_path = tmp_path / "session.zip"
    archive_path.write_text("session-bytes", encoding="utf-8")
    payload = {
        "status": "ok",
        "all_local_streams_stopped": True,
        "session_archive_path": str(archive_path),
        "session_archive_name": archive_path.name,
        "session_timestamp": "20260417_120000",
        "base_root": "/exports/nexus_n3_data/nexus_n3_outputs",
    }

    attempts = {"count": 0}

    class FakeUploader:
        def __init__(self, azure_client):
            self.azure_client = azure_client

        def upload_file(self, file_path, *, blob_name=None):
            attempts["count"] += 1
            if attempts["count"] == 1:
                return type(
                    "Result",
                    (),
                    {
                        "success": False,
                        "status_code": 500,
                        "status_description": "Transient upload failure",
                        "blob_name": blob_name,
                        "container_name": "uploads",
                    },
                )()
            return type(
                "Result",
                (),
                {
                    "success": True,
                    "status_code": 200,
                    "status_description": "Uploaded session.zip",
                    "blob_name": blob_name,
                    "container_name": "uploads",
                },
            )()

    monkeypatch.setattr("nexus_n3.azure_bridge.bridge.IoTHubFileUploader", FakeUploader)

    service._handle_stream_drained(payload)
    service._process_pending_uploads(now=0.0)
    assert len(service._pending_uploads) == 1

    service._process_pending_uploads(now=2.0)
    assert attempts["count"] == 2
    assert len(service._pending_uploads) == 0
    assert service.local_client.commands[-1]["type"] == mt.CMD_USB_SAFE_UNMOUNT


def test_local_fallback_upload_does_not_request_usb_unmount(monkeypatch, tmp_path):
    config = AzureBridgeConfig(
        connection_string="HostName=test;DeviceId=d1;SharedAccessKey=abc",
        device_id="d1",
        site="test-site",
    )
    service = AzureBridgeService(config)
    service.azure_client = FakeAzureClient()
    service.local_client = FakeLocalClient()
    service._local_client_started = True

    archive_path = tmp_path / "session.zip"
    archive_path.write_text("session-bytes", encoding="utf-8")
    payload = {
        "status": "ok",
        "all_local_streams_stopped": True,
        "session_archive_path": str(archive_path),
        "session_archive_name": archive_path.name,
        "session_timestamp": "20260417_120000",
        "base_root": str(tmp_path),
    }

    class FakeUploader:
        def __init__(self, azure_client):
            self.azure_client = azure_client

        def upload_file(self, file_path, *, blob_name=None):
            return type(
                "Result",
                (),
                {
                    "success": True,
                    "status_code": 200,
                    "status_description": "Uploaded session.zip",
                    "blob_name": blob_name,
                    "container_name": "uploads",
                },
            )()

    monkeypatch.setattr("nexus_n3.azure_bridge.bridge.IoTHubFileUploader", FakeUploader)

    service._handle_stream_drained(payload)
    service._process_pending_uploads(now=0.0)

    assert len(service._pending_uploads) == 0
    assert service.local_client.commands == []
