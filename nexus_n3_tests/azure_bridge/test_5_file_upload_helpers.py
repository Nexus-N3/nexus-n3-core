from pathlib import Path

from nexus_n3.azure_bridge.file_upload import (
    IoTHubFileUploader,
    build_blob_sas_url,
    build_session_blob_name,
)


class FakeAzureClient:
    def __init__(self):
        self.notifications = []
        self.storage_info = {
            "hostName": "examplestorage.blob.core.windows.net",
            "containerName": "uploads",
            "blobName": "sample.txt",
            "sasToken": "?sig=abc",
            "correlationId": "corr-1",
        }

    def get_storage_info_for_blob(self, blob_name):
        data = dict(self.storage_info)
        data["blobName"] = blob_name
        return data

    def notify_blob_upload_status(self, correlation_id, is_success, status_code, status_description):
        self.notifications.append(
            {
                "correlation_id": correlation_id,
                "is_success": is_success,
                "status_code": status_code,
                "status_description": status_description,
            }
        )


def test_build_blob_sas_url():
    url = build_blob_sas_url(
        {
            "hostName": "examplestorage.blob.core.windows.net",
            "containerName": "uploads",
            "blobName": "sample.txt",
            "sasToken": "?sig=abc",
        }
    )
    assert url == "https://examplestorage.blob.core.windows.net/uploads/sample.txt?sig=abc"


def test_upload_file_reports_missing_file():
    uploader = IoTHubFileUploader(FakeAzureClient())

    result = uploader.upload_file("/tmp/this_file_should_not_exist_azure_bridge.txt")

    assert result.success is False
    assert result.status_code == 404
    assert uploader.azure_client.notifications[0]["is_success"] is False


def test_upload_file_success_notifies_iot_hub(monkeypatch, tmp_path: Path):
    test_file = tmp_path / "sample.txt"
    test_file.write_text("hello", encoding="utf-8")
    azure_client = FakeAzureClient()
    uploader = IoTHubFileUploader(azure_client)

    uploaded = {}

    class FakeBlobClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def upload_blob(self, handle, overwrite):
            uploaded["content"] = handle.read()
            uploaded["overwrite"] = overwrite

            class _Response:
                status_code = 201

            class _Result:
                _response = _Response()

            return _Result()

    monkeypatch.setattr(
        "nexus_n3.azure_bridge.file_upload.BlobClient.from_blob_url",
        lambda url: FakeBlobClient(),
    )

    result = uploader.upload_file(test_file)

    assert result.success is True
    assert result.status_code == 201
    assert uploaded["content"] == b"hello"
    assert uploaded["overwrite"] is True
    assert azure_client.notifications[0]["is_success"] is True


def test_build_session_blob_name_uses_customer_site_and_device_prefix() -> None:
    blob_name = build_session_blob_name(
        "local_home_Right_Step_session_20260331_102030.zip",
        customer_id="customer-dlr",
        site_id="local_home",
        device_id="rs-nexus-edge-test-001",
    )

    assert blob_name == (
        "customer-dlr/local_home/rs-nexus-edge-test-001/"
        "local_home_Right_Step_session_20260331_102030.zip"
    )
