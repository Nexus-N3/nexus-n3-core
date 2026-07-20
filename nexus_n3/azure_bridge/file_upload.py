"""IoT Hub file upload helpers for the Azure bridge."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from azure.core.exceptions import AzureError
from azure.storage.blob import BlobClient

from nexus_n3.logger.logger import get_module_logger

logger = get_module_logger("Azure Bridge Upload")

_SEGMENT_SANITIZER = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(slots=True)
class FileUploadResult:
    """Outcome of a file upload through IoT Hub."""

    success: bool
    status_code: int
    status_description: str
    blob_name: str
    container_name: str | None = None
    correlation_id: str | None = None


def _sanitize_blob_segment(value: str | None, *, fallback: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return fallback
    cleaned = _SEGMENT_SANITIZER.sub("-", raw).strip("-._")
    return cleaned or fallback


def build_session_blob_name(
    archive_name: str,
    *,
    customer_id: str | None,
    site_id: str | None,
    device_id: str | None,
) -> str:
    """Build a stable blob path that preserves the archive filename."""
    return "/".join(
        [
            _sanitize_blob_segment(customer_id, fallback="unknown-customer"),
            _sanitize_blob_segment(site_id, fallback="unknown-site"),
            _sanitize_blob_segment(device_id, fallback="unknown-device"),
            Path(archive_name).name,
        ]
    )


def build_blob_sas_url(storage_info: dict) -> str:
    """Build the SAS URL expected by BlobClient.from_blob_url."""
    return "https://{}/{}/{}{}".format(
        storage_info["hostName"],
        storage_info["containerName"],
        storage_info["blobName"],
        storage_info["sasToken"],
    )


class IoTHubFileUploader:
    """Upload local files through the IoT Hub file-upload flow."""

    def __init__(self, azure_client):
        self.azure_client = azure_client

    def upload_file(self, file_path: str | Path, *, blob_name: str | None = None) -> FileUploadResult:
        """Upload a local file and notify IoT Hub of the outcome."""
        path = Path(file_path)
        upload_name = blob_name or path.name

        storage_info = self.azure_client.get_storage_info_for_blob(upload_name)
        correlation_id = storage_info["correlationId"]
        container_name = storage_info["containerName"]

        try:
            with BlobClient.from_blob_url(build_blob_sas_url(storage_info)) as blob_client:
                with path.open("rb") as handle:
                    response = blob_client.upload_blob(handle, overwrite=True)
            status_code = getattr(getattr(response, "_response", None), "status_code", None) or 200
            result = FileUploadResult(
                success=True,
                status_code=int(status_code),
                status_description=f"Uploaded {path.name}",
                blob_name=storage_info["blobName"],
                container_name=container_name,
                correlation_id=correlation_id,
            )
            self.azure_client.notify_blob_upload_status(
                correlation_id,
                True,
                result.status_code,
                result.status_description,
            )
            logger.info(
                f"file upload sent: blob={result.blob_name} container={container_name} status={result.status_code}",
                extra={"console": True},
            )
            return result
        except FileNotFoundError:
            description = f"File not found: {path}"
            self.azure_client.notify_blob_upload_status(correlation_id, False, 404, description)
            logger.info(
                f"file upload failed: blob={upload_name} status=404 reason=file_not_found",
                extra={"console": True},
            )
            return FileUploadResult(
                success=False,
                status_code=404,
                status_description=description,
                blob_name=upload_name,
                container_name=container_name,
                correlation_id=correlation_id,
            )
        except AzureError as exc:
            status_code = int(getattr(exc, "status_code", 500) or 500)
            description = str(exc)
            self.azure_client.notify_blob_upload_status(correlation_id, False, status_code, description)
            logger.info(
                f"file upload failed: blob={upload_name} status={status_code}",
                extra={"console": True},
            )
            return FileUploadResult(
                success=False,
                status_code=status_code,
                status_description=description,
                blob_name=upload_name,
                container_name=container_name,
                correlation_id=correlation_id,
            )
