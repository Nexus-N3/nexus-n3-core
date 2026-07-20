"""
Live IoT Hub file-upload smoke test.

Run this against a configured IoT Hub device and linked storage account:

    PYTHONPATH=/path/to/nexus-n3-core python -m nexus_n3_tests.azure_bridge.test_6_live_file_upload_smoke /path/to/file

The Azure bridge env file is used for device auth.
"""

from __future__ import annotations

import sys
from pathlib import Path

from nexus_n3.azure_bridge.azure_device_client import AzureDeviceClientAdapter
from nexus_n3.azure_bridge.config import AzureBridgeConfig
from nexus_n3.azure_bridge.file_upload import IoTHubFileUploader


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python -m nexus_n3_tests.azure_bridge.test_6_live_file_upload_smoke /path/to/file")
        raise SystemExit(2)

    file_path = Path(sys.argv[1]).expanduser().resolve()
    if not file_path.is_file():
        print(f"File not found: {file_path}")
        raise SystemExit(2)

    config = AzureBridgeConfig.from_env()
    client = AzureDeviceClientAdapter(config.connection_string)
    client.connect()
    try:
        uploader = IoTHubFileUploader(client)
        result = uploader.upload_file(file_path)
        print("Upload result:")
        print(
            {
                "success": result.success,
                "status_code": result.status_code,
                "status_description": result.status_description,
                "blob_name": result.blob_name,
                "container_name": result.container_name,
                "correlation_id": result.correlation_id,
            }
        )
    finally:
        client.shutdown()


if __name__ == "__main__":
    main()
