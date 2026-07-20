"""Azure IoT device client wrapper."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

logging.getLogger("azure.iot.device.common.handle_exceptions").setLevel(logging.ERROR)


class AzureDeviceClientAdapter:
    """Thin wrapper around the Azure IoT device SDK."""

    def __init__(
        self,
        connection_string: str,
        *,
        websockets: bool = False,
        keep_alive: int = 60,
        connection_retry_interval: int = 10,
    ):
        self._connection_string = connection_string
        self._websockets = websockets
        self._keep_alive = keep_alive
        self._connection_retry_interval = connection_retry_interval
        self._client = None

    def connect(self) -> None:
        """Create and connect the device client."""
        if self._client is not None:
            return
        try:
            from azure.iot.device import IoTHubDeviceClient
        except ImportError as exc:
            raise RuntimeError(
                "azure-iot-device is not installed. Add the package before running the Azure bridge."
            ) from exc
        self._client = IoTHubDeviceClient.create_from_connection_string(
            self._connection_string,
            websockets=self._websockets,
            keep_alive=self._keep_alive,
            connection_retry=True,
            connection_retry_interval=self._connection_retry_interval,
        )
        self._client.connect()

    def set_method_handler(self, handler: Callable[[Any], None]) -> None:
        """Register the direct-method callback."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        self._client.on_method_request_received = handler

    def set_twin_patch_handler(self, handler: Callable[[dict], None]) -> None:
        """Register the desired-properties callback."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        self._client.on_twin_desired_properties_patch_received = handler

    def set_connection_state_handler(self, handler: Callable[..., None]) -> None:
        """Register the connection-state callback."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        self._client.on_connection_state_change = handler

    def set_background_exception_handler(self, handler: Callable[[Exception], None]) -> None:
        """Register the background-exception callback."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        self._client.on_background_exception = handler

    def send_telemetry(self, payload: dict) -> None:
        """Send telemetry to IoT Hub."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        self._client.send_message(json.dumps(payload))

    def patch_reported_properties(self, payload: dict) -> None:
        """Update reported properties."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        self._client.patch_twin_reported_properties(payload)

    def send_method_response(self, request: Any, status: int, payload: dict) -> None:
        """Respond to a direct method request."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        from azure.iot.device import MethodResponse

        response = MethodResponse.create_from_method_request(
            request,
            status=status,
            payload=payload,
        )
        self._client.send_method_response(response)

    def get_storage_info_for_blob(self, blob_name: str) -> dict:
        """Request upload storage information from IoT Hub."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        return self._client.get_storage_info_for_blob(blob_name)

    def notify_blob_upload_status(
        self,
        correlation_id: str,
        is_success: bool,
        status_code: int,
        status_description: str,
    ) -> None:
        """Notify IoT Hub of upload completion state."""
        if self._client is None:
            raise RuntimeError("Azure device client is not connected")
        self._client.notify_blob_upload_status(
            correlation_id,
            is_success,
            status_code,
            status_description,
        )

    def shutdown(self) -> None:
        """Disconnect the device client."""
        if self._client is None:
            return
        self._client.shutdown()
        self._client = None

    @property
    def connected(self) -> bool:
        """Return the current client connection state."""
        if self._client is None:
            return False
        return bool(self._client.connected)
