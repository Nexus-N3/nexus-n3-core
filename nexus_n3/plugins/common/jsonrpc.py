"""Bidirectional JSON-RPC helpers for plugin host transports."""

from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, TextIO


class JsonRpcError(RuntimeError):
    """Raised when the JSON-RPC connection fails."""


@dataclass
class _PendingRequest:
    event: threading.Event
    response: dict[str, Any] | None = None


class JsonRpcConnection:
    """Threaded line-oriented JSON-RPC connection."""

    def __init__(
        self,
        reader: TextIO,
        writer: TextIO,
        *,
        autostart: bool = True,
        name: str = "jsonrpc",
    ):
        self._reader = reader
        self._writer = writer
        self._name = name
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._handler_lock = threading.Lock()
        self._closed = False
        self._pending: dict[str, _PendingRequest] = {}
        self._handlers: dict[str, Callable[[dict[str, Any]], Any]] = {}
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        if autostart:
            self.start()

    def register_handler(self, method: str, handler: Callable[[dict[str, Any]], Any]) -> None:
        """Register a callable to service an incoming request method."""
        with self._handler_lock:
            self._handlers[method] = handler

    def start(self) -> None:
        """Start the background reader thread once handlers are ready."""
        if self._reader_thread.is_alive():
            return
        self._reader_thread.start()

    def request(self, method: str, params: dict[str, Any] | None = None, *, timeout: float = 30.0) -> Any:
        """Send a JSON-RPC request and wait for the result."""
        self._ensure_open()
        request_id = uuid.uuid4().hex
        pending = _PendingRequest(event=threading.Event())
        with self._pending_lock:
            self._pending[request_id] = pending
        self._write_message(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params or {},
            }
        )
        if not pending.event.wait(timeout):
            with self._pending_lock:
                self._pending.pop(request_id, None)
            raise JsonRpcError(f"{self._name} request timed out: {method}")
        response = pending.response or {}
        if "error" in response:
            error = response["error"] or {}
            raise JsonRpcError(error.get("message") or f"{self._name} request failed: {method}")
        return response.get("result")

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Send a JSON-RPC notification without waiting for a response."""
        self._ensure_open()
        self._write_message(
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params or {},
            }
        )

    def close(self) -> None:
        """Mark the connection closed and release any pending requests."""
        if self._closed:
            return
        self._closed = True
        self._fail_pending(JsonRpcError(f"{self._name} connection closed"))

    def _write_message(self, payload: dict[str, Any]) -> None:
        with self._write_lock:
            self._writer.write(json.dumps(payload, sort_keys=True) + "\n")
            self._writer.flush()

    def _read_loop(self) -> None:
        try:
            for raw in self._reader:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if "method" in message:
                    self._dispatch_request(message)
                else:
                    self._dispatch_response(message)
        finally:
            self._closed = True
            self._fail_pending(JsonRpcError(f"{self._name} connection closed"))

    def _dispatch_request(self, message: dict[str, Any]) -> None:
        worker = threading.Thread(
            target=self._handle_request,
            args=(message,),
            daemon=True,
        )
        worker.start()

    def _handle_request(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        request_id = message.get("id")
        params = message.get("params") or {}
        with self._handler_lock:
            handler = self._handlers.get(method)
        if handler is None:
            if request_id is not None:
                self._write_message(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32601, "message": f"unknown method: {method}"},
                    }
                )
            return
        try:
            result = handler(params)
            if request_id is not None:
                self._write_message({"jsonrpc": "2.0", "id": request_id, "result": result})
        except Exception as exc:
            if request_id is not None:
                self._write_message(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32000, "message": str(exc)},
                    }
                )

    def _dispatch_response(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("id") or "")
        with self._pending_lock:
            pending = self._pending.pop(request_id, None)
        if pending is None:
            return
        pending.response = message
        pending.event.set()

    def _fail_pending(self, error: Exception) -> None:
        with self._pending_lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for item in pending:
            item.response = {"error": {"message": str(error)}}
            item.event.set()

    def _ensure_open(self) -> None:
        if self._closed:
            raise JsonRpcError(f"{self._name} connection closed")
