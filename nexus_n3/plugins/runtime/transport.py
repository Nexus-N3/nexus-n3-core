"""JSON-RPC transport support for isolated plugin hosts."""

from __future__ import annotations

import subprocess
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from nexus_n3.logger.logger import get_module_logger

from ..common.jsonrpc import JsonRpcConnection, JsonRpcError

logger = get_module_logger("PluginTransport")


class PluginTransportError(RuntimeError):
    """Raised when plugin transport requests fail."""


class PluginTransport(ABC):
    """Transport-neutral request/response contract for plugin hosts."""

    @abstractmethod
    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Send one RPC request and return its result."""

    @abstractmethod
    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Send one JSON-RPC notification without waiting for a response."""

    @abstractmethod
    def register_handler(self, method: str, handler) -> None:
        """Register a handler for host-initiated requests."""

    @abstractmethod
    def close(self) -> None:
        """Close transport resources."""


class StdioJsonRpcTransport(PluginTransport):
    """JSON-RPC over stdio for one plugin host process."""

    def __init__(self, command: list[str], *, env: dict[str, str] | None = None, cwd: str | Path | None = None):
        self._closed = False
        self._process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(cwd) if cwd is not None else None,
            env=env,
            bufsize=1,
        )
        assert self._process.stdout is not None
        assert self._process.stdin is not None
        self._rpc = JsonRpcConnection(
            self._process.stdout,
            self._process.stdin,
            name="plugin-host",
        )
        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self._ensure_open()
        try:
            return self._rpc.request(method, params or {})
        except JsonRpcError as exc:
            if self._process.poll() is not None:
                self._raise_dead_process(method)
            raise PluginTransportError(str(exc)) from exc

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        self._ensure_open()
        try:
            self._rpc.notify(method, params or {})
        except JsonRpcError as exc:
            if self._process.poll() is not None:
                self._raise_dead_process(method)
            raise PluginTransportError(str(exc)) from exc

    def register_handler(self, method: str, handler) -> None:
        self._rpc.register_handler(method, handler)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._rpc.request("shutdown", {}, timeout=2.0)
        except Exception:
            pass
        self._rpc.close()
        if self._process.stdin:
            try:
                self._process.stdin.close()
            except Exception:
                pass
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=2.0)

    def _ensure_open(self) -> None:
        if self._closed:
            raise PluginTransportError("plugin transport is closed")
        if self._process.poll() is not None:
            self._raise_dead_process("request")

    def _raise_dead_process(self, method: str) -> None:
        code = self._process.poll()
        raise PluginTransportError(f"plugin host exited before completing {method} (exit={code})")

    def _drain_stderr(self) -> None:
        assert self._process.stderr is not None
        for line in self._process.stderr:
            text = line.rstrip()
            if text:
                logger.error("plugin-host stderr: %s", text)
