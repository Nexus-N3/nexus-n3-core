"""ZeroMQ client for remote compute requests and async results."""

import os
import time
import pickle
import threading
import zmq

from nexus_n3.logger.logger import get_module_logger

logger = get_module_logger("Compute Manager")


def _env_flag(name, default=False):
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


class RemoteComputeClient:
    """ZeroMQ client for remote compute requests and async results."""

    def __init__(self, endpoint, on_result, error_cb=None):
        self.endpoint = endpoint
        self.on_result = on_result
        self.error_cb = error_cb

        self._ctx = zmq.Context.instance()
        self._socket = self._ctx.socket(zmq.DEALER)
        self._socket.connect(self.endpoint)
        self._socket.setsockopt(zmq.LINGER, 0)

        self._pending = {}
        self._pending_lock = threading.Lock()
        self._recv_thread = None
        self._running = False
        self._perf_enabled = _env_flag("NEXUS_PERF_LOG", default=False)

    def start(self):
        """Start the receive loop in a background thread."""
        if self._running:
            return
        self._running = True
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()

    def send(self, payload, request_id):
        """Send a compute request without blocking."""
        try:
            with self._pending_lock:
                self._pending[request_id] = {
                    "start": time.perf_counter(),
                    "algorithm_name": payload.get("algorithm_name"),
                    "address": payload.get("address"),
                }
            self._socket.send(pickle.dumps(payload))
            return True
        except Exception as exc:
            if self.error_cb:
                self.error_cb(f"Remote compute send failed: {exc}")
            return False

    def _recv_loop(self):
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while self._running:
            try:
                events = dict(poller.poll(timeout=1000))
                if self._socket in events:
                    frames = self._socket.recv_multipart()
                    msg_bytes = frames[-1]
                    msg = pickle.loads(msg_bytes)
                    request_id = msg.get("request_id")
                    result = msg.get("result")
                    if self._perf_enabled and request_id:
                        with self._pending_lock:
                            meta = self._pending.get(request_id)
                        if meta:
                            elapsed_ms = (time.perf_counter() - meta["start"]) * 1000.0
                            logger.info(
                                "perf delegate rtt: algo=%s address=%s ms=%.2f",
                                meta.get("algorithm_name"),
                                meta.get("address"),
                                elapsed_ms,
                            )
                    with self._pending_lock:
                        self._pending.pop(request_id, None)
                    if result is not None:
                        self.on_result(result, request_id=request_id)
            except Exception as exc:
                if self.error_cb:
                    self.error_cb(f"Remote compute recv failed: {exc}")
