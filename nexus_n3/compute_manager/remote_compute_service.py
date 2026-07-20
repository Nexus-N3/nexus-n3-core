"""Remote compute delegation service."""

import uuid
import threading
from collections import defaultdict

from nexus_n3.compute_manager.remote_compute_client import RemoteComputeClient


class RemoteComputeService:
    """Owns AI endpoint selection and remote compute client lifecycle."""

    def __init__(self, on_result, error_cb=None):
        self._on_result = on_result
        self._error_cb = error_cb
        self._registry = None
        self._remote_client = None
        self._remote_endpoint = None
        self._remote_result_counts = defaultdict(lambda: defaultdict(int))
        self._lock = threading.Lock()

    def set_registry(self, registry):
        """Attach a NodeRegistry for AI node discovery."""
        with self._lock:
            self._registry = registry

    def delegate_compute(self, algorithm, samples):
        """
        Delegate compute to an AI node if available.

        Returns:
            bool: True if delegated, False to fall back to local compute.
        """
        endpoint = self._select_ai_endpoint(getattr(algorithm, "name", None))
        if not endpoint:
            return False

        with self._lock:
            if self._remote_client is None or endpoint != self._remote_endpoint:
                self._remote_endpoint = endpoint
                self._remote_client = RemoteComputeClient(
                    endpoint=endpoint,
                    on_result=self.on_remote_result,
                    error_cb=self._error_cb,
                )
                self._remote_client.start()
            client = self._remote_client

        request_id = str(uuid.uuid4())
        payload = {
            "type": "RUN_ALGO",
            "request_id": request_id,
            "algorithm_name": getattr(algorithm, "name", None),
            "address": getattr(algorithm, "address", None),
            "subject_id": getattr(algorithm, "subject_id", None),
            "location": getattr(algorithm, "location", None),
            "sampling_rate": getattr(algorithm, "sampling_rate", None),
            "input_parameters": getattr(algorithm, "input_parameters", None),
            "samples": samples,
        }
        return client.send(payload, request_id=request_id)

    def on_remote_result(self, result, request_id=None):
        """Normalize remote result count locally, then forward result."""
        try:
            algo_name = result.algorithm_name
            address = result.address
            self._remote_result_counts[algo_name][address] += 1
            if hasattr(result, "result_count"):
                result.result_count = self._remote_result_counts[algo_name][address]
        except Exception as exc:
            if self._error_cb:
                self._error_cb(f"Remote result count update failed: {exc}")
        self._on_result(result)

    def _select_ai_endpoint(self, algorithm_name: str | None = None):
        with self._lock:
            registry = self._registry
        if not registry:
            return None
        ai_nodes = getattr(registry, "get_ai_nodes", None)
        if not ai_nodes:
            return None
        nodes = registry.get_ai_nodes()
        for _, data in nodes.items():
            ip = data.get("ip")
            port = data.get("compute_port")
            capabilities = data.get("capabilities") or {}
            supported_algorithms = {
                str(name).strip().lower()
                for name in capabilities.get("supported_algorithms", [])
                if str(name).strip()
            }
            if algorithm_name and str(algorithm_name).strip().lower() not in supported_algorithms:
                continue
            if ip and port:
                return f"tcp://{ip}:{port}"
        return None
