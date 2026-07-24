"""Runtime support for host-backed algorithm plugins."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..common.jsonio import read_json
from ..install.config import resolve_plugin_root
from ..install.layout import PluginLayout
from .serde import RemoteComputeResult, object_to_mapping, to_jsonable
from .transport import StdioJsonRpcTransport, PluginTransportError
from .environment import prepend_pythonpath, resolve_runtime_python

@dataclass(frozen=True)
class InstalledAlgorithmPlugin:
    plugin_id: str
    algorithm_name: str
    install_path: Path
    runtime_path: Path
    manifest: dict[str, Any]


class AlgorithmHostClient:
    """Client wrapper for one algorithm plugin host process."""

    def __init__(self, plugin: InstalledAlgorithmPlugin):
        self.plugin = plugin

        runtime_python = resolve_runtime_python(plugin.runtime_path)

        core_import_root = Path(__file__).resolve().parents[3]

        env = os.environ.copy()
        prepend_pythonpath(env, core_import_root)
        
        self.transport = StdioJsonRpcTransport(
            [
                str(runtime_python),
                "-m",
                "nexus_n3.plugins.runtime.algorithm_host",
                "--install-path",
                str(plugin.install_path),
            ],
            env=env,
            cwd=core_import_root,
        )
        description = self.transport.request("describe", {})
        health = self.transport.request("healthcheck", {})

        if description.get("plugin_id") != plugin.plugin_id:
            raise PluginTransportError(
                "Plugin host loaded an unexpected plugin: "
                f"expected={plugin.plugin_id!r}, "
                f"received={description.get('plugin_id')!r}"
            )

        if not health.get("ok"):
            raise PluginTransportError(
                f"Plugin host health check failed: {health}"
            )

    def close(self) -> None:
        self.transport.close()

    def start_algorithm(
        self,
        *,
        address: str,
        sampling_rate: int | None,
        input_parameters: dict[str, Any] | None,
        subject_id: str | None,
        location: str | None,
    ) -> None:
        self.transport.request(
            "start_algorithm",
            {
                "address": address,
                "sampling_rate": sampling_rate,
                "input_parameters": input_parameters,
                "subject_id": subject_id,
                "location": location,
            },
        )

    def ingest_sample(self, address: str, sample: Any) -> list[RemoteComputeResult]:
        payload = object_to_mapping(sample)
        payload["sample_type"] = getattr(sample, "sample_type", payload.get("sample_type"))
        result = self.transport.request(
            "ingest_sample",
            {
                "address": address,
                "sample": to_jsonable(payload),
            },
        )
        return [RemoteComputeResult(item) for item in (result or {}).get("results", [])]

    def should_run_intermediate(self, result_buffers: dict[str, Any]) -> bool:
        return bool(
            self.transport.request(
                "should_run_intermediate",
                {"result_buffers": _serialize_result_buffers(result_buffers)},
            )
        )

    def run_intermediate(self, result_buffers: dict[str, Any]) -> dict[str, Any] | None:
        return self.transport.request(
            "run_intermediate",
            {"result_buffers": _serialize_result_buffers(result_buffers)},
        )

    def run_consolidation(self, subject_id: str, intermediate_records: list[dict]) -> dict[str, Any] | None:
        return self.transport.request(
            "run_consolidation",
            {
                "subject_id": subject_id,
                "intermediate_records": intermediate_records,
            },
        )

    def run_batch(
        self,
        *,
        address: str,
        samples: list[Any],
        sampling_rate: int | None,
        input_parameters: dict[str, Any] | None,
        subject_id: str | None,
        location: str | None,
    ) -> list[RemoteComputeResult]:
        result = self.transport.request(
            "run_batch",
            {
                "address": address,
                "samples": [to_jsonable(object_to_mapping(sample)) for sample in samples],
                "sampling_rate": sampling_rate,
                "input_parameters": input_parameters,
                "subject_id": subject_id,
                "location": location,
            },
        )
        return [RemoteComputeResult(item) for item in (result or {}).get("results", [])]


class PluginRuntimeManager:
    """Resolves installed algorithm bundles and manages their host clients."""

    def __init__(self, plugin_root: str | Path | None = None):
        self.layout = PluginLayout(resolve_plugin_root(plugin_root))
        self._algorithm_clients: dict[str, AlgorithmHostClient] = {}

    def get_algorithm_client(self, algorithm_name: str) -> AlgorithmHostClient | None:
        normalized = _normalize_name(algorithm_name)
        existing = self._algorithm_clients.get(normalized)
        if existing is not None:
            return existing
        plugin = self._find_algorithm_plugin(algorithm_name)
        if plugin is None:
            return None
        client = AlgorithmHostClient(plugin)
        self._algorithm_clients[normalized] = client
        return client

    def close(self) -> None:
        for client in self._algorithm_clients.values():
            client.close()
        self._algorithm_clients.clear()

    def _find_algorithm_plugin(self, algorithm_name: str) -> InstalledAlgorithmPlugin | None:
        normalized = _normalize_name(algorithm_name)
        for catalog_path in sorted(self.layout.catalog_dir.glob("*.json")):
            if catalog_path.name in {"plugins.json", "install_failures.json"}:
                continue
            catalog = read_json(catalog_path, default={}) or {}
            if catalog.get("plugin_type") != "algorithm" or not catalog.get("enabled", True):
                continue
            active_version = catalog.get("active_version")
            if not active_version:
                continue
            version_payload = (catalog.get("versions") or {}).get(active_version) or {}
            install_path_raw = version_payload.get("install_path")
            runtime_path_raw = version_payload.get("runtime_path")
            if not install_path_raw or not runtime_path_raw:
                continue
            install_path = Path(install_path_raw)
            manifest = read_json(install_path / "manifest.json", default={}) or {}
            capabilities = manifest.get("capabilities") or {}
            manifest_algorithm_name = capabilities.get("algorithm_name") or manifest.get("plugin_id")
            if _normalize_name(manifest_algorithm_name) != normalized:
                continue
            return InstalledAlgorithmPlugin(
                plugin_id=str(catalog.get("plugin_id") or manifest.get("plugin_id")),
                algorithm_name=str(manifest_algorithm_name),
                install_path=install_path,
                runtime_path=Path(runtime_path_raw),
                manifest=manifest,
            )
        return None


class HostBackedAlgorithm:
    """Algorithm-like proxy that forwards samples to an isolated host."""

    def __init__(self, client: AlgorithmHostClient, *, address: str):
        self.client = client
        self.address = address
        self.name = client.plugin.algorithm_name
        self.result_callback = None

    def register_result_listener(self, callback) -> None:
        self.result_callback = callback

    def register_compute_delegate(self, callback) -> None:
        self.compute_delegate = callback

    def on_sample(self, sample) -> None:
        results = self.client.ingest_sample(self.address, sample)
        if self.result_callback:
            for result in results:
                self.result_callback(result)


class HostBackedIntermediateExecutor:
    """Intermediate executor proxy for host-backed algorithms."""

    def __init__(self, client: AlgorithmHostClient):
        self.client = client

    def should_run(self, result_buffers) -> bool:
        return self.client.should_run_intermediate(result_buffers)

    def run(self, result_buffers):
        return self.client.run_intermediate(result_buffers)


class HostBackedConsolidationExecutor:
    """Consolidation executor proxy for host-backed algorithms."""

    def __init__(self, client: AlgorithmHostClient):
        self.client = client

    def consolidate(self, *, subject_id: str, intermediate_records: list[dict]):
        return self.client.run_consolidation(subject_id, intermediate_records)


#def _runtime_python(runtime_path: Path) -> Path:
#    return runtime_path / "bin" / "python"


def _normalize_name(name: str) -> str:
    return str(name).strip().lower()


def _serialize_result_buffers(result_buffers: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    payload: dict[str, list[dict[str, Any]]] = {}
    for address, items in result_buffers.items():
        payload[address] = [to_jsonable(object_to_mapping(item)) for item in list(items)]
    return payload
