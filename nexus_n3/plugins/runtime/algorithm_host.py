"""Host process entry point for isolated algorithm plugins."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections import deque
from dataclasses import is_dataclass
from pathlib import Path
from typing import Any

from ..common.jsonio import read_json
from .serde import deep_namespace, object_to_mapping, to_jsonable


def _load_symbol(raw: str):
    module_name, _, attr_name = raw.partition(":")
    if not module_name or not attr_name:
        raise ValueError(f"invalid entry point: {raw}")
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def _sample_from_payload(payload: dict[str, Any]) -> Any:
    sample_type = str(payload.get("sample_type") or "").strip().lower()
    try:
        if sample_type == "imu":
            from nexus_n3_plugin_sdk.samples import IMUSample

            return IMUSample(
                timestamp=payload["timestamp"],
                sensor_type=payload["sensor_type"],
                address=payload["address"],
                location=payload.get("location"),
                sampling_rate=payload.get("sampling_rate"),
                quat=tuple(payload["quat"]) if payload.get("quat") is not None else None,
                accel=tuple(payload["accel"]) if payload.get("accel") is not None else None,
                gyro=tuple(payload["gyro"]) if payload.get("gyro") is not None else None,
            )
    except Exception:
        pass
    return deep_namespace(payload)


def _results_buffer_from_payload(payload: dict[str, list[dict]]) -> dict[str, deque]:
    return {
        address: deque([deep_namespace(item) for item in items])
        for address, items in payload.items()
    }


class AlgorithmHost:
    """Owns one plugin process runtime for one algorithm plugin version."""

    def __init__(self, install_path: Path):
        self.install_path = install_path
        self.manifest = read_json(install_path / "manifest.json", default={}) or {}
        self.capabilities = (self.manifest.get("capabilities") or {}).copy()
        self._algorithm_cls = _load_symbol(
            "{module}:{callable}".format(**self.manifest["entrypoint"])
        )
        self._intermediate_executor = None
        self._consolidation_executor = None
        executor_entry_points = self.capabilities.get("executor_entry_points") or {}
        if executor_entry_points.get("intermediate"):
            self._intermediate_executor = _load_symbol(executor_entry_points["intermediate"])()
        if executor_entry_points.get("consolidation"):
            self._consolidation_executor = _load_symbol(executor_entry_points["consolidation"])()
        self._instances: dict[str, Any] = {}
        self._emitted_results: list[dict[str, Any]] = []

    def describe(self) -> dict[str, Any]:
        return {
            "plugin_id": self.manifest.get("plugin_id"),
            "plugin_type": self.manifest.get("plugin_type"),
            "algorithm_name": self.capabilities.get("algorithm_name"),
            "supports_intermediate": self.capabilities.get("supports_intermediate", False),
            "supports_consolidation": self.capabilities.get("supports_consolidation", False),
        }

    def healthcheck(self) -> dict[str, Any]:
        return {"ok": True}

    def start_algorithm(self, params: dict[str, Any]) -> dict[str, Any]:
        address = params["address"]
        algorithm = self._algorithm_cls(
            address=address,
            sampling_rate=params.get("sampling_rate"),
            input_parameters=params.get("input_parameters"),
        )
        algorithm.subject_id = params.get("subject_id")
        algorithm.location = params.get("location")
        if hasattr(algorithm, "register_result_listener"):
            algorithm.register_result_listener(self._capture_result)
        else:
            algorithm.result_callback = self._capture_result
        if hasattr(algorithm, "register_compute_delegate"):
            algorithm.register_compute_delegate(lambda *_args, **_kwargs: False)
        else:
            algorithm.compute_delegate = lambda *_args, **_kwargs: False
        self._instances[address] = algorithm
        return {"address": address, "started": True}

    def ingest_sample(self, params: dict[str, Any]) -> dict[str, Any]:
        address = params["address"]
        algorithm = self._instances[address]
        sample = _sample_from_payload(params["sample"])
        self._emitted_results = []
        algorithm.on_sample(sample)
        results = list(self._emitted_results)
        self._emitted_results = []
        return {"results": results}

    def should_run_intermediate(self, params: dict[str, Any]) -> bool:
        if not self._intermediate_executor:
            return False
        result_buffers = _results_buffer_from_payload(params.get("result_buffers") or {})
        return bool(self._intermediate_executor.should_run(result_buffers))

    def run_intermediate(self, params: dict[str, Any]) -> Any:
        if not self._intermediate_executor:
            return None
        result_buffers = _results_buffer_from_payload(params.get("result_buffers") or {})
        result = self._intermediate_executor.run(result_buffers)
        return to_jsonable(result)

    def run_consolidation(self, params: dict[str, Any]) -> Any:
        if not self._consolidation_executor:
            return None
        if hasattr(self._consolidation_executor, "consolidate"):
            result = self._consolidation_executor.consolidate(
                subject_id=params["subject_id"],
                intermediate_records=params.get("intermediate_records") or [],
            )
        else:
            result = None
        return to_jsonable(result)

    def run_batch(self, params: dict[str, Any]) -> dict[str, Any]:
        algorithm = self._algorithm_cls(
            address=params["address"],
            sampling_rate=params.get("sampling_rate"),
            input_parameters=params.get("input_parameters"),
        )
        algorithm.subject_id = params.get("subject_id")
        algorithm.location = params.get("location")
        self._emitted_results = []
        if hasattr(algorithm, "register_result_listener"):
            algorithm.register_result_listener(self._capture_result)
        else:
            algorithm.result_callback = self._capture_result
        if hasattr(algorithm, "register_compute_delegate"):
            algorithm.register_compute_delegate(lambda *_args, **_kwargs: False)
        else:
            algorithm.compute_delegate = lambda *_args, **_kwargs: False
        for sample_payload in params.get("samples") or []:
            algorithm.on_sample(_sample_from_payload(sample_payload))
        results = list(self._emitted_results)
        self._emitted_results = []
        return {"results": results}

    def shutdown(self) -> dict[str, Any]:
        self._instances.clear()
        return {"ok": True}

    def _capture_result(self, result: Any) -> None:
        if is_dataclass(result):
            payload = to_jsonable(result)
        else:
            payload = to_jsonable(object_to_mapping(result))
        self._emitted_results.append(payload)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--install-path", required=True)
    return parser.parse_args(argv)


def _emit(handle, payload: dict[str, Any]) -> None:
    handle.write(json.dumps(payload, sort_keys=True) + "\n")
    handle.flush()


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    rpc_stdout = sys.stdout
    sys.stdout = sys.stderr
    host = AlgorithmHost(Path(args.install_path).resolve())
    methods = {
        "describe": lambda _params: host.describe(),
        "healthcheck": lambda _params: host.healthcheck(),
        "start_algorithm": host.start_algorithm,
        "ingest_sample": host.ingest_sample,
        "should_run_intermediate": host.should_run_intermediate,
        "run_intermediate": host.run_intermediate,
        "run_consolidation": host.run_consolidation,
        "run_batch": host.run_batch,
        "shutdown": lambda _params: host.shutdown(),
    }
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        request = None
        try:
            request = json.loads(raw)
            method_name = request["method"]
            request_id = request["id"]
            params = request.get("params") or {}
            result = methods[method_name](params)
            _emit(rpc_stdout, {"jsonrpc": "2.0", "id": request_id, "result": result})
            if method_name == "shutdown":
                break
        except Exception as exc:
            request_id = request.get("id") if isinstance(request, dict) else None
            _emit(
                rpc_stdout,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32000, "message": str(exc)},
                },
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
