from __future__ import annotations

import base64
import hashlib
import json
import sys
import textwrap
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

OS_ROOT = Path(__file__).resolve().parents[2]
if str(OS_ROOT) not in sys.path:
    sys.path.insert(0, str(OS_ROOT))

from nexus_n3.core.orchestrators.compute_orchestrator import ComputeOrchestrator
from nexus_n3.plugins.install.installer import PluginInstaller


def test_phase3_host_backed_algorithm_runtime(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    bundle_path = _build_algorithm_bundle(tmp_path)
    PluginInstaller(plugin_root).install_bundle(bundle_path)

    orchestrator = ComputeOrchestrator(plugin_root=plugin_root)
    compute_results = []
    intermediate_results = []
    orchestrator.register_listeners(compute_results.append, intermediate_results.append)

    sensor = SimpleNamespace(address="sensor-1", attributes={"SAMPLING_RATE": 50})
    subjects = [
        SimpleNamespace(
            subject_id="subject-1",
            sensors=[
                {
                    "sensor": sensor,
                    "meta": {
                        "location": "CHEST",
                        "compute_algorithm": {
                            "name": "external_runtime_algo",
                            "inputs": {"scale": 2},
                        },
                    },
                }
            ],
        )
    ]

    orchestrator.register_algorithms(subjects)

    sample = SimpleNamespace(
        timestamp=1,
        address="sensor-1",
        sensor_type="mock",
        location="CHEST",
        sampling_rate=50,
        sample_type="mock",
        value=3,
    )
    orchestrator.ingest_sample(sample)

    deadline = time.time() + 3.0
    while time.time() < deadline and not compute_results:
        time.sleep(0.05)

    assert len(compute_results) == 1
    result = compute_results[0]
    assert result.address == "sensor-1"
    assert result.algorithm_name == "external_runtime_algo"
    assert result.value == 6

    buffered_results = orchestrator.compute_manager.get_results("external_runtime_algo")
    assert len(buffered_results) == 1
    deadline = time.time() + 3.0
    while time.time() < deadline and not intermediate_results:
        time.sleep(0.05)
    assert len(intermediate_results) == 1
    intermediate = intermediate_results[0]
    assert intermediate == {
        "algorithm_name": "external_runtime_algo",
        "stage": "intermediate_time",
        "results": [{"address": "sensor-1", "total": 6}],
    }

    consolidated = orchestrator.compute_manager.run_consolidation_for_subject(
        "subject-1",
        "external_runtime_algo",
        intermediate_records=[intermediate],
    )
    assert consolidated == {
        "algorithm_name": "external_runtime_algo",
        "stage": "consolidated_time",
        "results": [{"subject_id": "subject-1", "sum_total": 6}],
    }
    orchestrator.reset()


def _build_algorithm_bundle(tmp_path: Path) -> Path:
    wheel_path = _build_algorithm_wheel(tmp_path / "wheel-build")
    manifest = {
        "schema_version": 1,
        "plugin_id": "external-runtime-algo",
        "plugin_type": "algorithm",
        "display_name": "External Runtime Algo",
        "version": "1.0.0",
        "sdk_version": "0.1.0",
        "min_nexus_n3_core_version": "0.0.0",
        "runtime_protocol": {"name": "nexusn3-local-jsonrpc", "version": 1},
        "entrypoint": {"module": "external_runtime_algo.core", "callable": "ExternalRuntimeAlgorithm"},
        "artifacts": [
            {
                "type": "wheel",
                "path": f"artifacts/{wheel_path.name}",
                "sha256": _sha256_file(wheel_path),
            }
        ],
        "spec": {"type": "algorithm_config", "path": "metadata/algorithm_config.yaml"},
        "capabilities": {
            "algorithm_name": "external_runtime_algo",
            "supports_intermediate": True,
            "supports_consolidation": True,
            "executor_entry_points": {
                "intermediate": "external_runtime_algo.intermediate_executor:ExternalIntermediateExecutor",
                "consolidation": "external_runtime_algo.consolidation_executor:ExternalConsolidationExecutor",
            },
        },
        "inputs": [],
        "outputs": [],
        "adapter_requirements": {},
        "permissions": {},
        "healthcheck": {
            "command": "callable",
            "module": "external_runtime_algo.core",
            "callable": "healthcheck",
            "timeout_seconds": 10,
        },
    }
    metadata = textwrap.dedent(
        """
        algorithm:
          name: external_runtime_algo
        inputs:
          parameters:
            scale: 2
        """
    ).encode("utf-8")
    payloads = {
        "manifest.json": json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
        f"artifacts/{wheel_path.name}": wheel_path.read_bytes(),
        "metadata/algorithm_config.yaml": metadata,
    }
    checksums = {name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()}
    payloads["checksums.json"] = json.dumps(checksums, indent=2, sort_keys=True).encode("utf-8")
    bundle_path = tmp_path / "external-runtime-algo-1.0.0.rsnxplugin"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in payloads.items():
            archive.writestr(name, data)
    return bundle_path


def _build_algorithm_wheel(build_dir: Path) -> Path:
    build_dir.mkdir(parents=True, exist_ok=True)
    package_dir = build_dir / "external_runtime_algo"
    dist_info = build_dir / "external_runtime_algo-1.0.0.dist-info"
    package_dir.mkdir(parents=True, exist_ok=True)
    dist_info.mkdir(parents=True, exist_ok=True)

    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "core.py").write_text(
        textwrap.dedent(
            """
            from dataclasses import dataclass


            @dataclass
            class ExternalResult:
                address: str
                stage: str
                algorithm_name: str
                value: int
                subject_id: str | None = None
                location: str | None = None


            class ExternalRuntimeAlgorithm:
                name = "external_runtime_algo"

                def __init__(self, address, sampling_rate, input_parameters=None):
                    self.address = address
                    self.sampling_rate = sampling_rate
                    self.input_parameters = input_parameters or {}
                    self.result_callback = None
                    self.compute_delegate = None
                    self.subject_id = None
                    self.location = None

                def register_result_listener(self, callback):
                    self.result_callback = callback

                def register_compute_delegate(self, callback):
                    self.compute_delegate = callback

                def on_sample(self, sample):
                    result = ExternalResult(
                        address=self.address,
                        stage="real_time",
                        algorithm_name=self.name,
                        value=int(sample.value) * int(self.input_parameters.get("scale", 1)),
                        subject_id=self.subject_id,
                        location=self.location,
                    )
                    if self.result_callback:
                        self.result_callback(result)


            def healthcheck():
                return True
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (package_dir / "intermediate_executor.py").write_text(
        textwrap.dedent(
            """
            class ExternalIntermediateExecutor:
                def should_run(self, result_buffers):
                    return all(bool(items) for items in result_buffers.values())

                def run(self, result_buffers):
                    results = []
                    for address, items in result_buffers.items():
                        total = sum(int(item.value) for item in list(items))
                        results.append({"address": address, "total": total})
                    return {
                        "algorithm_name": "external_runtime_algo",
                        "stage": "intermediate_time",
                        "results": results,
                    }
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (package_dir / "consolidation_executor.py").write_text(
        textwrap.dedent(
            """
            class ExternalConsolidationExecutor:
                def consolidate(self, subject_id, intermediate_records):
                    total = 0
                    for record in intermediate_records:
                        for entry in record.get("results", []):
                            total += int(entry.get("total", 0))
                    return {
                        "algorithm_name": "external_runtime_algo",
                        "stage": "consolidated_time",
                        "results": [{"subject_id": subject_id, "sum_total": total}],
                    }
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (dist_info / "WHEEL").write_text(
        "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        encoding="utf-8",
    )
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: external-runtime-algo\nVersion: 1.0.0\n",
        encoding="utf-8",
    )

    records = []
    for path in sorted(build_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(build_dir).as_posix()
        digest = hashlib.sha256(path.read_bytes()).digest()
        records.append(f"{rel},sha256={_urlsafe_b64(digest)},{path.stat().st_size}")
    records.append("external_runtime_algo-1.0.0.dist-info/RECORD,,")
    (dist_info / "RECORD").write_text("\n".join(records) + "\n", encoding="utf-8")

    wheel_path = build_dir / "external_runtime_algo-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(build_dir.rglob("*")):
            if path.is_dir() or path == wheel_path:
                continue
            archive.write(path, path.relative_to(build_dir).as_posix())
    return wheel_path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _urlsafe_b64(digest: bytes) -> str:
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
