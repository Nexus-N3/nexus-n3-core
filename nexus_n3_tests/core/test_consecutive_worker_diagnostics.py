"""Exercise retained Core/FileManager state with real files and shared archives."""

import importlib
import json
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import zipfile

import pytest

from nexus_n3.core import core as core_module
from nexus_n3.core.core import Core
from nexus_n3.core.pipeline_diagnostics import PipelineDiagnostics
from nexus_n3.core.subject import Subject
from nexus_n3.gateway.messaging import message_types as mt


@pytest.mark.parametrize("subject_scoped", [False, True])
def test_worker_diagnostics_survive_repeat_and_reconfigured_sessions(
    tmp_path, monkeypatch, subject_scoped
):
    # Each distributed node normally has its own process and diagnostic singleton.
    pipeline = PipelineDiagnostics(snapshot_interval_seconds=3600)
    pipeline.set_enabled(True)
    file_module = importlib.import_module("nexus_n3.file_manager.FileManager")
    monkeypatch.setattr(file_module, "pipeline_diagnostics", pipeline)
    monkeypatch.setattr(core_module, "pipeline_diagnostics", pipeline)
    monkeypatch.setattr(core_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(core_module, "SensorOrchestrator", lambda *a, **kw: Mock())
    monkeypatch.setattr(core_module, "ComputeOrchestrator", lambda *a, **kw: Mock())
    # Keep constructor output inside the test directory.
    monkeypatch.chdir(tmp_path)

    def configure(core, configs):
        subjects = []
        for subject_id in configs:
            subject = Subject(subject_id, [])
            for location in ("left", "right"):
                subject.add_sensor(
                    SimpleNamespace(address=f"{subject_id}-{location}", sampling_rate=60),
                    {"location": location, "compute_algorithm": {"name": "test"}},
                )
            subjects.append(subject)
        core.subject_graph.subjects = subjects
        core.subjects = subjects
        return True

    monkeypatch.setattr(Core, "_init_subjects", configure)
    worker = Core("test-site", node_id="worker-01", system_event_bus=Mock())
    master = Core("test-site", node_id="master", system_event_bus=Mock())
    worker.set_file_path(str(tmp_path / "shared"))
    master.set_file_path(str(tmp_path / "shared"))
    files = worker.storage.file_manager
    writer = files._session_diagnostics_writer_thread
    worker.init_core(["subject-1", "subject-2"], init_label="repeat")

    for run in (1, 2, 3):
        if run == 3:
            recovered = Future()
            recovered.set_result({"core_ready_for_configuration": True, "results": []})
            worker.sensor_orch.manager.recover_sensors.return_value = recovered
            worker.reset_session().result(timeout=2)
            worker.init_core(
                ["subject-1", "subject-2", "subject-3"], init_label="new session"
            )
        timestamp = f"20261019_12000{run}"
        master.storage.set_session_label(files.session_label)
        master.session_timestamp = timestamp
        master.archived_session_timestamp = None
        master.storage.file_manager.start_session_diagnostics(timestamp)
        master.storage.file_manager.append_session_diagnostics_event(timestamp, "master_event")
        payload = {"session_timestamp": timestamp, "official_start_mode": "coordinated"}
        if subject_scoped:
            worker.start_stream_for_subjects(
                {**payload, "subject_ids": [s.subject_id for s in worker.subjects]}
            )
        else:
            worker.start_stream(payload)
        worker._official_commit_received = True
        worker._activate_official_streaming()
        worker._official_outputs_active = True
        worker.stream_phase = "official_streaming"
        expected_outputs = []
        session_dir = Path(files.describe_session(timestamp)["session_dir"])
        for subject in worker.subjects:
            for entry in subject.sensors:
                sample = {"address": entry["sensor"].address, "timestamp": run, "value": run}
                files.enqueue_block(entry, [sample])
                files.write_computed_json(entry, {"stage": "real_time", "value": run})
                expected_outputs.extend(
                    entry["meta"][key].relative_to(session_dir).as_posix()
                    for key in ("file_path", "real_time_results_file_path")
                )
                worker._on_compute_performance({"address": entry["sensor"].address})
        worker._on_diagnostics({"run": run})
        stopped = Future()
        stopped.set_result([
            {"address": e["sensor"].address, "status": "stopped"}
            for s in worker.subjects for e in s.sensors
        ])
        worker.sensor_orch.stop_all.return_value = stopped
        worker.sensor_orch.stop_specific.return_value = stopped
        worker.compute_orch.run_consolidation.return_value = []
        context = {"stop_session_id": f"stop-{run}"}
        if subject_scoped:
            worker.stop_stream_for_subjects([s.subject_id for s in worker.subjects], context)
        else:
            worker.stop_stream(context)

        assert files is worker.storage.file_manager
        assert writer is files._session_diagnostics_writer_thread and writer.is_alive()
        assert files._session_diagnostics_state == {}
        assert timestamp in files._finalized_diagnostics_sessions
        archive_info = master.finalize_distributed_session(session_timestamp=timestamp)
        with zipfile.ZipFile(archive_info["session_archive_path"]) as archive:
            for output in expected_outputs:
                assert archive.read(output).strip()
            summary = json.loads(archive.read("worker-01-diagnostics/session_diagnostics.json"))
            assert summary["session_timestamp"] == timestamp
            assert summary["session_label"] == ("new_session" if run == 3 else "repeat")
            assert len(summary["subject_ids"]) == (3 if run == 3 else 2)
            assert summary["drain_summary"]["status"] == "ok"
            events = [json.loads(line) for line in archive.read(
                "worker-01-diagnostics/session_diagnostics.jsonl"
            ).splitlines()]
            diagnostic = next(e for e in events if e["type"] == mt.EVT_SENSOR_DIAGNOSTICS)
            assert diagnostic["payload"]["session_timestamp"] == timestamp
            assert diagnostic["payload"]["run"] == run
            assert sum(e["type"] == mt.EVT_COMPUTE_PERFORMANCE for e in events) == len(expected_outputs) // 2
            records = [json.loads(line) for line in archive.read(
                "worker-01-diagnostics/pipeline_debug.ndjson"
            ).splitlines()]
            assert all(r["session_timestamp"] == timestamp for r in records)
            assert any(r["type"] == "stream_stop_summary" for r in records)
            assert archive.read("master-diagnostics/session_diagnostics.jsonl")

        # Delayed callbacks from each finished run must not recreate the source.
        files.append_session_diagnostics_event(timestamp, "late_event")
        files.enqueue_session_diagnostics_event(timestamp, "late_queued_event")
        files._session_diagnostics_queue.join()
        pipeline.record_event("late_pipeline_event")
        pipeline.flush()
        assert not session_dir.exists()
