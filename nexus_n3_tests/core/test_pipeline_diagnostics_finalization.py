import json
from pathlib import Path
import sys
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from nexus_n3.core.pipeline_diagnostics import PipelineDiagnostics
from nexus_n3.file_manager.FileManager import FileManager
from nexus_n3.file_manager.session_archive import archive_session_directory


def test_finished_pipeline_diagnostics_cannot_recreate_archived_session(tmp_path: Path):
    session_dir = tmp_path / "lunar" / "sessions" / "session_1"
    session_dir.mkdir(parents=True)
    (session_dir / "data.csv").write_text("sample", encoding="utf-8")
    diagnostics = PipelineDiagnostics(snapshot_interval_seconds=3600)
    diagnostics.set_enabled(True)
    diagnostics.start_session(
        session_dir,
        site="lunar",
        session_label="session",
        session_timestamp="1",
    )
    diagnostics.record_event("stream_stop_summary", status="ok")

    diagnostics.finish_session()
    archive_result = archive_session_directory(
        session_dir,
        archive_path=session_dir.with_suffix(".zip"),
        remove_source=True,
    )

    diagnostics.record_event("late_event")
    diagnostics._enqueue_snapshots()
    diagnostics.flush()

    assert archive_result.archive_path.is_file()
    assert not session_dir.exists()
    with zipfile.ZipFile(archive_result.archive_path) as archive:
        assert "data.csv" in archive.namelist()
        pipeline_records = [
            json.loads(line)
            for line in archive.read("diagnostics/pipeline_debug.ndjson").decode().splitlines()
        ]
    assert any(record["type"] == "stream_stop_summary" for record in pipeline_records)
    assert all(record["type"] != "late_event" for record in pipeline_records)


def test_finished_structured_diagnostics_ignore_late_events(tmp_path: Path):
    manager = FileManager("lunar", base_dir=tmp_path)
    manager.set_session_label("session")
    timestamp = "20260803_120000"
    manager.start_session_diagnostics(timestamp)
    session_dir = tmp_path / "lunar" / "sessions" / f"session_{timestamp}"
    drain_summary = {
        "scope": "all",
        "subject_ids": ["subject1"],
        "status": "ok",
        "all_local_streams_stopped": True,
    }
    manager.finalize_session_diagnostics(
        timestamp,
        status="ok",
        summary_updates={"drain_summary": drain_summary},
    )

    manager.finish_session_diagnostics(timestamp)
    archive_info = manager.archive_session(timestamp)
    manager.append_session_diagnostics_event(timestamp, "late_event", {"late": True})
    manager.update_session_diagnostics_summary(timestamp, {"late": True})

    archive_path = Path(archive_info["session_archive_path"])
    assert archive_path.is_file()
    assert not session_dir.exists()
    with zipfile.ZipFile(archive_path) as archive:
        summary = json.loads(archive.read("diagnostics/session_diagnostics.json"))
    assert summary["drain_summary"] == drain_summary
    assert "late" not in summary
