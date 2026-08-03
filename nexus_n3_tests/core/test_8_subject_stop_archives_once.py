from pathlib import Path
import sys
import threading
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from nexus_n3.core import core as core_module
from nexus_n3.core.core import Core
from nexus_n3.gateway.messaging import message_types as mt


class _StubEventBus:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def emit(self, event: dict) -> None:
        self.events.append(event)


class _StubFileManager:
    session_label = None
    session_name = None

    def __init__(self) -> None:
        self.archive_calls = 0
        self.describe_calls = 0
        self.finished_diagnostics = []
        self.finalized_summaries = []

    def flush(self) -> None:
        pass

    def update_session_diagnostics_summary(self, session_timestamp, updates) -> None:
        pass

    def finalize_session_diagnostics(self, session_timestamp, *, status: str, reason: str | None = None, summary_updates=None) -> None:
        self.finalized_summaries.append(summary_updates)

    def finish_session_diagnostics(self, session_timestamp) -> None:
        self.finished_diagnostics.append(session_timestamp)

    def get_raw_write_failures(self, subject_ids) -> dict:
        return {}

    def mark_subject_outputs_partial(self, subject_ids) -> dict:
        return {}

    def stop_stream(self, subject) -> None:
        pass

    def archive_session(self, session_timestamp: str | None) -> dict:
        self.archive_calls += 1
        return {
            "session_timestamp": session_timestamp,
            "session_archive_name": f"{session_timestamp}.zip",
            "session_archive_path": f"/tmp/{session_timestamp}.zip",
            "session_archive_exists": True,
        }

    def describe_session(self, session_timestamp: str | None) -> dict:
        self.describe_calls += 1
        return {
            "session_timestamp": session_timestamp,
            "session_dir": f"/tmp/session_{session_timestamp}",
            "session_dir_exists": True,
        }


class _StubStorage:
    def __init__(self, file_manager: _StubFileManager) -> None:
        self.file_manager = file_manager


class _StubSensorOrchestrator:
    def stop_specific(self, addresses) -> None:
        pass

    def stop_all(self) -> None:
        pass


class _StubComputeOrchestrator:
    def run_consolidation(self, subjects, file_manager) -> list[dict]:
        return []


def _make_subject(subject_id: str, address: str):
    return SimpleNamespace(
        subject_id=subject_id,
        is_streaming=True,
        sensors=[
            {
                "sensor": SimpleNamespace(address=address, raw_data=[]),
                "meta": {"file_path": None},
            }
        ],
    )


def _make_core(subjects) -> tuple[Core, _StubFileManager, _StubEventBus]:
    file_manager = _StubFileManager()
    event_bus = _StubEventBus()
    core = Core.__new__(Core)
    core.site = "local-test-site"
    core.system_event_bus = event_bus
    core.subjects = subjects
    core.storage = _StubStorage(file_manager)
    core.sensor_orch = _StubSensorOrchestrator()
    core.compute_orch = _StubComputeOrchestrator()
    core.active_subject_ids = None
    core.session_timestamp = "20260424_120000"
    core.archived_session_timestamp = None
    core.app_id = None
    core.app_name = None
    core.pending_correlation_id = None
    core._startup_lock = threading.RLock()
    core.stream_phase = "official_streaming"
    core._startup_manual_stop = False
    core._startup_retry_pending = False
    core._startup_gate_token = 0
    core._startup_subject_ids = []
    core._startup_addresses = []
    core._startup_stats_by_address = {}
    core._startup_attempt = 0
    core._pending_stream_tags = {}
    core._startup_last_failure_reason = None
    core._stream_stop_finalization_lock = threading.RLock()
    core._stream_stop_finalization_pending = False
    return core, file_manager, event_bus


def test_subject_stops_wait_for_last_subject_before_archiving(monkeypatch) -> None:
    monkeypatch.setattr(core_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(core_module.pipeline_diagnostics, "record_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(core_module.pipeline_diagnostics, "flush", lambda: None)

    subject_one = _make_subject("subject1", "A1")
    subject_two = _make_subject("subject2", "A2")
    core, file_manager, event_bus = _make_core([subject_one, subject_two])

    core._stop_stream_impl([subject_one], stop_specific=True)

    assert file_manager.archive_calls == 0
    assert file_manager.describe_calls >= 1
    assert subject_one.is_streaming is False
    assert subject_two.is_streaming is True
    assert event_bus.events[-1]["type"] == mt.EVT_STREAM_DRAINED
    assert event_bus.events[-1]["payload"]["all_local_streams_stopped"] is False

    core._stop_stream_impl([subject_two], stop_specific=True)

    assert file_manager.archive_calls == 1
    assert subject_two.is_streaming is False
    assert event_bus.events[-1]["type"] == mt.EVT_STREAM_DRAINED
    assert event_bus.events[-1]["payload"]["all_local_streams_stopped"] is True
    assert event_bus.events[-1]["payload"]["session_archive_path"] == "/tmp/20260424_120000.zip"
    assert file_manager.finalized_summaries[-1]["drain_summary"]["all_local_streams_stopped"] is True
    assert file_manager.finalized_summaries[-1]["drain_summary"]["status"] == "ok"


def test_single_subject_stop_archives_session_once(monkeypatch) -> None:
    monkeypatch.setattr(core_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(core_module.pipeline_diagnostics, "record_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(core_module.pipeline_diagnostics, "flush", lambda: None)

    subject = _make_subject("subject1", "A1")
    core, file_manager, event_bus = _make_core([subject])

    core._stop_stream_impl([subject], stop_specific=True)
    core._stop_stream_impl([subject], stop_specific=True)

    assert file_manager.archive_calls == 1
    assert event_bus.events[-1]["type"] == mt.EVT_STREAM_DRAINED
    assert event_bus.events[-1]["payload"]["all_local_streams_stopped"] is True


def test_disconnect_is_blocked_while_stream_finalization_is_pending() -> None:
    subject = _make_subject("subject1", "A1")
    core, _, _ = _make_core([subject])

    core._set_stream_stop_finalization_pending(True)

    try:
        core.disconnect_all()
        raise AssertionError("disconnect_all should be blocked while finalization is pending")
    except RuntimeError as exc:
        assert "finalization is still in progress" in str(exc)
