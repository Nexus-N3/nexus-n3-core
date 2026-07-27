from pathlib import Path
from types import SimpleNamespace
import zipfile

from nexus_n3.file_manager.FileManager import FileManager, pipeline_diagnostics


def _sensor_entry():
    return {
        "sensor": SimpleNamespace(address="AA:BB:CC:DD:EE:FF"),
        "meta": {
            "location": "LEFT ANKLE",
            "compute_algorithm": {"name": "standard loading/intensity"},
        },
    }


def test_canonical_session_paths_and_archive(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(pipeline_diagnostics, "start_session", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        pipeline_diagnostics, "register_sensor", lambda *args, **kwargs: None
    )

    manager = FileManager("DLR Cologne Campus", base_dir=tmp_path)
    manager.set_session_label("sample session")
    entry = _sensor_entry()
    subject = SimpleNamespace(subject_id="subject/1", sensors=[entry])
    timestamp = "20260727_120626"

    manager.start_stream(subject, timestamp, tag="sample session")
    manager.start_session_diagnostics(timestamp)

    session_dir = (
        tmp_path
        / "DLR_Cologne_Campus"
        / "sessions"
        / "sample_session_20260727_120626"
    )
    activity_dir = (
        session_dir
        / "subjects"
        / "subject_1"
        / "activities"
        / "sample_session"
    )
    raw_path = activity_dir / "raw" / "LEFT_ANKLE_AABBCCDDEEFF.csv"
    real_time_path = (
        activity_dir
        / "computed"
        / "real_time"
        / "standard_loading_intensity"
        / "LEFT_ANKLE_AABBCCDDEEFF.ndjson"
    )
    intermediate_path = (
        activity_dir
        / "computed"
        / "intermediate"
        / "standard_loading_intensity.ndjson"
    )
    consolidated_path = (
        activity_dir
        / "computed"
        / "consolidated"
        / "standard_loading_intensity.ndjson"
    )

    assert entry["meta"]["file_path"] == raw_path
    assert entry["meta"]["real_time_results_file_path"] == real_time_path
    assert entry["meta"]["consolidated_time_results_file_path"] == consolidated_path
    assert manager.get_subject_intermediate_path(
        "subject/1", "standard loading/intensity"
    ) == intermediate_path
    assert raw_path.is_file()
    assert real_time_path.is_file()
    assert intermediate_path.is_file()
    assert consolidated_path.is_file()
    assert (session_dir / "diagnostics" / "session_diagnostics.json").is_file()

    description = manager.describe_session(timestamp)
    assert description["session_id"] == "sample_session_20260727_120626"
    assert description["session_dir"] == str(session_dir.resolve())
    assert description["session_relative_path"] == (
        "DLR_Cologne_Campus/sessions/sample_session_20260727_120626"
    )

    archive_info = manager.archive_session(timestamp)
    assert archive_info["session_archive_name"] == "sample_session_20260727_120626.zip"
    assert not session_dir.exists()
    with zipfile.ZipFile(archive_info["session_archive_path"]) as archive:
        assert (
            "subjects/subject_1/activities/sample_session/computed/"
            "intermediate/standard_loading_intensity.ndjson"
        ) in archive.namelist()


def test_representative_windows_paths_stay_below_legacy_limit():
    manager = FileManager.__new__(FileManager)
    manager.site = "DLR Cologne Campus"
    manager.base_root = Path(
        r"C:\Users\Mike\Desktop\The Nexus Project\nexus-n3-core\nexus_n3_output"
    )
    manager.session_name = "sample_session"
    manager.base_dir = manager._resolve_base_dir()

    paths = [
        manager._build_raw_path(
            "20260727_120626",
            "subject1",
            "sample_session",
            "LEFT_ANKLE",
            "D4:22:CD:00:AA:6F",
        ),
        manager._build_real_time_path(
            "20260727_120626",
            "subject1",
            "sample_session",
            "standard_loading_intensity",
            "LEFT_ANKLE",
            "D4:22:CD:00:AA:6F",
        ),
        manager._build_intermediate_path(
            "20260727_120626",
            "subject1",
            "sample_session",
            "standard_loading_intensity",
        ),
        manager._build_consolidated_path(
            "20260727_120626",
            "subject1",
            "sample_session",
            "standard_loading_intensity",
        ),
    ]

    assert max(len(str(path.absolute())) for path in paths) < 250


def test_windows_reserved_component_is_made_safe():
    manager = FileManager.__new__(FileManager)

    assert manager._sanitize_component("CON") == "_CON"
    assert manager._sanitize_component("..", "fallback") == "fallback"
