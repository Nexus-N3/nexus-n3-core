from pathlib import Path
import zipfile

from nexus_n3.file_manager.session_archive import (
    archive_session_directory,
    build_session_archive_name,
)


def test_build_archive_name_uses_site_and_session_timestamp():
    archive_name = build_session_archive_name(
        site="local_home",
        session_label="Right_Step",
        session_timestamp="20260331_102030",
    )

    assert archive_name == "local_home_Right_Step_session_20260331_102030.zip"


def test_archive_session_directory_zips_directory_contents_and_removes_source(tmp_path: Path):
    session_dir = tmp_path / "session_20260331_102030"
    nested = session_dir / "subject1" / "run_20260331_102030" / "raw"
    nested.mkdir(parents=True)
    sample = nested / "LEFT_ANKLE_run_20260331_102030.csv"
    sample.write_text("a,b\n1,2\n", encoding="utf-8")
    archive_path = tmp_path / "local_home_session_20260331_102030.zip"

    archive = archive_session_directory(session_dir, archive_path=archive_path, remove_source=True)

    assert archive.source_dir == session_dir.resolve()
    assert session_dir.exists() is False
    with zipfile.ZipFile(archive.archive_path, "r") as zipf:
        names = zipf.namelist()
        assert "subject1/run_20260331_102030/raw/LEFT_ANKLE_run_20260331_102030.csv" in names
