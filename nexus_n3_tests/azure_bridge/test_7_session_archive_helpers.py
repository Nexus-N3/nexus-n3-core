from pathlib import Path
import zipfile

from nexus_n3.file_manager.session_archive import (
    archive_session_directory,
    build_session_archive_name,
)


def test_build_archive_name_uses_canonical_session_id():
    archive_name = build_session_archive_name(
        session_name="Right_Step",
        session_timestamp="20260331_102030",
    )

    assert archive_name == "Right_Step_20260331_102030.zip"


def test_archive_session_directory_zips_directory_contents_and_removes_source(tmp_path: Path):
    session_dir = tmp_path / "sessions" / "Right_Step_20260331_102030"
    nested = session_dir / "subjects" / "subject1" / "activities" / "Right_Step" / "raw"
    nested.mkdir(parents=True)
    sample = nested / "LEFT_ANKLE_AABBCCDDEEFF.csv"
    sample.write_text("a,b\n1,2\n", encoding="utf-8")
    archive_path = tmp_path / "Right_Step_20260331_102030.zip"

    archive = archive_session_directory(session_dir, archive_path=archive_path, remove_source=True)

    assert archive.source_dir == session_dir.resolve()
    assert session_dir.exists() is False
    with zipfile.ZipFile(archive.archive_path, "r") as zipf:
        names = zipf.namelist()
        assert (
            "subjects/subject1/activities/Right_Step/raw/LEFT_ANKLE_AABBCCDDEEFF.csv"
            in names
        )
