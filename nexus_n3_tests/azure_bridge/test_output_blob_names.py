from pathlib import Path

import pytest

from nexus_n3.azure_bridge.file_upload import build_output_blob_name


def test_output_blob_name_is_relative_and_uses_forward_slashes(tmp_path: Path):
    output_root = tmp_path / "nexus_n3_output"
    output_file = (
        output_root
        / "DLR_Cologne_Campus"
        / "sessions"
        / "sample_session_20260727_120626"
        / "subjects"
        / "subject1"
        / "activities"
        / "sample_session"
        / "computed"
        / "consolidated"
        / "standard_loading_intensity.ndjson"
    )

    blob_name = build_output_blob_name(output_file, output_root=output_root)

    assert blob_name == (
        "DLR_Cologne_Campus/sessions/sample_session_20260727_120626/"
        "subjects/subject1/activities/sample_session/computed/consolidated/"
        "standard_loading_intensity.ndjson"
    )
    assert "\\" not in blob_name
    assert ":" not in blob_name


def test_output_blob_name_rejects_files_outside_output_root(tmp_path: Path):
    with pytest.raises(ValueError, match="outside"):
        build_output_blob_name(
            tmp_path / "other" / "result.ndjson",
            output_root=tmp_path / "nexus_n3_output",
        )
