import os
from pathlib import Path

import pytest

from nexus_n3.admin.archive_service import (
    ArchiveNotFound,
    ArchiveService,
    ArchiveSourceChanged,
    ArchiveSiteChanged,
    InvalidArchiveId,
)


def _status(path: Path | None = None):
    return {
        "usb_disk": {
            "present": path is not None,
            "path": str(path) if path is not None else None,
        }
    }


def test_lists_only_root_zip_archives_newest_first(tmp_path):
    internal = tmp_path / "internal"
    archive_root = internal / "lunar" / "sessions"
    archive_root.mkdir(parents=True)
    older = archive_root / "older.zip"
    newer = archive_root / "newer.ZIP"
    older.write_bytes(b"old")
    newer.write_bytes(b"new")
    (archive_root / "notes.txt").write_text("ignore")
    (archive_root / "nested").mkdir()
    (archive_root / "nested" / "nested.zip").write_bytes(b"ignore")
    (archive_root / "linked.zip").symlink_to(archive_root / "nested" / "nested.zip")
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))

    source, archives = ArchiveService(internal, lambda: _status(), "lunar").list_archives("lunar")

    assert source == "internal"
    assert [item.filename for item in archives] == ["newer.ZIP", "older.zip"]
    assert all(str(internal) not in str(item.public_dict()) for item in archives)


def test_uses_usb_root_when_present(tmp_path):
    internal = tmp_path / "internal"
    usb = tmp_path / "usb"
    (internal / "lunar" / "sessions").mkdir(parents=True)
    (usb / "lunar" / "sessions").mkdir(parents=True)
    (internal / "lunar" / "sessions" / "internal.zip").write_bytes(b"internal")
    (usb / "lunar" / "sessions" / "usb.zip").write_bytes(b"usb")

    source, archives = ArchiveService(internal, lambda: _status(usb), "lunar").list_archives("lunar")

    assert source == "usb"
    assert [item.filename for item in archives] == ["usb.zip"]


def test_download_is_pinned_to_listed_storage_source(tmp_path):
    internal = tmp_path / "internal"
    usb = tmp_path / "usb"
    archive_root = internal / "lunar" / "sessions"
    archive_root.mkdir(parents=True)
    usb.mkdir()
    archive = archive_root / "session.zip"
    archive.write_bytes(b"archive")
    status = _status()
    service = ArchiveService(internal, lambda: status, "lunar")
    _, listed = service.list_archives("lunar")

    status.update(_status(usb))

    with pytest.raises(ArchiveSourceChanged):
        service.resolve_archive(listed[0].archive_id, "internal", "lunar")


def test_rejects_invalid_and_missing_archive_ids(tmp_path):
    internal = tmp_path / "internal"
    internal.mkdir()
    service = ArchiveService(internal, lambda: _status(), "lunar")

    with pytest.raises(InvalidArchiveId):
        service.resolve_archive("../session.zip", "internal", "lunar")

    missing_id = service._encode_id("missing.zip")
    with pytest.raises(ArchiveNotFound):
        service.resolve_archive(missing_id, "internal", "lunar")


def test_rejects_a_site_that_differs_from_the_active_runtime_site(tmp_path):
    service = ArchiveService(tmp_path / "internal", lambda: _status(), "new-site")

    with pytest.raises(ArchiveSiteChanged):
        service.list_archives("old-site")
