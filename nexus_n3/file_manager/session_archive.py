"""Generic session archive helpers for local finalized session artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import zipfile


def _clean(value: str | None, fallback: str) -> str:
    raw = str(value or "").strip()
    cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw)
    return cleaned or fallback


def build_session_archive_name(
    *,
    site: str | None,
    session_label: str | None,
    session_timestamp: str | None,
) -> str:
    """Build a stable archive filename from site and session timestamp."""
    safe_site = _clean(site, "site")
    safe_label = _clean(session_label, "session")
    safe_session = _clean(session_timestamp, "unknown_session")
    return f"{safe_site}_{safe_label}_session_{safe_session}.zip"


@dataclass(slots=True)
class SessionArchiveResult:
    """Archive metadata for a finalized local session."""

    archive_path: Path
    archive_name: str
    source_dir: Path


def archive_session_directory(
    session_dir: str | Path,
    *,
    archive_path: str | Path,
    remove_source: bool = True,
) -> SessionArchiveResult:
    """Zip a session directory and optionally remove the original directory."""
    source_dir = Path(session_dir).resolve()
    target_archive = Path(archive_path).resolve()
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Session directory not found: {source_dir}")

    target_archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target_archive, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for file_path in source_dir.rglob("*"):
            if file_path.is_file():
                arcname = file_path.relative_to(source_dir)
                zipf.write(file_path, arcname)

    if remove_source:
        shutil.rmtree(source_dir)

    return SessionArchiveResult(
        archive_path=target_archive,
        archive_name=target_archive.name,
        source_dir=source_dir,
    )
