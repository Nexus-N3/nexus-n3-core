"""Safe, reusable access to completed Nexus N3 session archives."""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal


StorageSource = Literal["internal", "usb"]


class ArchiveServiceError(Exception):
    """Base class for archive service failures."""

    code = "archive_service_error"


class InvalidArchiveId(ArchiveServiceError):
    code = "invalid_archive_id"


class ArchiveNotFound(ArchiveServiceError):
    code = "archive_not_found"


class ArchiveSourceChanged(ArchiveServiceError):
    code = "archive_source_changed"


class ArchiveStorageUnavailable(ArchiveServiceError):
    code = "archive_storage_unavailable"


class ArchiveSiteChanged(ArchiveServiceError):
    code = "archive_site_changed"


@dataclass(frozen=True)
class ArchiveFile:
    archive_id: str
    filename: str
    path: Path
    size_bytes: int
    modified_at: str

    def public_dict(self) -> dict:
        return {
            "id": self.archive_id,
            "filename": self.filename,
            "size_bytes": self.size_bytes,
            "modified_at": self.modified_at,
        }


class ArchiveService:
    """Resolve the active output root and expose only root-level ZIP archives."""

    def __init__(
        self,
        internal_root: Path,
        status_provider: Callable[[], dict],
        site: str | None,
    ):
        self.internal_root = internal_root
        self.status_provider = status_provider
        self.site = str(site or "local").strip() or "local"
        self.site_directory = self._sanitize_site(self.site)

    def active_root(self) -> tuple[StorageSource, Path]:
        status = self.status_provider()
        usb_disk = status.get("usb_disk", {}) if isinstance(status, dict) else {}
        usb_path = usb_disk.get("path") if isinstance(usb_disk, dict) else None
        if usb_disk.get("present") and usb_path:
            candidate = Path(usb_path)
            if candidate.exists() and candidate.is_dir():
                return "usb", candidate
        try:
            self.internal_root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ArchiveStorageUnavailable("Archive storage is unavailable.") from exc
        return "internal", self.internal_root

    def archive_root(self, requested_site: str) -> tuple[StorageSource, Path]:
        """Return the canonical archive directory for the active runtime site."""
        if requested_site != self.site:
            raise ArchiveSiteChanged(
                f"Archive site changed from {requested_site!r} to {self.site!r}; refresh the list."
            )
        source, storage_root = self.active_root()
        root = storage_root / self.site_directory / "sessions"
        try:
            root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ArchiveStorageUnavailable("Archive storage is unavailable.") from exc
        return source, root

    def list_archives(self, requested_site: str) -> tuple[StorageSource, list[ArchiveFile]]:
        source, root = self.archive_root(requested_site)
        archives: list[ArchiveFile] = []
        try:
            entries = list(root.iterdir())
        except OSError as exc:
            raise ArchiveStorageUnavailable("Archive storage is unavailable.") from exc
        for path in entries:
            if path.suffix.lower() != ".zip" or path.is_symlink() or not path.is_file():
                continue
            resolved = path.resolve()
            if not resolved.is_relative_to(root.resolve()):
                continue
            stat = resolved.stat()
            modified_at = (
                datetime.fromtimestamp(stat.st_mtime, timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
            archives.append(
                ArchiveFile(
                    archive_id=self._encode_id(path.name),
                    filename=path.name,
                    path=resolved,
                    size_bytes=stat.st_size,
                    modified_at=modified_at,
                )
            )
        archives.sort(key=lambda item: (item.modified_at, item.filename), reverse=True)
        return source, archives

    def resolve_archive(
        self,
        archive_id: str,
        storage_source: str,
        requested_site: str,
    ) -> ArchiveFile:
        active_source, root = self.archive_root(requested_site)
        if storage_source not in {"internal", "usb"} or storage_source != active_source:
            raise ArchiveSourceChanged(
                f"Archive storage changed from {storage_source!r} to {active_source!r}; refresh the list."
            )

        filename = self._decode_id(archive_id)
        if Path(filename).name != filename or Path(filename).suffix.lower() != ".zip":
            raise InvalidArchiveId("Archive identifier is invalid.")

        candidate = root / filename
        resolved = candidate.resolve()
        if not resolved.is_relative_to(root.resolve()):
            raise InvalidArchiveId("Archive identifier is invalid.")
        if candidate.is_symlink() or not candidate.is_file() or not resolved.is_file():
            raise ArchiveNotFound("Archive was not found.")

        stat = resolved.stat()
        modified_at = (
            datetime.fromtimestamp(stat.st_mtime, timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        return ArchiveFile(
            archive_id=archive_id,
            filename=filename,
            path=resolved,
            size_bytes=stat.st_size,
            modified_at=modified_at,
        )

    @staticmethod
    def _encode_id(filename: str) -> str:
        return base64.urlsafe_b64encode(filename.encode("utf-8")).decode("ascii").rstrip("=")

    @staticmethod
    def _sanitize_site(site: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", site).strip(" .")
        if not cleaned or cleaned in {".", ".."}:
            return "local"
        reserved = {
            "CON",
            "PRN",
            "AUX",
            "NUL",
            *(f"COM{index}" for index in range(1, 10)),
            *(f"LPT{index}" for index in range(1, 10)),
        }
        if cleaned.split(".", 1)[0].upper() in reserved:
            return f"_{cleaned}"
        return cleaned

    @staticmethod
    def _decode_id(archive_id: str) -> str:
        if not archive_id or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for char in archive_id):
            raise InvalidArchiveId("Archive identifier is invalid.")
        try:
            padding = "=" * (-len(archive_id) % 4)
            decoded = base64.urlsafe_b64decode(archive_id + padding).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise InvalidArchiveId("Archive identifier is invalid.") from exc
        if ArchiveService._encode_id(decoded) != archive_id:
            raise InvalidArchiveId("Archive identifier is invalid.")
        return decoded
