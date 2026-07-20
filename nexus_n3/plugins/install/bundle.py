"""Bundle validation and safe extraction."""

from __future__ import annotations

import hashlib
import shutil
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path

from .versions import version_gte


CURRENT_OS_VERSION = "0.0.7"


class PluginBundleError(RuntimeError):
    """Raised when a .rsnxplugin bundle is invalid."""


@dataclass(frozen=True)
class ValidatedBundle:
    bundle_path: Path
    manifest: dict
    checksums: dict[str, str]
    artifact_members: list[str]


def probe_manifest(bundle_path: Path) -> dict:
    """Best-effort manifest read for failure recording."""

    try:
        with zipfile.ZipFile(bundle_path) as archive:
            if "manifest.json" not in archive.namelist():
                return {}
            return _load_json_member(archive, "manifest.json")
    except Exception:
        return {}


REQUIRED_MANIFEST_FIELDS = {
    "plugin_id",
    "plugin_type",
    "display_name",
    "version",
    "sdk_version",
    "min_nexus_n3_core_version",
    "runtime_protocol",
    "entrypoint",
    "artifacts",
    "spec",
    "capabilities",
    "inputs",
    "outputs",
    "adapter_requirements",
    "permissions",
    "healthcheck",
}


def validate_bundle(bundle_path: Path) -> ValidatedBundle:
    bundle_path = bundle_path.resolve()
    if bundle_path.suffix != ".rsnxplugin":
        raise PluginBundleError("bundle must use the .rsnxplugin extension")

    if not bundle_path.exists():
        raise PluginBundleError(f"bundle does not exist: {bundle_path}")

    try:
        archive = zipfile.ZipFile(bundle_path)
    except zipfile.BadZipFile as exc:
        raise PluginBundleError(f"invalid ZIP archive: {exc}") from exc

    with archive:
        members = archive.infolist()
        _validate_member_names(members)
        member_names = {info.filename for info in members}
        if "manifest.json" not in member_names:
            raise PluginBundleError("manifest.json must exist at archive root")
        if "checksums.json" not in member_names:
            raise PluginBundleError("checksums.json must exist at archive root")

        manifest = _load_json_member(archive, "manifest.json")
        checksums = _load_json_member(archive, "checksums.json")
        _validate_manifest(manifest)
        _validate_checksums(archive, checksums)
        _validate_artifacts(manifest, checksums, member_names)

        min_version = manifest["min_nexus_n3_core_version"]
        if not version_gte(CURRENT_OS_VERSION, min_version):
            raise PluginBundleError(
                f"bundle requires nexus-n3-core>={min_version}, current={CURRENT_OS_VERSION}"
            )

        artifact_members = [artifact["path"] for artifact in manifest["artifacts"]]
        return ValidatedBundle(
            bundle_path=bundle_path,
            manifest=manifest,
            checksums=checksums,
            artifact_members=artifact_members,
        )


def extract_bundle(bundle: ValidatedBundle, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle.bundle_path) as archive:
        for info in archive.infolist():
            rel_path = Path(info.filename)
            destination = target_dir / rel_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            if info.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            with archive.open(info, "r") as source, destination.open("wb") as dest:
                shutil.copyfileobj(source, dest)


def _validate_member_names(members: list[zipfile.ZipInfo]) -> None:
    seen: set[str] = set()
    for info in members:
        name = info.filename
        if name in seen:
            raise PluginBundleError(f"duplicate archive entry: {name}")
        seen.add(name)
        pure = Path(name)
        if pure.is_absolute():
            raise PluginBundleError(f"absolute archive paths are not allowed: {name}")
        if any(part == ".." for part in pure.parts):
            raise PluginBundleError(f"archive path traversal is not allowed: {name}")

        mode = (info.external_attr >> 16) & 0xFFFF
        if stat.S_ISLNK(mode):
            raise PluginBundleError(f"symlink entries are not allowed: {name}")


def _load_json_member(archive: zipfile.ZipFile, name: str) -> dict:
    import json

    with archive.open(name, "r") as handle:
        return json.loads(handle.read().decode("utf-8"))


def _validate_manifest(manifest: dict) -> None:
    missing = sorted(REQUIRED_MANIFEST_FIELDS - set(manifest))
    if missing:
        raise PluginBundleError(f"manifest missing required fields: {', '.join(missing)}")

    if not isinstance(manifest["artifacts"], list) or not manifest["artifacts"]:
        raise PluginBundleError("manifest artifacts must be a non-empty list")


def _validate_checksums(archive: zipfile.ZipFile, checksums: dict[str, str]) -> None:
    if not isinstance(checksums, dict):
        raise PluginBundleError("checksums.json must be a JSON object")

    for info in archive.infolist():
        if info.is_dir():
            continue
        if info.filename == "checksums.json":
            continue
        expected = checksums.get(info.filename)
        if not expected:
            raise PluginBundleError(f"missing checksum for archive entry: {info.filename}")
        digest = hashlib.sha256()
        with archive.open(info, "r") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        actual = digest.hexdigest()
        if actual != expected:
            raise PluginBundleError(f"checksum mismatch for {info.filename}")


def _validate_artifacts(manifest: dict, checksums: dict[str, str], member_names: set[str]) -> None:
    for artifact in manifest["artifacts"]:
        artifact_path = artifact.get("path")
        if not artifact_path:
            raise PluginBundleError("artifact entry missing path")
        if artifact_path not in member_names:
            raise PluginBundleError(f"artifact not found in archive: {artifact_path}")
        declared_sha = artifact.get("sha256")
        actual_sha = checksums.get(artifact_path)
        if declared_sha and actual_sha and declared_sha != actual_sha:
            raise PluginBundleError(f"artifact sha256 mismatch for {artifact_path}")
