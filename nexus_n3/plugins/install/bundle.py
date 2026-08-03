"""Bundle validation and safe extraction."""

from __future__ import annotations

import hashlib
import platform
import re
import shutil
import stat
import sys
import sysconfig
import zipfile
from dataclasses import dataclass
from pathlib import Path

from .versions import version_gte


CURRENT_OS_VERSION = "0.1.3"


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
        _validate_bundle_target(manifest)

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


def _validate_bundle_target(manifest: dict) -> None:
    target = manifest.get("target")
    if not target:
        return
    if not isinstance(target, dict):
        raise PluginBundleError("manifest target must be a JSON object")

    target_id = str(target.get("id", "")).strip().lower()
    host_os = platform.system().lower()
    host_machine = platform.machine().lower()
    host_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    host_implementation = _normalize_python_implementation(sys.implementation.name)
    host_abi = _current_abi_tag()

    if target_id == "rpi":
        if host_os != "linux" or host_machine not in {"aarch64", "arm64", "armv7l"}:
            raise PluginBundleError(
                f"bundle target '{target_id}' is incompatible with host {host_os}/{host_machine}"
            )
    elif target_id == "jetson":
        if host_os != "linux" or host_machine not in {"aarch64", "arm64"}:
            raise PluginBundleError(
                f"bundle target '{target_id}' is incompatible with host {host_os}/{host_machine}"
            )
    elif target_id == "win":
        if host_os != "windows":
            raise PluginBundleError(
                f"bundle target '{target_id}' is incompatible with host {host_os}/{host_machine}"
            )

    expected_python = target.get("python_version")
    if expected_python and expected_python != host_python_version:
        raise PluginBundleError(
            f"bundle target python_version={expected_python} is incompatible with host python_version={host_python_version}"
        )

    expected_implementation = target.get("implementation")
    if expected_implementation and expected_implementation != host_implementation:
        raise PluginBundleError(
            f"bundle target implementation={expected_implementation} is incompatible with host implementation={host_implementation}"
        )

    expected_abi = target.get("abi")
    if expected_abi and host_abi and expected_abi != host_abi:
        raise PluginBundleError(
            f"bundle target abi={expected_abi} is incompatible with host abi={host_abi}"
        )


def _normalize_python_implementation(name: str) -> str:
    normalized = name.strip().lower()
    if normalized == "cpython":
        return "cp"
    return normalized


def _current_abi_tag() -> str | None:
    soabi = sysconfig.get_config_var("SOABI") or ""
    cpython_match = re.match(r"^cpython-(\d+)([a-z]*)", soabi)
    if cpython_match:
        return f"cp{cpython_match.group(1)}{cpython_match.group(2)}"
    for part in soabi.split("-"):
        if re.fullmatch(r"cp\d+[a-z]*", part):
            return part

    version = f"{sys.version_info.major}{sys.version_info.minor}"
    implementation = _normalize_python_implementation(sys.implementation.name)
    if implementation == "cp":
        return f"cp{version}"
    return None
