"""Plugin installer."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import venv
from dataclasses import dataclass
from pathlib import Path

from ..common.jsonio import read_json, write_json
from .bundle import ValidatedBundle, extract_bundle, probe_manifest, validate_bundle
from .catalog import record_install_failure, refresh_plugins_index, update_plugin_catalog, utc_now
from .config import resolve_plugin_root, resolve_system_site_packages
from .layout import PluginLayout


class PluginInstallError(RuntimeError):
    """Raised when install fails."""


@dataclass(frozen=True)
class PluginInstallResult:
    plugin_id: str
    version: str
    plugin_root: Path
    install_path: Path
    runtime_path: Path
    activated: bool


@dataclass(frozen=True)
class PluginActivationResult:
    plugin_id: str
    version: str
    changed: bool


@dataclass(frozen=True)
class PluginPruneResult:
    plugin_id: str
    active_version: str
    removed_versions: tuple[str, ...]


class PluginInstaller:
    """Install .rsnxplugin bundles into the configured plugin root."""

    def __init__(
        self,
        plugin_root: str | Path | None = None,
        *,
        config_root: str | Path | None = None,
        system_site_packages: bool | None = None,
    ):
        resolved_root = resolve_plugin_root(plugin_root, config_root=config_root)
        self.layout = PluginLayout(resolved_root)
        self.layout.ensure_base_dirs()
        self.system_site_packages = resolve_system_site_packages(system_site_packages)

    def install_bundle(self, bundle_path: str | Path, *, activate: bool = True) -> PluginInstallResult:
        bundle_path = Path(bundle_path)
        try:
            bundle = validate_bundle(bundle_path)
            manifest = bundle.manifest
            plugin_id = manifest["plugin_id"]
            version = manifest["version"]
            install_dir = self.layout.version_dir(plugin_id, version)
            current_link = self.layout.current_link(plugin_id)
            previous_target = _safe_readlink(current_link)

            if install_dir.exists():
                catalog = read_json(self.layout.plugin_catalog_path(plugin_id), default={}) or {}
                if version in (catalog.get("versions") or {}):
                    raise PluginInstallError(f"plugin version already installed: {plugin_id} {version}")
                # A previous install may have failed after moving its staging
                # directory but before catalog persistence. Catalog state is
                # authoritative, so this directory is safe to recover.
                shutil.rmtree(install_dir)

            staging_dir = Path(tempfile.mkdtemp(prefix="plugin-install-", dir=self.layout.cache_dir))
            install_committed = False
            try:
                bundle_dir = staging_dir / "bundle"
                extract_bundle(bundle, bundle_dir)
                runtime_dir = staging_dir / "runtime"
                venv_dir = runtime_dir / ".venv"
                self._create_runtime(venv_dir)
                self._install_artifacts(venv_dir, bundle, bundle_dir)
                self._run_describe_check(venv_dir, manifest)
                self._run_healthcheck(venv_dir, manifest)

                install_dir.parent.mkdir(parents=True, exist_ok=True)
                staging_dir.replace(install_dir)
                install_committed = True
                shutil.copy2(install_dir / "bundle" / "manifest.json", install_dir / "manifest.json")

                install_metadata = {
                    "plugin_id": plugin_id,
                    "version": version,
                    "bundle_path": str(bundle.bundle_path),
                    "runtime_path": str(install_dir / "runtime" / ".venv"),
                    "state": "installed",
                    "activated": activate,
                }
                write_json(install_dir / "install.json", install_metadata)

                if activate:
                    self._activate(plugin_id, version, previous_target)
                update_plugin_catalog(
                    self.layout,
                    manifest,
                    version=version,
                    install_path=install_dir,
                    runtime_path=install_dir / "runtime" / ".venv",
                    state="installed",
                    enabled=True,
                    active=activate,
                )

                return PluginInstallResult(
                    plugin_id=plugin_id,
                    version=version,
                    plugin_root=self.layout.root,
                    install_path=install_dir,
                    runtime_path=install_dir / "runtime" / ".venv",
                    activated=activate,
                )
            except Exception as exc:
                record_install_failure(
                    self.layout,
                    bundle_path=bundle_path,
                    plugin_id=manifest.get("plugin_id"),
                    version=manifest.get("version"),
                    error=str(exc),
                )
                if staging_dir.exists():
                    shutil.rmtree(staging_dir, ignore_errors=True)
                if install_committed and install_dir.exists():
                    shutil.rmtree(install_dir, ignore_errors=True)
                raise PluginInstallError(str(exc)) from exc
        except Exception as exc:
            if isinstance(exc, PluginInstallError):
                raise
            manifest_hint = probe_manifest(bundle_path)
            record_install_failure(
                self.layout,
                bundle_path=bundle_path,
                plugin_id=locals().get("plugin_id") or manifest_hint.get("plugin_id"),
                version=locals().get("version") or manifest_hint.get("version"),
                error=str(exc),
            )
            raise PluginInstallError(str(exc)) from exc

    def activate_version(self, plugin_id: str, version: str) -> PluginActivationResult:
        """Activate one installed plugin version without reinstalling its bundle."""
        catalog_path = self.layout.plugin_catalog_path(plugin_id)
        catalog = read_json(catalog_path, default={}) or {}
        version_payload = (catalog.get("versions") or {}).get(version)
        install_dir = self.layout.version_dir(plugin_id, version)
        manifest_path = install_dir / "manifest.json"
        if not version_payload or not manifest_path.is_file():
            raise PluginInstallError(f"plugin version is not installed: {plugin_id} {version}")
        if catalog.get("active_version") == version:
            return PluginActivationResult(plugin_id=plugin_id, version=version, changed=False)

        manifest = read_json(manifest_path, default={}) or {}
        previous_target = _safe_readlink(self.layout.current_link(plugin_id))
        self._activate(plugin_id, version, previous_target)
        update_plugin_catalog(
            self.layout,
            manifest,
            version=version,
            install_path=install_dir,
            runtime_path=Path(version_payload["runtime_path"]),
            state="installed",
            enabled=True,
            active=True,
        )
        return PluginActivationResult(plugin_id=plugin_id, version=version, changed=True)

    def prune_inactive_versions(self, plugin_id: str, *, keep_version: str) -> PluginPruneResult:
        """Remove cataloged inactive versions after the desired version is active."""
        catalog_path = self.layout.plugin_catalog_path(plugin_id)
        catalog = read_json(catalog_path, default={}) or {}
        active_version = str(catalog.get("active_version") or "")
        if not active_version:
            raise PluginInstallError(f"plugin has no active version: {plugin_id}")
        if active_version != keep_version:
            raise PluginInstallError(
                f"refusing to prune {plugin_id}: active version is {active_version}, expected {keep_version}"
            )

        versions = catalog.get("versions") or {}
        if keep_version not in versions:
            raise PluginInstallError(f"active plugin version is missing from catalog: {plugin_id} {keep_version}")
        removed_versions = tuple(sorted(version for version in versions if version != keep_version))
        for version in removed_versions:
            install_dir = self.layout.version_dir(plugin_id, version)
            if install_dir.exists():
                shutil.rmtree(install_dir)
            versions.pop(version, None)

        previous_link = self.layout.previous_link(plugin_id)
        previous_target = _safe_readlink(previous_link)
        if previous_target is not None and previous_target.name in removed_versions:
            previous_link.unlink(missing_ok=True)

        if removed_versions:
            catalog["versions"] = versions
            catalog["updated_at"] = utc_now()
            write_json(catalog_path, catalog)
            refresh_plugins_index(self.layout)
        return PluginPruneResult(
            plugin_id=plugin_id,
            active_version=active_version,
            removed_versions=removed_versions,
        )

    def _create_runtime(self, venv_dir: Path) -> None:
        builder = venv.EnvBuilder(
            with_pip=True,
            clear=False,
            symlinks=sys.platform != "win32",
            system_site_packages=self.system_site_packages,
        )
        builder.create(venv_dir)

    def _install_artifacts(self, venv_dir: Path, bundle: ValidatedBundle, bundle_dir: Path) -> None:
        artifacts_dir = bundle_dir / "artifacts"
        artifact_paths = [bundle_dir / path for path in bundle.artifact_members]
        python_bin = _venv_python(venv_dir)
        cmd = [
            str(python_bin),
            "-m",
            "pip",
            "install",
            "--no-index",
            "--find-links",
            str(artifacts_dir),
            *[str(path) for path in artifact_paths],
        ]
        _run(cmd, timeout_seconds=300)

    def _run_describe_check(self, venv_dir: Path, manifest: dict) -> None:
        entrypoint = manifest["entrypoint"]
        module_name = entrypoint["module"]
        callable_name = entrypoint["callable"]
        code = (
            "import importlib\n"
            f"module = importlib.import_module({module_name!r})\n"
            f"getattr(module, {callable_name!r})\n"
        )
        _run([str(_venv_python(venv_dir)), "-c", code], timeout_seconds=30)

    def _run_healthcheck(self, venv_dir: Path, manifest: dict) -> None:
        print("Running plugin healthcheck...", manifest)

        healthcheck = manifest.get("healthcheck") or {}
        timeout_seconds = int(healthcheck.get("timeout_seconds", 30))
        module_name = healthcheck.get("module")
        callable_name = healthcheck.get("callable")
        command = healthcheck.get("command", "call")

        if not module_name or not callable_name:
            return

        if command == "import_entrypoint":
            code = (
                "import importlib\n"
                f"module = importlib.import_module({module_name!r})\n"
                f"getattr(module, {callable_name!r})\n"
            )

        elif command in {"call", "callable"}:
            code = (
                "import importlib\n"
                f"module = importlib.import_module({module_name!r})\n"
                f"callable_obj = getattr(module, {callable_name!r})\n"
                "result = callable_obj()\n"
                "if result is False:\n"
                "    raise SystemExit(1)\n"
            )

        else:
            raise PluginInstallError(f"unsupported healthcheck command: {command}")

        try:
            _run([str(_venv_python(venv_dir)), "-c", code], timeout_seconds=timeout_seconds)
        except PluginInstallError as exc:
            raise PluginInstallError(
                f"plugin healthcheck failed: "
                f"{module_name}.{callable_name} command={command}: {exc}"
            ) from exc

    def _activate(self, plugin_id: str, version: str, previous_target: Path | None) -> None:
        # Runtime discovery uses the persisted catalog's active_version. Windows
        # symlinks require privileges that normal developer shells often lack,
        # so the compatibility links are POSIX-only.
        if sys.platform == "win32":
            return

        plugin_dir = self.layout.plugin_dir(plugin_id)
        current_link = self.layout.current_link(plugin_id)
        previous_link = self.layout.previous_link(plugin_id)
        target_name = version

        if current_link.exists() or current_link.is_symlink():
            current_link.unlink()
        os.symlink(target_name, current_link)

        if previous_target is not None:
            if previous_link.exists() or previous_link.is_symlink():
                previous_link.unlink()
            os.symlink(previous_target.name, previous_link)


def _venv_python(venv_dir: Path) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _run(cmd: list[str], *, timeout_seconds: int) -> None:
    completed = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        output = completed.stderr.strip() or completed.stdout.strip()
        raise PluginInstallError(output or f"command failed: {' '.join(cmd)}")


def _safe_readlink(path: Path) -> Path | None:
    if not path.is_symlink():
        return None
    target = os.readlink(path)
    return (path.parent / target).resolve()
