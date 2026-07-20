"""Developer workflow helpers for building and installing plugins from a local catalog."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from nexus_n3.core.runtime_env import load_runtime_env

from ..install.installer import PluginInstaller


@dataclass(frozen=True)
class PreparedPlugin:
    plugin_id: str
    plugin_root: Path
    bundle_path: Path
    installed: bool
    install_path: Path | None


@dataclass(frozen=True)
class DevBootstrapConfig:
    enabled: bool
    plugin_catalog_root: Path | None
    selected_plugins: list[str]


def prepare_dev_plugins(
    *,
    plugin_catalog_root: str | Path,
    plugin_root: str | Path | None = None,
    plugin_tooling_root: str | Path | None = None,
    build_root: str | Path | None = None,
    selected_plugins: list[str] | None = None,
    system_site_packages: bool | None = None,
) -> list[PreparedPlugin]:
    """Build `.rsnxplugin` bundles from mounted dev plugins and install them."""

    dev_root = Path(plugin_catalog_root).expanduser().resolve()
    tooling_root = _resolve_plugin_tooling_root(plugin_tooling_root)
    selected = {value.strip().lower() for value in (selected_plugins or []) if value and value.strip()}
    plugin_repos = _discover_plugin_repos(dev_root)
    installer = PluginInstaller(plugin_root, system_site_packages=system_site_packages)

    if build_root is None:
        build_root_path = Path(tempfile.mkdtemp(prefix="nexusn3-dev-plugin-build-"))
        cleanup_build_root = True
    else:
        build_root_path = Path(build_root).expanduser().resolve()
        build_root_path.mkdir(parents=True, exist_ok=True)
        cleanup_build_root = False

    prepared: list[PreparedPlugin] = []
    try:
        for repo in plugin_repos:
            manifest = _load_legacy_manifest(repo)
            plugin_id = str(manifest.get("plugin_id") or "").strip()
            repo_name = repo.name.strip().lower()
            if selected and plugin_id.lower() not in selected and repo_name not in selected:
                continue

            bundle_path = _build_bundle(
                plugin_repo=repo,
                plugin_tooling_root=tooling_root,
                output_dir=build_root_path / plugin_id,
            )
            manifest_payload = _read_bundle_manifest(bundle_path)
            version = str(manifest_payload["version"])
            install_path = installer.layout.version_dir(plugin_id, version)
            if install_path.exists():
                prepared.append(
                    PreparedPlugin(
                        plugin_id=plugin_id,
                        plugin_root=repo,
                        bundle_path=bundle_path,
                        installed=False,
                        install_path=install_path,
                    )
                )
                continue

            result = installer.install_bundle(bundle_path)
            prepared.append(
                PreparedPlugin(
                    plugin_id=plugin_id,
                    plugin_root=repo,
                    bundle_path=bundle_path,
                    installed=True,
                    install_path=result.install_path,
                )
            )
        return prepared
    finally:
        if cleanup_build_root:
            shutil.rmtree(build_root_path, ignore_errors=True)


def load_dev_bootstrap_config() -> DevBootstrapConfig:
    """Load developer bootstrap settings from the shared runtime env."""
    load_runtime_env()
    enabled = _env_flag("NEXUS_N3_BOOTSTRAP_PLUGINS", default=False)
    plugin_catalog_root_raw = os.environ.get("NEXUS_N3_PLUGIN_CATALOG_ROOT", "").strip()
    plugin_catalog_root = (
        Path(plugin_catalog_root_raw).expanduser().resolve() if plugin_catalog_root_raw else None
    )
    return DevBootstrapConfig(
        enabled=enabled,
        plugin_catalog_root=plugin_catalog_root,
        selected_plugins=_env_csv_list("NEXUS_N3_BOOTSTRAP_PLUGIN_LIST"),
    )


def prepare_dev_plugins_from_env(
    *,
    plugin_root: str | Path | None = None,
    plugin_tooling_root: str | Path | None = None,
    build_root: str | Path | None = None,
    system_site_packages: bool | None = None,
) -> list[PreparedPlugin]:
    """Build and install the runtime-env plugin list without starting the server."""

    config = load_dev_bootstrap_config()
    if config.plugin_catalog_root is None:
        raise ValueError("NEXUS_N3_PLUGIN_CATALOG_ROOT is not configured")
    if not config.selected_plugins:
        raise ValueError("NEXUS_N3_BOOTSTRAP_PLUGIN_LIST is empty")
    return prepare_dev_plugins(
        plugin_catalog_root=config.plugin_catalog_root,
        plugin_root=plugin_root,
        plugin_tooling_root=plugin_tooling_root,
        build_root=build_root,
        selected_plugins=config.selected_plugins,
        system_site_packages=system_site_packages,
    )


def _discover_plugin_repos(dev_root: Path) -> list[Path]:
    repos: list[Path] = []
    for category in ("sensors", "algorithms"):
        category_root = dev_root / category
        if not category_root.is_dir():
            continue
        for repo in sorted(category_root.iterdir()):
            if not repo.is_dir():
                continue
            if (repo / "plugin.json").is_file() and (repo / "pyproject.toml").is_file():
                repos.append(repo)
    return repos


def _resolve_plugin_tooling_root(plugin_tooling_root: str | Path | None) -> Path:
    if plugin_tooling_root is not None:
        candidate = Path(plugin_tooling_root).expanduser().resolve()
        if (candidate / "packages" / "cli" / "src").is_dir():
            return candidate
        raise FileNotFoundError(f"invalid plugin tooling root: {candidate}")

    candidate = Path(__file__).resolve().parents[3] / "nexus-n3-plugin-tooling"
    if (candidate / "packages" / "cli" / "src").is_dir():
        return candidate
    raise FileNotFoundError(
        "nexus-n3-plugin-tooling not found; pass --plugin-tooling-root or mount it in the container"
    )


def _build_bundle(*, plugin_repo: Path, plugin_tooling_root: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    cli_src = plugin_tooling_root / "packages" / "cli" / "src"
    sdk_src = plugin_tooling_root / "packages" / "sdk" / "src"
    env = os.environ.copy()
    extra_paths = [str(cli_src), str(sdk_src)]
    current = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = ":".join(extra_paths + ([current] if current else []))
    cmd = [
        sys.executable,
        "-m",
        "nexus_n3_plugin_cli.main",
        "build",
        "--plugin-root",
        str(plugin_repo),
        "--output-dir",
        str(output_dir),
        "--sdk-root",
        str(plugin_tooling_root / "packages" / "sdk"),
        "--force",
    ]
    subprocess.run(cmd, check=True, env=env)
    bundles = sorted(output_dir.glob("*.rsnxplugin"))
    if len(bundles) != 1:
        raise RuntimeError(f"expected one bundle for {plugin_repo}, found {len(bundles)}")
    return bundles[0]


def _load_legacy_manifest(plugin_repo: Path) -> dict[str, object]:
    return json.loads((plugin_repo / "plugin.json").read_text(encoding="utf-8"))


def _read_bundle_manifest(bundle_path: Path) -> dict[str, object]:
    with zipfile.ZipFile(bundle_path) as archive:
        return json.loads(archive.read("manifest.json").decode("utf-8"))


def _env_flag(name: str, default: bool = False) -> bool:
    value = str(os.environ.get(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _env_csv_list(name: str) -> list[str]:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]
