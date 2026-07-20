#!/usr/bin/env python3
"""Remove installed nexus-n3 plugin versions and catalog entries."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nexus_n3.plugins.common.jsonio import read_json, write_json
from nexus_n3.plugins.install.config import resolve_plugin_root
from nexus_n3.plugins.install.layout import PluginLayout


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="remove_plugin.py")
    parser.add_argument("plugin_id", help="Installed plugin id, for example movesense or pass-through")
    parser.add_argument(
        "--version",
        help="Optional installed version to remove. Omit to remove the whole plugin.",
    )
    parser.add_argument(
        "--plugin-root",
        help="Plugin root. Defaults to NEXUS_N3_PLUGIN_ROOT or /opt/nexus-n3-plugins.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    layout = PluginLayout(resolve_plugin_root(args.plugin_root))
    plugin_id = args.plugin_id.strip()
    catalog_path = layout.plugin_catalog_path(plugin_id)
    plugin_dir = layout.plugin_dir(plugin_id)

    catalog = read_json(catalog_path, default=None)
    if not catalog:
        print(f"Error: plugin catalog entry not found for '{plugin_id}'.")
        return 1

    versions = dict((catalog.get("versions") or {}))
    if not versions:
        print(f"Error: no installed versions recorded for '{plugin_id}'.")
        return 1

    if args.version:
        version = args.version.strip()
        if version not in versions:
            print(f"Error: version '{version}' not found for plugin '{plugin_id}'.")
            return 1
        install_path = Path(str((versions[version] or {}).get("install_path") or layout.version_dir(plugin_id, version)))
        prompt = f"Remove plugin '{plugin_id}' version '{version}' from {install_path}? [y/N]: "
        if not _confirm(prompt, assume_yes=args.yes):
            print("Cancelled.")
            return 1
        _remove_version(layout=layout, plugin_id=plugin_id, plugin_dir=plugin_dir, catalog=catalog, version=version)
        print(f"Removed plugin '{plugin_id}' version '{version}'.")
    else:
        prompt = f"Remove plugin '{plugin_id}' and all installed versions from {plugin_dir}? [y/N]: "
        if not _confirm(prompt, assume_yes=args.yes):
            print("Cancelled.")
            return 1
        _remove_plugin(layout=layout, plugin_id=plugin_id, plugin_dir=plugin_dir, catalog_path=catalog_path)
        print(f"Removed plugin '{plugin_id}'.")

    return 0


def _remove_version(
    *,
    layout: PluginLayout,
    plugin_id: str,
    plugin_dir: Path,
    catalog: dict,
    version: str,
) -> None:
    versions = dict((catalog.get("versions") or {}))
    version_payload = dict((versions.get(version) or {}))
    install_path = Path(str(version_payload.get("install_path") or layout.version_dir(plugin_id, version)))
    runtime_path = Path(str(version_payload.get("runtime_path") or "")) if version_payload.get("runtime_path") else None

    if install_path.exists():
        shutil.rmtree(install_path)

    versions.pop(version, None)
    current_link = layout.current_link(plugin_id)
    previous_link = layout.previous_link(plugin_id)
    active_version = catalog.get("active_version")

    if active_version == version:
        if current_link.exists() or current_link.is_symlink():
            current_link.unlink()
        replacement = _choose_replacement_version(versions)
        if replacement:
            os.symlink(replacement, current_link)
            versions[replacement]["active"] = True
            catalog["active_version"] = replacement
        else:
            catalog["active_version"] = None
    if previous_link.exists() or previous_link.is_symlink():
        target = os.readlink(previous_link) if previous_link.is_symlink() else None
        if target == version or not versions:
            previous_link.unlink()

    if not versions:
        _remove_plugin(
            layout=layout,
            plugin_id=plugin_id,
            plugin_dir=plugin_dir,
            catalog_path=layout.plugin_catalog_path(plugin_id),
            plugin_dir_already_pruned=True,
        )
        return

    for other_version, payload in versions.items():
        payload["active"] = other_version == catalog.get("active_version")
        install_root = Path(str(payload.get("install_path") or ""))
        if not install_root.exists():
            payload["state"] = "missing"
        elif payload.get("state") == "running":
            payload["state"] = "installed"
    catalog["versions"] = versions
    write_json(layout.plugin_catalog_path(plugin_id), catalog)
    _cleanup_empty_plugin_dir(plugin_dir)
    _refresh_plugins_index(layout)

    if runtime_path and runtime_path.exists() and install_path.exists():
        # Included for completeness; runtime_path should live under install_path and
        # therefore already be gone after the install root is removed.
        shutil.rmtree(runtime_path)


def _remove_plugin(
    *,
    layout: PluginLayout,
    plugin_id: str,
    plugin_dir: Path,
    catalog_path: Path,
    plugin_dir_already_pruned: bool = False,
) -> None:
    if not plugin_dir_already_pruned and plugin_dir.exists():
        shutil.rmtree(plugin_dir)
    elif plugin_dir.exists():
        _cleanup_empty_plugin_dir(plugin_dir)
    if catalog_path.exists():
        catalog_path.unlink()
    _refresh_plugins_index(layout)


def _choose_replacement_version(versions: dict[str, dict]) -> str | None:
    if not versions:
        return None
    return sorted(versions.keys())[-1]


def _cleanup_empty_plugin_dir(plugin_dir: Path) -> None:
    if plugin_dir.exists() and not any(plugin_dir.iterdir()):
        plugin_dir.rmdir()


def _refresh_plugins_index(layout: PluginLayout) -> None:
    plugins: list[dict] = []
    for path in sorted(layout.catalog_dir.glob("*.json")):
        if path.name in {"plugins.json", "install_failures.json"}:
            continue
        payload = read_json(path, default=None)
        if not payload:
            continue
        plugins.append(
            {
                "plugin_id": payload.get("plugin_id"),
                "display_name": payload.get("display_name"),
                "plugin_type": payload.get("plugin_type"),
                "enabled": payload.get("enabled"),
                "active_version": payload.get("active_version"),
                "runtime_protocol": payload.get("runtime_protocol"),
                "updated_at": payload.get("updated_at"),
            }
        )
    write_json(layout.plugins_index_path, {"plugins": plugins})


def _confirm(prompt: str, *, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    response = input(prompt).strip().lower()
    return response in {"y", "yes"}


if __name__ == "__main__":
    raise SystemExit(main())
