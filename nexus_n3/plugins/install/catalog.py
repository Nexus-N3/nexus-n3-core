"""Catalog persistence for installed plugin metadata."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ..common.jsonio import read_json, write_json
from .layout import PluginLayout


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def update_plugin_catalog(
    layout: PluginLayout,
    manifest: dict,
    *,
    version: str,
    install_path: Path,
    runtime_path: Path,
    state: str,
    enabled: bool,
    active: bool,
) -> None:
    plugin_id = manifest["plugin_id"]
    catalog_path = layout.plugin_catalog_path(plugin_id)
    catalog = read_json(catalog_path, default={}) or {}
    versions = catalog.setdefault("versions", {})
    versions[version] = {
        "version": version,
        "state": state,
        "enabled": enabled,
        "active": active,
        "install_path": str(install_path),
        "runtime_path": str(runtime_path),
        "runtime_protocol": manifest.get("runtime_protocol"),
        "capabilities": manifest.get("capabilities"),
        "display_name": manifest.get("display_name"),
        "plugin_type": manifest.get("plugin_type"),
        "updated_at": utc_now(),
    }
    for other_version, payload in versions.items():
        if other_version != version:
            payload["active"] = False
            if payload["state"] == "running":
                payload["state"] = "installed"

    catalog.update(
        {
            "plugin_id": plugin_id,
            "display_name": manifest.get("display_name"),
            "plugin_type": manifest.get("plugin_type"),
            "enabled": enabled,
            "active_version": version if active else catalog.get("active_version"),
            "runtime_protocol": manifest.get("runtime_protocol"),
            "min_nexus_n3_core_version": manifest.get("min_nexus_n3_core_version"),
            "sdk_version": manifest.get("sdk_version"),
            "install_root": str(layout.root),
            "updated_at": utc_now(),
        }
    )
    if active:
        catalog["active_version"] = version
    write_json(catalog_path, catalog)
    _update_plugins_index(layout)


def record_install_failure(
    layout: PluginLayout,
    *,
    bundle_path: Path,
    plugin_id: str | None,
    version: str | None,
    error: str,
) -> None:
    failures = read_json(layout.install_failures_path, default=[]) or []
    failures.append(
        {
            "bundle_path": str(bundle_path),
            "plugin_id": plugin_id,
            "version": version,
            "error": error,
            "recorded_at": utc_now(),
        }
    )
    write_json(layout.install_failures_path, failures)

    error_path = layout.incoming_dir / f"{bundle_path.name}.error.json"
    write_json(
        error_path,
        {
            "bundle_path": str(bundle_path),
            "plugin_id": plugin_id,
            "version": version,
            "error": error,
            "recorded_at": utc_now(),
        },
    )


def _update_plugins_index(layout: PluginLayout) -> None:
    plugins: list[dict] = []
    for path in sorted(layout.catalog_dir.glob("*.json")):
        if path.name == "plugins.json" or path.name == "install_failures.json":
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
    write_json(layout.plugins_index_path, {"plugins": plugins, "updated_at": utc_now()})
