"""Filesystem layout helpers for the plugin root."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PluginLayout:
    root: Path

    @property
    def incoming_dir(self) -> Path:
        return self.root / "incoming"

    @property
    def installed_dir(self) -> Path:
        return self.root / "installed"

    @property
    def cache_dir(self) -> Path:
        return self.root / "cache"

    @property
    def catalog_dir(self) -> Path:
        return self.root / "catalog"

    @property
    def failed_dir(self) -> Path:
        return self.root / "failed"

    @property
    def plugins_index_path(self) -> Path:
        return self.catalog_dir / "plugins.json"

    @property
    def install_failures_path(self) -> Path:
        return self.catalog_dir / "install_failures.json"

    def plugin_dir(self, plugin_id: str) -> Path:
        return self.installed_dir / plugin_id

    def version_dir(self, plugin_id: str, version: str) -> Path:
        return self.plugin_dir(plugin_id) / version

    def current_link(self, plugin_id: str) -> Path:
        return self.plugin_dir(plugin_id) / "current"

    def previous_link(self, plugin_id: str) -> Path:
        return self.plugin_dir(plugin_id) / "previous"

    def plugin_catalog_path(self, plugin_id: str) -> Path:
        return self.catalog_dir / f"{plugin_id}.json"

    def ensure_base_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.incoming_dir.mkdir(parents=True, exist_ok=True)
        self.installed_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
