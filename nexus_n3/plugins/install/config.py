"""Plugin root configuration helpers."""

from __future__ import annotations

import os
from pathlib import Path

from nexus_n3.core.runtime_env import load_runtime_env


DEFAULT_PLUGIN_ROOT = Path("/opt/nexus-n3-plugins")


def resolve_plugin_root(
    plugin_root: str | Path | None = None,
    *,
    env: dict[str, str] | None = None,
    config_root: str | Path | None = None,
) -> Path:
    """Resolve the plugin root using the documented precedence rules."""

    if plugin_root is not None:
        return Path(plugin_root).expanduser().resolve()

    load_runtime_env()
    env_map = os.environ if env is None else env
    env_root = env_map.get("NEXUS_N3_PLUGIN_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    if config_root is not None:
        return Path(config_root).expanduser().resolve()

    return DEFAULT_PLUGIN_ROOT


def resolve_system_site_packages(
    system_site_packages: bool | None = None,
    *,
    env: dict[str, str] | None = None,
) -> bool:
    """Resolve whether plugin runtimes may inherit base site-packages."""

    if system_site_packages is not None:
        return bool(system_site_packages)

    load_runtime_env()
    env_map = os.environ if env is None else env
    value = str(env_map.get("NEXUS_N3_PLUGIN_USE_SYSTEM_SITE_PACKAGES", "")).strip().lower()
    return value in {"1", "true", "yes", "on"}
