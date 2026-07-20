"""Install-time plugin support for nexus_n3.plugins."""

from .bundle import (
    CURRENT_OS_VERSION,
    PluginBundleError,
    ValidatedBundle,
    extract_bundle,
    probe_manifest,
    validate_bundle,
)
from .catalog import record_install_failure, update_plugin_catalog
from .config import DEFAULT_PLUGIN_ROOT, resolve_plugin_root
from .installer import PluginInstallError, PluginInstallResult, PluginInstaller
from .layout import PluginLayout
from .versions import normalize_version, version_gte

__all__ = [
    "CURRENT_OS_VERSION",
    "DEFAULT_PLUGIN_ROOT",
    "PluginBundleError",
    "PluginInstallError",
    "PluginInstallResult",
    "PluginInstaller",
    "PluginLayout",
    "ValidatedBundle",
    "extract_bundle",
    "normalize_version",
    "probe_manifest",
    "record_install_failure",
    "resolve_plugin_root",
    "update_plugin_catalog",
    "validate_bundle",
    "version_gte",
]
