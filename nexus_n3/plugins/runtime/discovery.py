"""Support discovery for installed runtime plugins."""

from __future__ import annotations

from pathlib import Path

import yaml

try:
    from nexus_n3_algorithms.algorithms_registry import AlgorithmRegistry
    from nexus_n3_algorithms.yaml_loader import load_yaml as load_algorithm_yaml
except ModuleNotFoundError:
    AlgorithmRegistry = None
    load_algorithm_yaml = None

from ..common.jsonio import read_json
from ..install.config import resolve_plugin_root
from ..install.layout import PluginLayout


def get_installed_plugin_inventory(plugin_root: str | Path | None = None) -> dict:
    """Return a startup-friendly summary of enabled installed plugins."""
    resolved_root = resolve_plugin_root(plugin_root)
    sensors: list[dict] = []
    algorithms: list[dict] = []

    for plugin in _iter_installed_plugins(resolved_root):
        entry = {
            "plugin_id": plugin.get("plugin_id"),
            "display_name": _plugin_display_name(plugin),
            "version": plugin.get("version"),
        }
        if plugin.get("plugin_type") == "sensor":
            metadata = plugin.get("metadata") or {}
            sensor_name = ((metadata.get("sensor") or {}).get("type") or plugin.get("plugin_id"))
            sensors.append(
                {
                    **entry,
                    "sensor_name": sensor_name,
                }
            )
        elif plugin.get("plugin_type") == "algorithm":
            metadata = plugin.get("metadata") or {}
            algorithm_name = ((metadata.get("algorithm") or {}).get("name") or plugin.get("plugin_id"))
            algorithms.append(
                {
                    **entry,
                    "algorithm_name": algorithm_name,
                }
            )

    sensors.sort(key=lambda item: str(item.get("sensor_name") or item.get("display_name") or "").lower())
    algorithms.sort(key=lambda item: str(item.get("algorithm_name") or item.get("display_name") or "").lower())
    return {
        "plugin_root": str(resolved_root),
        "sensor_plugins": sensors,
        "algorithm_plugins": algorithms,
        "sensor_count": len(sensors),
        "algorithm_count": len(algorithms),
        "total_count": len(sensors) + len(algorithms),
    }


def get_supported_algorithms(plugin_root: str | Path | None = None) -> list[str]:
    """Return registered plus installed algorithm names."""
    algorithms = _registered_algorithm_inputs()
    for plugin in _iter_installed_plugins(plugin_root):
        if plugin.get("plugin_type") != "algorithm":
            continue
        config = plugin.get("metadata") or {}
        algorithm_name = (config.get("algorithm") or {}).get("name") or plugin["plugin_id"]
        algorithms[_normalize_name(algorithm_name)] = {
            "name": algorithm_name,
            "inputs": _algorithm_inputs_from_config(config),
        }
    return sorted((payload["name"] for payload in algorithms.values()), key=str.lower)


def get_supported_sensors(plugin_root: str | Path | None = None) -> list[dict]:
    """Return installed sensor support metadata."""
    algorithm_inputs = _registered_algorithm_inputs()
    sensors: dict[str, dict] = {}
    plugins = _iter_installed_plugins(plugin_root)

    for plugin in plugins:
        metadata = plugin.get("metadata") or {}
        if plugin.get("plugin_type") == "algorithm":
            algorithm_name = (metadata.get("algorithm") or {}).get("name") or plugin["plugin_id"]
            algorithm_inputs[_normalize_name(algorithm_name)] = {
                "name": algorithm_name,
                "inputs": _algorithm_inputs_from_config(metadata),
            }
    for plugin in plugins:
        metadata = plugin.get("metadata") or {}

        if plugin.get("plugin_type") != "sensor":
            continue

        sensor_section = metadata.get("sensor") or {}
        sensor_name = sensor_section.get("type") or plugin["plugin_id"]
        locations = list((metadata.get("locations") or {}).get("supported", []) or [])
        computations = list(metadata.get("computations", []) or [])
        sensors[_normalize_name(sensor_name)] = {
            "name": sensor_name,
            "locations": locations,
            "computations": _resolve_computations(computations, algorithm_inputs),
        }

    return sorted(sensors.values(), key=lambda item: item["name"].lower())


def _registered_algorithm_inputs() -> dict[str, dict]:
    algorithms: dict[str, dict] = {}
    if AlgorithmRegistry is None or load_algorithm_yaml is None:
        return algorithms
    for descriptor in AlgorithmRegistry().all_algorithms().values():
        inputs = {}
        try:
            config = load_algorithm_yaml(descriptor.algorithm_cls.yaml_path())
            inputs = _algorithm_inputs_from_config(config)
        except Exception:
            inputs = {}
        algorithms[_normalize_name(descriptor.name)] = {
            "name": descriptor.name,
            "inputs": inputs,
        }
    return algorithms


def _resolve_computations(computations: list[str], algorithm_inputs: dict[str, dict]) -> list[dict]:
    resolved = []
    for algorithm_name in computations:
        payload = algorithm_inputs.get(_normalize_name(str(algorithm_name)))
        if payload:
            resolved.append(
                {
                    "name": payload["name"],
                    "inputs": payload["inputs"],
                }
            )
            continue
        resolved.append(
            {
                "name": algorithm_name,
                "inputs": {},
            }
        )
    return resolved


def _iter_installed_plugins(plugin_root: str | Path | None = None) -> list[dict]:
    layout = PluginLayout(resolve_plugin_root(plugin_root))
    plugins: list[dict] = []
    for catalog_path in sorted(layout.catalog_dir.glob("*.json")):
        if catalog_path.name in {"plugins.json", "install_failures.json"}:
            continue
        catalog = read_json(catalog_path, default={}) or {}
        if not catalog.get("enabled", True):
            continue
        active_version = catalog.get("active_version")
        if not active_version:
            continue
        version_payload = (catalog.get("versions") or {}).get(active_version) or {}
        install_path_raw = version_payload.get("install_path")
        if not install_path_raw:
            continue
        install_path = Path(install_path_raw)
        manifest = _load_manifest(install_path)
        if not manifest:
            continue
        plugins.append(
            {
                "plugin_id": catalog.get("plugin_id") or manifest.get("plugin_id"),
                "plugin_type": catalog.get("plugin_type") or manifest.get("plugin_type"),
                "version": active_version,
                "install_path": install_path,
                "manifest": manifest,
                "metadata": _load_plugin_metadata(install_path, manifest),
            }
        )
    return plugins


def _load_manifest(install_path: Path) -> dict:
    manifest = read_json(install_path / "manifest.json", default=None)
    if manifest:
        return manifest
    return read_json(install_path / "bundle" / "manifest.json", default={}) or {}


def _load_plugin_metadata(install_path: Path, manifest: dict) -> dict:
    bundle_dir = install_path / "bundle"
    metadata_dir = bundle_dir / "metadata"
    for candidate in (
        metadata_dir / "sensor_spec.yaml",
        metadata_dir / "algorithm_config.yaml",
    ):
        if candidate.exists():
            return _read_yaml(candidate)

    spec_path = ((manifest.get("spec") or {}).get("path") or "").strip()
    if spec_path:
        candidate = bundle_dir / spec_path
        if candidate.exists():
            return _read_yaml(candidate)
        candidate = bundle_dir / Path(spec_path).name
        if candidate.exists():
            return _read_yaml(candidate)
    return {}


def _algorithm_inputs_from_config(config: dict) -> dict:
    return ((config.get("inputs") or {}).get("parameters") or {}).copy()


def _read_yaml(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload or {}


def _normalize_name(name: str) -> str:
    return str(name).strip().lower()


def _plugin_display_name(plugin: dict) -> str:
    manifest = plugin.get("manifest") or {}
    return (
        manifest.get("display_name")
        or plugin.get("plugin_id")
        or "unknown"
    )
