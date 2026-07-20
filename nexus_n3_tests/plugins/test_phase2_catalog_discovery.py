from __future__ import annotations

import hashlib
import json
import sys
import textwrap
import zipfile
from pathlib import Path

import pytest

OS_ROOT = Path(__file__).resolve().parents[2]
if str(OS_ROOT) not in sys.path:
    sys.path.insert(0, str(OS_ROOT))

from nexus_n3.plugins.install.installer import PluginInstaller
from nexus_n3.plugins.runtime.discovery import get_supported_algorithms, get_supported_sensors


def test_catalog_discovery_lists_active_installed_plugins(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    installer = PluginInstaller(plugin_root)

    algorithm_bundle = _build_bundle(
        tmp_path,
        plugin_name="external_algorithm_plugin",
        version="1.0.0",
        plugin_type="algorithm",
        metadata_name="metadata/algorithm_config.yaml",
        metadata_body=textwrap.dedent(
            """
            algorithm:
              name: external_loading
            inputs:
              parameters:
                window_seconds: 7
                mode: chest
            """
        ),
    )
    loading_bundle = _build_bundle(
        tmp_path,
        plugin_name="standard_loading_plugin",
        version="1.0.0",
        plugin_type="algorithm",
        metadata_name="metadata/algorithm_config.yaml",
        metadata_body=textwrap.dedent(
            """
            algorithm:
              name: standard_loading_intensity
            inputs:
              parameters:
                gravity: 9.80665
                gravity_option: earth
                gravity_options:
                  earth: 9.80665
                  moon: 1.62
                  mars: 3.721
                  zero_g: 0.0
            """
        ),
    )
    sensor_bundle = _build_bundle(
        tmp_path,
        plugin_name="external_sensor_plugin",
        version="1.0.0",
        plugin_type="sensor",
        metadata_name="metadata/sensor_spec.yaml",
        metadata_body=textwrap.dedent(
            """
            sensor:
              type: externalhr
            locations:
              supported:
                - CHEST
            computations:
              - external_loading
              - standard_loading_intensity
            """
        ),
    )

    installer.install_bundle(algorithm_bundle)
    installer.install_bundle(loading_bundle)
    installer.install_bundle(sensor_bundle)

    algorithms = get_supported_algorithms(plugin_root)
    assert "external_loading" in algorithms
    assert "standard_loading_intensity" in algorithms

    sensors = {item["name"]: item for item in get_supported_sensors(plugin_root)}
    assert "externalhr" in sensors
    assert sensors["externalhr"]["locations"] == ["CHEST"]
    assert sensors["externalhr"]["computations"] == [
        {
            "name": "external_loading",
            "inputs": {
                "window_seconds": 7,
                "mode": "chest",
            },
        },
        {
            "name": "standard_loading_intensity",
            "inputs": {
                "gravity": 9.80665,
                "gravity_option": "earth",
                "gravity_options": {
                    "earth": 9.80665,
                    "moon": 1.62,
                    "mars": 3.721,
                    "zero_g": 0.0,
                },
            },
        },
    ]


def test_catalog_discovery_skips_inactive_installed_plugins(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    installer = PluginInstaller(plugin_root)
    bundle = _build_bundle(
        tmp_path,
        plugin_name="inactive_sensor_plugin",
        version="1.0.0",
        plugin_type="sensor",
        metadata_name="metadata/sensor_spec.yaml",
        metadata_body=textwrap.dedent(
            """
            sensor:
              type: inactivehr
            locations:
              supported:
                - CHEST
            """
        ),
    )

    installer.install_bundle(bundle, activate=False)

    sensors = {item["name"]: item for item in get_supported_sensors(plugin_root)}
    assert "inactivehr" not in sensors


def _build_bundle(
    tmp_path: Path,
    *,
    plugin_name: str,
    version: str,
    plugin_type: str,
    metadata_name: str,
    metadata_body: str,
) -> Path:
    wheel_path = _build_minimal_wheel(
        tmp_path / f"wheel-{plugin_name}-{version}",
        distribution=plugin_name,
        version=version,
        package_name=plugin_name,
    )
    manifest = {
        "schema_version": 1,
        "plugin_id": plugin_name,
        "plugin_type": plugin_type,
        "display_name": plugin_name,
        "version": version,
        "sdk_version": "0.1.0",
        "min_nexus_n3_core_version": "0.0.0",
        "runtime_protocol": {"name": "nexusn3-local-jsonrpc", "version": 1},
        "entrypoint": {"module": plugin_name, "callable": "Plugin"},
        "artifacts": [
            {
                "type": "wheel",
                "path": f"artifacts/{wheel_path.name}",
                "sha256": _sha256_file(wheel_path),
            }
        ],
        "spec": {
            "type": "sensor_yaml" if plugin_type == "sensor" else "algorithm_config",
            "path": metadata_name,
        },
        "capabilities": {},
        "inputs": [],
        "outputs": [],
        "adapter_requirements": {},
        "permissions": {},
        "healthcheck": {
            "command": "callable",
            "module": plugin_name,
            "callable": "check",
            "timeout_seconds": 10,
        },
    }
    bundle_path = tmp_path / f"{plugin_name}-{version}.rsnxplugin"
    payloads = {
        "manifest.json": json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
        f"artifacts/{wheel_path.name}": wheel_path.read_bytes(),
        metadata_name: metadata_body.encode("utf-8"),
    }
    checksums = {name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()}
    payloads["checksums.json"] = json.dumps(checksums, indent=2, sort_keys=True).encode("utf-8")

    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in payloads.items():
            archive.writestr(name, data)
    return bundle_path


def _build_minimal_wheel(
    build_dir: Path,
    *,
    distribution: str,
    version: str,
    package_name: str,
) -> Path:
    build_dir.mkdir(parents=True, exist_ok=True)
    dist_info = build_dir / f"{distribution}-{version}.dist-info"
    package_dir = build_dir / package_name
    dist_info.mkdir(parents=True, exist_ok=True)
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text(
        textwrap.dedent(
            """
            class Plugin:
                pass

            def check():
                return True
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (dist_info / "WHEEL").write_text(
        "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        encoding="utf-8",
    )
    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {distribution}\nVersion: {version}\n",
        encoding="utf-8",
    )
    records = []
    for path in sorted(build_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(build_dir).as_posix()
        digest = hashlib.sha256(path.read_bytes()).digest()
        records.append(f"{rel},sha256={_urlsafe_b64(digest)},{path.stat().st_size}")
    records.append(f"{distribution}-{version}.dist-info/RECORD,,")
    (dist_info / "RECORD").write_text("\n".join(records) + "\n", encoding="utf-8")

    wheel_path = build_dir / f"{distribution}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(build_dir.rglob("*")):
            if path.is_dir() or path == wheel_path:
                continue
            archive.write(path, path.relative_to(build_dir).as_posix())
    return wheel_path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _urlsafe_b64(digest: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
