from __future__ import annotations

import hashlib
import json
import os
import sys
import textwrap
import zipfile
from pathlib import Path

import pytest

OS_ROOT = Path(__file__).resolve().parents[2]
if str(OS_ROOT) not in sys.path:
    sys.path.insert(0, str(OS_ROOT))

from nexus_n3.plugins.common.jsonio import read_json
from nexus_n3.plugins.install.config import (
    DEFAULT_PLUGIN_ROOT,
    resolve_plugin_root,
    resolve_system_site_packages,
)
from nexus_n3.plugins.install.installer import PluginInstallError, PluginInstaller
from nexus_n3.plugins.install import installer as installer_module
from nexus_n3.plugins.install import bundle as bundle_module


def test_resolve_plugin_root_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env_root = tmp_path / "env-root"
    explicit_root = tmp_path / "explicit-root"
    config_root = tmp_path / "config-root"
    monkeypatch.setenv("NEXUS_N3_PLUGIN_ROOT", str(env_root))

    assert resolve_plugin_root(explicit_root, config_root=config_root) == explicit_root.resolve()
    assert resolve_plugin_root(config_root=config_root) == env_root.resolve()

    monkeypatch.delenv("NEXUS_N3_PLUGIN_ROOT")
    assert resolve_plugin_root(config_root=config_root) == config_root.resolve()
    assert resolve_plugin_root() == DEFAULT_PLUGIN_ROOT


def test_installer_uses_explicit_plugin_root(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    bundle_path = _build_fixture_bundle(tmp_path, plugin_name="demo_sensor", version="1.0.0")

    result = PluginInstaller(plugin_root).install_bundle(bundle_path)

    assert result.plugin_root == plugin_root.resolve()
    assert result.install_path == plugin_root.resolve() / "installed" / "demo_sensor" / "1.0.0"
    assert (plugin_root / "catalog" / "demo_sensor.json").exists()
    current_link = plugin_root / "installed" / "demo_sensor" / "current"
    if sys.platform == "win32":
        assert not current_link.exists()
    else:
        assert os.readlink(current_link) == "1.0.0"


def test_installer_uses_env_plugin_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    plugin_root = tmp_path / "env-plugins"
    bundle_path = _build_fixture_bundle(tmp_path, plugin_name="demo_sensor_env", version="1.0.0")
    monkeypatch.setenv("NEXUS_N3_PLUGIN_ROOT", str(plugin_root))

    result = PluginInstaller().install_bundle(bundle_path)

    assert result.plugin_root == plugin_root.resolve()
    assert result.install_path.parent.parent == plugin_root.resolve() / "installed"


def test_resolve_system_site_packages_precedence(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NEXUS_N3_PLUGIN_USE_SYSTEM_SITE_PACKAGES", "true")
    assert resolve_system_site_packages() is True
    assert resolve_system_site_packages(False) is False

    monkeypatch.setenv("NEXUS_N3_PLUGIN_USE_SYSTEM_SITE_PACKAGES", "0")
    assert resolve_system_site_packages() is False


def test_windows_runtime_venv_uses_copies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    calls = {}

    class FakeBuilder:
        def __init__(self, **kwargs):
            calls["kwargs"] = kwargs

        def create(self, path):
            calls["path"] = path

    monkeypatch.setattr(installer_module.sys, "platform", "win32")
    monkeypatch.setattr(installer_module.venv, "EnvBuilder", FakeBuilder)

    installer = PluginInstaller(tmp_path / "plugins")
    venv_dir = tmp_path / "runtime" / ".venv"
    installer._create_runtime(venv_dir)

    assert calls["kwargs"]["symlinks"] is False
    assert calls["path"] == venv_dir


def test_windows_activation_relies_on_catalog_without_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(installer_module.sys, "platform", "win32")
    monkeypatch.setattr(
        installer_module.os,
        "symlink",
        lambda *_args, **_kwargs: pytest.fail("Windows activation must not create a symlink"),
    )

    installer = PluginInstaller(tmp_path / "plugins")
    installer._activate("demo_sensor", "1.0.0", None)

    assert not (tmp_path / "plugins" / "installed" / "demo_sensor" / "current").exists()


def test_rejects_absolute_archive_paths(tmp_path: Path):
    bundle_path = tmp_path / "bad.rsnxplugin"
    _write_bundle_with_members(
        bundle_path,
        {
            "/absolute.txt": b"x",
            "manifest.json": b"{}",
            "checksums.json": b"{}",
        },
    )
    with pytest.raises(PluginInstallError, match="absolute archive paths"):
        PluginInstaller(tmp_path / "plugins").install_bundle(bundle_path)


def test_rejects_dotdot_archive_paths(tmp_path: Path):
    bundle_path = tmp_path / "bad2.rsnxplugin"
    _write_bundle_with_members(
        bundle_path,
        {
            "../escape.txt": b"x",
            "manifest.json": b"{}",
            "checksums.json": b"{}",
        },
    )
    with pytest.raises(PluginInstallError, match="path traversal"):
        PluginInstaller(tmp_path / "plugins").install_bundle(bundle_path)


def test_rejects_duplicate_entries(tmp_path: Path):
    bundle_path = tmp_path / "dupe.rsnxplugin"
    with zipfile.ZipFile(bundle_path, "w") as archive:
        archive.writestr("manifest.json", "{}")
        archive.writestr("manifest.json", "{}")
        archive.writestr("checksums.json", "{}")
    with pytest.raises(PluginInstallError, match="duplicate archive entry"):
        PluginInstaller(tmp_path / "plugins").install_bundle(bundle_path)


def test_rejects_symlink_entries(tmp_path: Path):
    bundle_path = tmp_path / "symlink.rsnxplugin"
    info = zipfile.ZipInfo("link")
    info.create_system = 3
    info.external_attr = 0o120777 << 16
    with zipfile.ZipFile(bundle_path, "w") as archive:
        archive.writestr(info, "target")
        archive.writestr("manifest.json", "{}")
        archive.writestr("checksums.json", "{}")
    with pytest.raises(PluginInstallError, match="symlink entries"):
        PluginInstaller(tmp_path / "plugins").install_bundle(bundle_path)


def test_failed_install_does_not_replace_current(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    installer = PluginInstaller(plugin_root)
    good_bundle = _build_fixture_bundle(tmp_path, plugin_name="demo_sensor", version="1.0.0")
    installer.install_bundle(good_bundle)

    bad_bundle = _build_fixture_bundle(
        tmp_path,
        plugin_name="demo_sensor",
        version="2.0.0",
        corrupt_checksum=True,
    )

    with pytest.raises(PluginInstallError):
        installer.install_bundle(bad_bundle)

    current_link = plugin_root / "installed" / "demo_sensor" / "current"
    if sys.platform == "win32":
        assert not current_link.exists()
    else:
        assert os.readlink(current_link) == "1.0.0"
    catalog = read_json(plugin_root / "catalog" / "demo_sensor.json", default={})
    assert catalog["active_version"] == "1.0.0"
    assert not (plugin_root / "installed" / "demo_sensor" / "2.0.0").exists()
    failures = read_json(plugin_root / "catalog" / "install_failures.json", default=[])
    assert failures
    assert failures[-1]["plugin_id"] == "demo_sensor"
    assert (plugin_root / "incoming" / f"{bad_bundle.name}.error.json").exists()


def test_uncataloged_version_directory_is_recovered(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    orphan_dir = plugin_root / "installed" / "demo_sensor" / "1.0.0"
    orphan_dir.mkdir(parents=True)
    orphan_dir.joinpath("orphan.txt").write_text("failed install", encoding="utf-8")
    bundle_path = _build_fixture_bundle(
        tmp_path,
        plugin_name="demo_sensor",
        version="1.0.0",
    )

    result = PluginInstaller(plugin_root).install_bundle(bundle_path)

    assert result.install_path == orphan_dir
    assert not orphan_dir.joinpath("orphan.txt").exists()
    catalog = read_json(plugin_root / "catalog" / "demo_sensor.json", default={})
    assert catalog["active_version"] == "1.0.0"


def test_activate_and_prune_superseded_plugin_versions(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    installer = PluginInstaller(plugin_root)
    version_one = _build_fixture_bundle(tmp_path, plugin_name="demo_sensor", version="1.0.0")
    version_two = _build_fixture_bundle(tmp_path, plugin_name="demo_sensor", version="2.0.0")

    installer.install_bundle(version_two)
    installer.install_bundle(version_one)
    assert read_json(plugin_root / "catalog" / "demo_sensor.json", default={})["active_version"] == "1.0.0"

    activation = installer.activate_version("demo_sensor", "2.0.0")
    assert activation.changed is True
    assert installer.activate_version("demo_sensor", "2.0.0").changed is False

    result = installer.prune_inactive_versions("demo_sensor", keep_version="2.0.0")
    assert result.removed_versions == ("1.0.0",)
    assert not (plugin_root / "installed" / "demo_sensor" / "1.0.0").exists()
    assert (plugin_root / "installed" / "demo_sensor" / "2.0.0").is_dir()
    catalog = read_json(plugin_root / "catalog" / "demo_sensor.json", default={})
    assert catalog["active_version"] == "2.0.0"
    assert list(catalog["versions"]) == ["2.0.0"]
    if sys.platform != "win32":
        assert os.readlink(plugin_root / "installed" / "demo_sensor" / "current") == "2.0.0"
        assert not (plugin_root / "installed" / "demo_sensor" / "previous").exists()

    assert installer.prune_inactive_versions(
        "demo_sensor", keep_version="2.0.0"
    ).removed_versions == ()


def test_prune_refuses_to_remove_the_active_plugin_version(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    installer = PluginInstaller(plugin_root)
    version_one = _build_fixture_bundle(tmp_path, plugin_name="demo_sensor", version="1.0.0")
    version_two = _build_fixture_bundle(tmp_path, plugin_name="demo_sensor", version="2.0.0")
    installer.install_bundle(version_one)
    installer.install_bundle(version_two)

    with pytest.raises(PluginInstallError, match="active version is 2.0.0"):
        installer.prune_inactive_versions("demo_sensor", keep_version="1.0.0")

    assert (plugin_root / "installed" / "demo_sensor" / "1.0.0").is_dir()
    assert (plugin_root / "installed" / "demo_sensor" / "2.0.0").is_dir()


def test_rejects_incompatible_bundle_target(tmp_path: Path):
    plugin_root = tmp_path / "plugins"
    bundle_path = _build_fixture_bundle(
        tmp_path,
        plugin_name="demo_sensor_target",
        version="1.0.0",
        target={"id": "incompatible", "python_version": "0.0", "implementation": "cp", "abi": "cp00"},
    )

    with pytest.raises(PluginInstallError, match="bundle target"):
        PluginInstaller(plugin_root).install_bundle(bundle_path)


def test_cpython_soabi_is_normalized_to_a_wheel_abi_tag(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        bundle_module.sysconfig,
        "get_config_var",
        lambda name: "cpython-312-aarch64-linux-gnu" if name == "SOABI" else None,
    )

    assert bundle_module._current_abi_tag() == "cp312"

def _build_fixture_bundle(
    tmp_path: Path,
    *,
    plugin_name: str,
    version: str,
    corrupt_checksum: bool = False,
    target: dict | None = None,
) -> Path:
    wheel_path = _build_minimal_sdkless_wheel(
        tmp_path / f"wheel-{plugin_name}-{version}",
        distribution=plugin_name,
        version=version,
        package_name=plugin_name,
        module_body=textwrap.dedent(
            """
            class Plugin:
                pass

            def check():
                return True
            """
        ),
    )
    manifest = {
        "schema_version": 1,
        "plugin_id": plugin_name,
        "plugin_type": "sensor",
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
        "spec": {"type": "sensor_yaml", "path": f"{plugin_name}/Spec.yaml"},
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
    if target is not None:
        manifest["target"] = target
    bundle_path = tmp_path / f"{plugin_name}-{version}.rsnxplugin"
    _write_bundle(bundle_path, manifest, {"artifacts/" + wheel_path.name: wheel_path}, corrupt_checksum=corrupt_checksum)
    return bundle_path


def _write_bundle(bundle_path: Path, manifest: dict, artifacts: dict[str, Path], *, corrupt_checksum: bool = False):
    payloads: dict[str, bytes] = {}
    payloads["manifest.json"] = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
    for archive_name, source_path in artifacts.items():
        payloads[archive_name] = source_path.read_bytes()
    checksums = {name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()}
    if corrupt_checksum:
        first_key = next(iter(artifacts))
        checksums[first_key] = "0" * 64
    payloads["checksums.json"] = json.dumps(checksums, indent=2, sort_keys=True).encode("utf-8")

    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in payloads.items():
            archive.writestr(name, data)


def _write_bundle_with_members(bundle_path: Path, members: dict[str, bytes]):
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)


def _build_minimal_sdkless_wheel(
    build_dir: Path,
    *,
    distribution: str,
    version: str,
    package_name: str,
    module_body: str,
) -> Path:
    build_dir.mkdir(parents=True, exist_ok=True)
    wheel_name = f"{distribution}-{version}-py3-none-any.whl"
    wheel_path = build_dir / wheel_name

    dist_info = f"{distribution}-{version}.dist-info"
    records: list[str] = []
    files = {
        f"{package_name}/__init__.py": module_body.encode("utf-8"),
        f"{dist_info}/WHEEL": b"Wheel-Version: 1.0\nGenerator: nexus-n3-tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{dist_info}/METADATA": (
            f"Metadata-Version: 2.1\nName: {distribution}\nVersion: {version}\nSummary: Test wheel\n".encode("utf-8")
        ),
    }

    for name, data in list(files.items()):
        digest = hashlib.sha256(data).digest()
        b64 = __import__("base64").urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        records.append(f"{name},sha256={b64},{len(data)}")
    records.append(f"{dist_info}/RECORD,,")
    files[f"{dist_info}/RECORD"] = ("\n".join(records) + "\n").encode("utf-8")

    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return wheel_path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        digest.update(handle.read())
    return digest.hexdigest()
