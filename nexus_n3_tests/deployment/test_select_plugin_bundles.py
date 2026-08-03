from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path


SELECTOR = (
    Path(__file__).resolve().parents[2]
    / "deployment"
    / "ansible"
    / "roles"
    / "nexus_release"
    / "files"
    / "select_plugin_bundles.py"
)


def _bundle(root: Path, filename: str, plugin_id: str, version: str, plugin_type: str) -> None:
    with zipfile.ZipFile(root / filename, "w") as archive:
        archive.writestr(
            "manifest.json",
            json.dumps({
                "plugin_id": plugin_id,
                "version": version,
                "plugin_type": plugin_type,
            }),
        )


def test_selects_only_the_newest_bundle_for_each_plugin_id(tmp_path: Path) -> None:
    _bundle(tmp_path, "loading-0.1.0.rsnxplugin", "loading", "0.1.0", "algorithm")
    _bundle(tmp_path, "loading-0.1.2.rsnxplugin", "loading", "0.1.2", "algorithm")
    _bundle(tmp_path, "other-1.0.0-rc1.rsnxplugin", "other", "1.0.0-rc1", "algorithm")
    _bundle(tmp_path, "other-1.0.0.rsnxplugin", "other", "1.0.0", "algorithm")

    completed = subprocess.run(
        [sys.executable, str(SELECTOR), str(tmp_path), "algorithm"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.splitlines() == [
        "loading-0.1.2.rsnxplugin",
        "other-1.0.0.rsnxplugin",
    ]


def test_rejects_a_bundle_from_the_wrong_plugin_family(tmp_path: Path) -> None:
    _bundle(tmp_path, "sensor.rsnxplugin", "sensor", "1.0.0", "sensor")

    completed = subprocess.run(
        [sys.executable, str(SELECTOR), str(tmp_path), "algorithm"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert "expected 'algorithm'" in completed.stderr
