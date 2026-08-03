#!/usr/bin/env python3
"""Select the newest bundle for each plugin ID from one catalog directory."""

from __future__ import annotations

import json
import re
import sys
import zipfile
from pathlib import Path


def version_key(version: str) -> tuple[int, int, int, int, bool, str]:
    """Return a deterministic key for the SemVer versions used by plugin bundles."""
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?", version)
    if match is None:
        return (0, 0, 0, 0, False, version)
    prerelease = match.group(4)
    return (
        1,
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
        prerelease is None,
        prerelease or "",
    )


def select_bundles(root: Path, expected_type: str) -> list[Path]:
    selected: dict[str, tuple[tuple[int, int, int, int, bool, str], Path]] = {}
    for bundle_path in sorted(root.glob("*.rsnxplugin")):
        with zipfile.ZipFile(bundle_path) as archive:
            manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
        plugin_type = str(manifest.get("plugin_type") or "")
        if plugin_type != expected_type:
            raise ValueError(
                f"{bundle_path.name} contains plugin_type={plugin_type!r}, expected {expected_type!r}"
            )
        plugin_id = str(manifest.get("plugin_id") or "")
        version = str(manifest.get("version") or "")
        if not plugin_id or not version:
            raise ValueError(f"{bundle_path.name} is missing plugin_id or version")
        candidate = (version_key(version), bundle_path)
        current = selected.get(plugin_id)
        if current is None or candidate[0] > current[0]:
            selected[plugin_id] = candidate
    return [selected[plugin_id][1] for plugin_id in sorted(selected)]


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: select_plugin_bundles.py BUNDLE_ROOT {sensor|algorithm}")
    for selected_path in select_bundles(Path(sys.argv[1]), sys.argv[2]):
        print(selected_path.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
