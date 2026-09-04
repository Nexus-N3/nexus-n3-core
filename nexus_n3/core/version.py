"""Resolve the Nexus N3 Core release version from project metadata."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as distribution_version
from pathlib import Path
import re


CORE_DISTRIBUTION_NAME = "nexus-n3-core"
CORE_PROJECT_FILE = Path(__file__).resolve().parents[2] / "pyproject.toml"
_TOML_VERSION = re.compile(r'''^version\s*=\s*["']([^"']+)["']\s*$''')


def get_core_version() -> str:
    """Return the version of the source checkout or installed distribution.

    A source checkout is authoritative when its ``pyproject.toml`` is present.
    Installed deployments do not include that project file, so they use the
    standard distribution metadata generated from it during packaging.
    """

    source_version = _read_project_version(CORE_PROJECT_FILE)
    if source_version is not None:
        return source_version

    try:
        return distribution_version(CORE_DISTRIBUTION_NAME)
    except PackageNotFoundError as exc:
        raise RuntimeError(
            "Unable to determine the nexus-n3-core version from pyproject.toml "
            "or installed package metadata"
        ) from exc


def _read_project_version(path: Path) -> str | None:
    """Read the static PEP 621 version from a core ``pyproject.toml`` file."""

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    in_project_section = False
    for raw_line in lines:
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            in_project_section = line == "[project]"
            continue
        if not in_project_section:
            continue
        match = _TOML_VERSION.fullmatch(line)
        if match:
            return match.group(1).strip()
    return None
