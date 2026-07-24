"""Cross-platform helpers for isolated plugin runtimes."""

from __future__ import annotations

import os
from pathlib import Path


class PluginRuntimeEnvironmentError(RuntimeError):
    """Raised when an installed plugin runtime is incomplete."""


def resolve_runtime_python(runtime_path: str | Path) -> Path:
    """Return the Python executable inside a plugin virtual environment."""

    runtime_path = Path(runtime_path)

    if os.name == "nt":
        executable = runtime_path / "Scripts" / "python.exe"
    else:
        executable = runtime_path / "bin" / "python"

    if not executable.is_file():
        raise PluginRuntimeEnvironmentError(
            f"Plugin runtime Python executable was not found: {executable}"
        )

    return executable


def prepend_pythonpath(
    env: dict[str, str],
    path: str | Path,
) -> None:
    """Add a path to PYTHONPATH using the platform path separator."""

    path_text = str(Path(path))
    current = env.get("PYTHONPATH")

    env["PYTHONPATH"] = (
        os.pathsep.join((path_text, current))
        if current
        else path_text
    )