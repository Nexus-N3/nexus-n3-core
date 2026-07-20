"""Central runtime environment loading for nexus-n3-core."""

from __future__ import annotations

import os
from pathlib import Path


DEFAULT_PROD_ENV_PATH = Path("/etc/nexus-n3/runtime.env")
DEFAULT_DEV_ENV_PATH = Path(__file__).resolve().parents[2] / "config" / "runtime.env"
ENV_FILE_VAR = "NEXUS_N3_ENV_FILE"

_loaded_env_paths: set[Path] = set()


def load_runtime_env(*, force: bool = False) -> Path | None:
    """Load runtime environment variables from the shared env file when present."""
    env_path = _resolve_env_path()
    if env_path is None:
        return None

    normalized = env_path.expanduser().resolve()
    if not force and normalized in _loaded_env_paths:
        return normalized

    _apply_env_file(normalized)
    _loaded_env_paths.add(normalized)
    return normalized


def reset_runtime_env() -> None:
    """Forget previously loaded env files so the loader may be re-run."""
    _loaded_env_paths.clear()


def _resolve_env_path() -> Path | None:
    explicit = os.environ.get(ENV_FILE_VAR, "").strip()
    if explicit:
        path = Path(explicit).expanduser()
        return path if path.exists() else None

    if DEFAULT_DEV_ENV_PATH.exists():
        return DEFAULT_DEV_ENV_PATH

    if DEFAULT_PROD_ENV_PATH.exists():
        return DEFAULT_PROD_ENV_PATH

    return None


def _apply_env_file(path: Path) -> None:
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
