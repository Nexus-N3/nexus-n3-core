"""Shared YAML loading helpers for plugin/runtime metadata."""

from __future__ import annotations

from pathlib import Path

import yaml


def load_yaml(path: str | Path) -> dict:
    """Load a YAML file from an explicit filesystem path."""

    candidate = Path(path)
    with candidate.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
