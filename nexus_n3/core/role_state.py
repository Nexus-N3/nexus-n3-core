"""Persist the locally selected standalone/master Core role."""

from __future__ import annotations

import json
import os
from pathlib import Path

SELECTABLE_ROLES = frozenset({"standalone", "master"})


def role_state_path() -> Path:
    configured = os.environ.get("NEXUS_N3_ROLE_STATE_FILE", "").strip()
    if configured:
        return Path(configured).expanduser()
    output_root = os.environ.get("NEXUS_N3_OUTPUT_ROOT", "nexus_n3_outputs").strip()
    return Path(output_root).expanduser() / ".nexus_n3_role.json"


def load_role_override() -> str | None:
    try:
        payload = json.loads(role_state_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    role = payload.get("role") if isinstance(payload, dict) else None
    return role if role in SELECTABLE_ROLES else None


def save_role_override(role: str) -> None:
    if role not in SELECTABLE_ROLES:
        raise ValueError(f"Unsupported selectable Core role: {role}")
    path = role_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps({"role": role}, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)

def resolve_role(configured_role: str) -> str:
    if configured_role not in SELECTABLE_ROLES:
        return configured_role
    return load_role_override() or configured_role