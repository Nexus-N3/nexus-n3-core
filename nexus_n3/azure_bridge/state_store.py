"""Bridge-local state storage."""

from __future__ import annotations

from dataclasses import dataclass, asdict
import json
from pathlib import Path


@dataclass
class BridgeState:
    """Minimal persisted state for the bridge."""

    control_mode: str = "local_primary"
    device_lock_state: str = "idle"
    lock_owner: str = "none"
    active_session_id: str | None = None
    pending_session_config_id: str | None = None
    last_error: str | None = None


class BridgeStateStore:
    """In-memory state with optional file persistence."""

    def __init__(self, state_file: str | None = None):
        self._path = Path(state_file) if state_file else None
        self._state = self._load()

    @property
    def state(self) -> BridgeState:
        return self._state

    def snapshot(self) -> dict:
        return asdict(self._state)

    def update(self, **kwargs) -> BridgeState:
        for key, value in kwargs.items():
            if hasattr(self._state, key):
                setattr(self._state, key, value)
        self._save()
        return self._state

    def _load(self) -> BridgeState:
        if not self._path or not self._path.exists():
            return BridgeState()
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
            return BridgeState(**payload)
        except Exception:
            return BridgeState()

    def _save(self) -> None:
        if not self._path:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(asdict(self._state), indent=2, sort_keys=True), encoding="utf-8")
