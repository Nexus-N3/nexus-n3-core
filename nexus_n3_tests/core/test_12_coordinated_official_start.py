"""Core-level tests for committing a coordinated official start."""

import threading
from types import MethodType

import pytest

from nexus_n3.core.core import Core
from nexus_n3.gateway.messaging import message_types as mt


def _ready_core():
    core = Core.__new__(Core)
    core._startup_lock = threading.RLock()
    core._official_start_mode = "coordinated"
    core._start_session_id = "start-1"
    core.session_timestamp = "20260910_120000"
    core.stream_phase = "ready_for_official"
    core._startup_last_failure_reason = None
    core.activations = 0
    core.events = []

    def activate(self):
        self.activations += 1

    def status(self, *, phase, reason=None):
        return {
            "phase": phase,
            "start_session_id": self._start_session_id,
            "session_timestamp": self.session_timestamp,
        }

    def emit(self, event_type, payload):
        self.events.append((event_type, payload))

    core._activate_official_streaming = MethodType(activate, core)
    core._startup_status_payload = MethodType(status, core)
    core._emit_startup_event = MethodType(emit, core)
    return core


def test_coordinated_commit_activates_once_and_is_idempotent():
    core = _ready_core()
    payload = {
        "start_session_id": "start-1",
        "session_timestamp": "20260910_120000",
    }

    core.start_official_stream(payload)
    core.start_official_stream(payload)

    assert core.activations == 1
    assert core.stream_phase == "official_streaming"
    assert [event_type for event_type, _ in core.events] == [
        mt.EVT_STREAM_OFFICIAL_STARTED,
        mt.EVT_STREAM_OFFICIAL_STARTED,
    ]


def test_coordinated_commit_rejects_stale_session():
    core = _ready_core()

    with pytest.raises(RuntimeError, match="stale session timestamp"):
        core.start_official_stream(
            {
                "start_session_id": "start-1",
                "session_timestamp": "20260910_115959",
            }
        )

    assert core.activations == 0
    assert core.stream_phase == "ready_for_official"
