"""Core-level tests for committing a coordinated official start."""

import threading
from types import MethodType, SimpleNamespace

import pytest

from nexus_n3.core.core import Core
from nexus_n3.gateway.messaging import message_types as mt


def _ready_core():
    core = Core.__new__(Core)
    core._startup_lock = threading.RLock()
    core.node_id = "worker-1"
    core._official_start_mode = "coordinated"
    core._start_session_id = "start-1"
    core.session_timestamp = "20260910_120000"
    core.stream_phase = "ready_for_official"
    core._startup_last_failure_reason = None
    core._official_commit_received = False
    core._official_outputs_active = False
    core._official_start_timing = {}
    core.activations = 0
    core.events = []
    core.states_at_emit = []

    def activate(self):
        self.activations += 1

    def status(self, *, phase, reason=None):
        return {
            "phase": phase,
            "start_session_id": self._start_session_id,
            "session_timestamp": self.session_timestamp,
        }

    def emit(self, event_type, payload, **_kwargs):
        self.events.append((event_type, payload))
        self.states_at_emit.append(
            (self.stream_phase, self._official_outputs_active)
        )

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
    assert core._official_commit_received is True
    assert core._official_outputs_active is True
    assert core.stream_phase == "official_streaming"
    assert [event_type for event_type, _ in core.events] == [
        mt.EVT_STREAM_OFFICIAL_STARTED,
        mt.EVT_STREAM_OFFICIAL_STARTED,
    ]
    assert core.states_at_emit[0] == ("activating_official", False)


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


def test_coordinated_activation_requires_the_global_commit():
    core = Core.__new__(Core)
    core._official_start_mode = "coordinated"
    core._official_commit_received = False

    with pytest.raises(RuntimeError, match="requires the global commit"):
        core._activate_official_streaming()


def test_compute_outputs_are_dropped_before_official_activation():
    core = Core.__new__(Core)
    core._official_outputs_active = False
    core.subject_graph = None

    core._on_compute_result({"address": "sensor-1"})
    core._on_intermediate_result({"results": []})
    core._on_compute_performance({"address": "sensor-1"})


def test_persistence_preflight_is_idempotent_and_does_not_enable_subject():
    start_calls = []

    class _FileManager:
        def start_stream(self, subject, session_timestamp, tag=None):
            start_calls.append((subject.subject_id, session_timestamp, tag))

    subject = SimpleNamespace(subject_id="subject-1", is_streaming=False)
    core = Core.__new__(Core)
    core._startup_subject_ids = [subject.subject_id]
    core._pending_stream_tags = {subject.subject_id: "walk"}
    core._official_persistence_prepared_subject_ids = set()
    core._official_start_timing = {}
    core.subjects = [subject]
    core.session_timestamp = "20260911_120000"
    core.storage = SimpleNamespace(file_manager=_FileManager())

    core._prepare_official_persistence()
    core._prepare_official_persistence()

    assert start_calls == [("subject-1", "20260911_120000", "walk")]
    assert subject.is_streaming is False
    assert core._official_persistence_prepared_subject_ids == {"subject-1"}
    assert (
        core._official_start_timing["node_persistence_prepared_subject_ids"]
        == ["subject-1"]
    )
    assert "node_persistence_preparation_duration_ms" in core._official_start_timing

    core._official_start_mode = "coordinated"
    core._official_commit_received = True
    core._official_stream_origin_monotonic_ns = None
    core._activate_official_streaming()

    assert start_calls == [("subject-1", "20260911_120000", "walk")]
    assert subject.is_streaming is True
    assert core._official_stream_origin_monotonic_ns is not None


def test_coordinated_readiness_is_emitted_after_persistence_preflight():
    core = Core.__new__(Core)
    core._startup_lock = threading.RLock()
    core._startup_post_connect_settle_seconds = 0
    core._startup_stability_window_seconds = 0
    core._startup_gate_token = 1
    core.stream_phase = "warming_up"
    core._startup_addresses = ["sensor-1"]
    core._startup_stats_by_address = {"sensor-1": object()}
    core._official_start_mode = "coordinated"
    order = []

    core._is_sensor_startup_stable = MethodType(lambda self, _stats: True, core)
    core._prepare_official_persistence = MethodType(
        lambda self: order.append("prepare"),
        core,
    )
    core._startup_status_payload = MethodType(
        lambda self, *, phase, reason=None: {"phase": phase},
        core,
    )
    core._emit_startup_event = MethodType(
        lambda self, event_type, _payload: order.append(event_type),
        core,
    )

    core._evaluate_startup_gate(1)

    assert order == ["prepare", mt.EVT_STREAM_READY_FOR_OFFICIAL]
    assert core.stream_phase == "ready_for_official"
