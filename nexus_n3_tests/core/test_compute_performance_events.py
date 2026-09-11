from pathlib import Path
import sys
from types import SimpleNamespace

CORE_ROOT = Path(__file__).resolve().parents[2]
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from nexus_n3.core.core import Core
from nexus_n3.core.orchestrators.event_assembler import EventAssembler
from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.plugins.runtime.serde import RemoteComputeResult


class _EventBus:
    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(event)


class _FileManager:
    session_label = "timing-test"
    session_name = "timing-test"

    def __init__(self):
        self.events = []

    def enqueue_session_diagnostics_event(self, session_timestamp, event_type, payload):
        self.events.append((session_timestamp, event_type, payload))


def test_compute_performance_is_enriched_emitted_and_archived():
    core = Core.__new__(Core)
    sensor = SimpleNamespace(address="sensor-1")
    subject = SimpleNamespace(
        subject_id="subject-1",
        sensors=[{"sensor": sensor, "meta": {"location": "CHEST"}}],
    )
    core.subject_graph = SimpleNamespace(
        find_subject_by_address=lambda address: subject if address == "sensor-1" else None
    )
    file_manager = _FileManager()
    core.storage = SimpleNamespace(file_manager=file_manager)
    core.system_event_bus = _EventBus()
    core.site = "test-site"
    core.session_timestamp = "20260825_120000"
    core.app_id = None
    core.app_name = None
    core.pending_correlation_id = None
    core._official_outputs_active = True

    core._on_compute_performance(
        {
            "address": "sensor-1",
            "algorithm_name": "standard_loading_intensity",
            "result_count": 2,
            "algorithm_execution_ms": 3.5,
            "result_interval_ms": 5001.0,
        }
    )

    assert len(core.system_event_bus.events) == 1
    emitted = core.system_event_bus.events[0]
    assert emitted["type"] == mt.EVT_COMPUTE_PERFORMANCE
    assert emitted["payload"]["subject_id"] == "subject-1"
    assert emitted["payload"]["location"] == "CHEST"
    assert emitted["payload"]["algorithm_execution_ms"] == 3.5

    assert len(file_manager.events) == 1
    session_timestamp, event_type, archived = file_manager.events[0]
    assert session_timestamp == "20260825_120000"
    assert event_type == mt.EVT_COMPUTE_PERFORMANCE
    assert archived == emitted["payload"]


def test_compute_performance_metadata_does_not_leak_into_algorithm_result():
    result = RemoteComputeResult(
        {
            "address": "sensor-1",
            "algorithm_name": "standard_loading_intensity",
            "result_count": 1,
        }
    )
    result._compute_performance = {"algorithm_execution_ms": 3.5}

    payload = EventAssembler().build_compute_payload(
        "subject-1",
        result,
        "CHEST",
    )

    assert payload["result"] == {
        "address": "sensor-1",
        "algorithm_name": "standard_loading_intensity",
        "result_count": 1,
    }
