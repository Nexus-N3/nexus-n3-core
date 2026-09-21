"""Unit tests for the distributed official-start barrier."""

import threading

from nexus_n3.distributed.master_node import MasterNode
from nexus_n3.gateway.messaging import message_types as mt


class _Registry:
    def __init__(self):
        self.nodes = {
            "master": {"role": "master"},
            "worker-1": {"role": "worker"},
        }
        self.subjects = {
            "subject-a": {"assigned_node": "master"},
            "subject-b": {"assigned_node": "worker-1"},
        }

    def get_nodes(self):
        return self.nodes

    def get_subjects(self):
        return self.subjects


class _Handler:
    def __init__(self):
        self.calls = []
        self.events = []

    def prepare_stream_start(self):
        self.events.append(("prepare", None))

    def _handle_local(self, msg_type, payload, *, stream_start_prepared=False):
        self.events.append(("local", msg_type))
        self.calls.append((msg_type, payload))
        assert stream_start_prepared is (
            msg_type in (mt.CMD_START_STREAM_FOR_ALL, mt.CMD_START_STREAM_FOR_SUBJECTS)
        )


class _EventBus:
    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(event)


def _master():
    master = MasterNode.__new__(MasterNode)
    master.node_id = "master"
    master.registry = _Registry()
    master.system_event_bus = None
    master._official_start_lock = threading.RLock()
    master._official_start_tracking = {}
    master._drain_lock = threading.RLock()
    master._drain_tracking = {}
    master._active_stream_subjects = set()
    master.sent = []
    master.send_command = lambda msg, target_node_id=None: master.sent.append(
        (target_node_id, msg)
    )
    return master


def test_official_commit_waits_for_every_expected_node():
    master = _master()
    handler = _Handler()
    master._create_official_start_record(
        "start-1",
        session_timestamp="20260910_120000",
        subject_ids=["subject-a", "subject-b"],
        expected_nodes=["master", "worker-1"],
    )
    ready = {
        "start_session_id": "start-1",
        "session_timestamp": "20260910_120000",
    }
    master._record_official_start_event(
        "master", mt.EVT_STREAM_READY_FOR_OFFICIAL, ready
    )

    master.dispatch_command(
        {"type": mt.CMD_START_OFFICIAL_STREAM, "payload": ready},
        message_handler=handler,
    )

    assert handler.calls == []
    assert master.sent == []

    master._record_official_start_event(
        "worker-1", mt.EVT_STREAM_READY_FOR_OFFICIAL, ready
    )
    master.dispatch_command(
        {"type": mt.CMD_START_OFFICIAL_STREAM, "payload": ready},
        message_handler=handler,
    )

    assert handler.calls[0][0] == mt.CMD_START_OFFICIAL_STREAM
    assert handler.calls[0][1]["start_session_id"] == "start-1"
    assert handler.calls[0][1]["official_start_timing"]["target_node_id"] == "master"
    assert handler.calls[0][1]["official_start_timing"]["dispatch_sequence"] == 2
    assert master.sent[0][0] == "worker-1"
    assert master.sent[0][1]["type"] == mt.CMD_START_OFFICIAL_STREAM
    assert master.sent[0][1]["payload"]["start_session_id"] == "start-1"
    assert (
        master.sent[0][1]["payload"]["official_start_timing"]["target_node_id"]
        == "worker-1"
    )
    assert (
        master.sent[0][1]["payload"]["official_start_timing"]["dispatch_sequence"]
        == 1
    )


def test_distributed_start_injects_coordinated_mode_for_every_participant():
    master = _master()
    handler = _Handler()

    master.dispatch_command(
        {
            "type": mt.CMD_START_STREAM_FOR_ALL,
            "payload": {"start_session_id": "start-1", "tag": "session"},
        },
        message_handler=handler,
    )

    local_type, local_payload = handler.calls[0]
    assert local_type == mt.CMD_START_STREAM_FOR_ALL
    assert local_payload["official_start_mode"] == "coordinated"
    assert local_payload["start_session_id"] == "start-1"

    worker_id, worker_message = master.sent[0]
    assert worker_id == "worker-1"
    assert worker_message["payload"]["official_start_mode"] == "coordinated"
    assert worker_message["payload"]["start_session_id"] == "start-1"
    assert master._official_start_tracking["start-1"]["expected_nodes"] == {
        "master",
        "worker-1",
    }
    assert handler.events == [
        ("prepare", None),
        ("local", mt.CMD_START_STREAM_FOR_ALL),
    ]


def test_distributed_start_prepares_master_storage_without_a_local_subject():
    master = _master()
    master.registry.subjects = {
        "subject-b": {"assigned_node": "worker-1"},
    }
    handler = _Handler()
    dispatch_events = []
    master.send_command = lambda msg, target_node_id=None: dispatch_events.append(
        ("send", target_node_id, msg["type"])
    )

    original_prepare = handler.prepare_stream_start

    def record_prepare():
        dispatch_events.append(("prepare", None, None))
        original_prepare()

    handler.prepare_stream_start = record_prepare

    master.dispatch_command(
        {
            "type": mt.CMD_START_STREAM_FOR_ALL,
            "payload": {"start_session_id": "start-worker-only", "tag": "session"},
        },
        message_handler=handler,
    )

    assert handler.calls == []
    assert handler.events == [("prepare", None)]
    assert dispatch_events[0] == ("prepare", None, None)
    assert dispatch_events[1] == (
        "send",
        "worker-1",
        mt.CMD_START_STREAM_FOR_ALL,
    )


def test_stale_readiness_does_not_release_barrier():
    master = _master()
    master._create_official_start_record(
        "start-current",
        session_timestamp="20260910_120000",
        subject_ids=["subject-a"],
        expected_nodes=["worker-1"],
    )

    master._record_official_start_event(
        "worker-1",
        mt.EVT_STREAM_READY_FOR_OFFICIAL,
        {
            "start_session_id": "start-old",
            "session_timestamp": "20260910_115959",
        },
    )

    record = master._official_start_tracking["start-current"]
    assert record["ready_nodes"] == set()


def test_master_announces_the_completed_readiness_barrier_once():
    master = _master()
    master.system_event_bus = _EventBus()
    master._create_official_start_record(
        "start-1",
        session_timestamp="20260910_120000",
        subject_ids=["subject-a", "subject-b"],
        expected_nodes=["master", "worker-1"],
    )
    ready = {
        "start_session_id": "start-1",
        "session_timestamp": "20260910_120000",
    }

    master._record_official_start_event(
        "master", mt.EVT_STREAM_READY_FOR_OFFICIAL, ready
    )
    assert master.system_event_bus.events == []

    master._record_official_start_event(
        "worker-1", mt.EVT_STREAM_READY_FOR_OFFICIAL, ready
    )
    master._record_official_start_event(
        "worker-1", mt.EVT_STREAM_READY_FOR_OFFICIAL, ready
    )

    assert master.system_event_bus.events == [
        {
            "type": mt.EVT_DISTRIBUTED_READY_FOR_OFFICIAL,
            "node_id": "master",
            "payload": {
                "start_session_id": "start-1",
                "session_timestamp": "20260910_120000",
                "subject_ids": ["subject-a", "subject-b"],
                "expected_nodes": ["master", "worker-1"],
                "ready_nodes": ["master", "worker-1"],
            },
        }
    ]
