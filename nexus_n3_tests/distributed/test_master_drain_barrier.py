"""Unit tests for distributed stream-drain finalization."""

import threading

from nexus_n3.distributed.master_node import MasterNode


def _master_with_active_subjects(*subject_ids: str) -> MasterNode:
    master = MasterNode.__new__(MasterNode)
    master._drain_lock = threading.RLock()
    master._drain_tracking = {}
    master._active_stream_subjects = set(subject_ids)
    master._after_all_streams_drained = None
    return master


def test_error_ack_still_finalizes_shared_session() -> None:
    master = _master_with_active_subjects("master-subject", "worker-subject")
    callbacks = []
    master.set_after_all_streams_drained(callbacks.append)
    master._create_drain_record(
        "stop-1",
        scope="all",
        subject_ids=["master-subject", "worker-subject"],
        expected_nodes=["master", "worker-1"],
    )

    master._record_drain_ack(
        "master",
        {
            "stop_session_id": "stop-1",
            "status": "ok",
            "subject_ids": ["master-subject"],
            "session_timestamp": "20260908_120000",
        },
    )
    master._record_drain_ack(
        "worker-1",
        {
            "stop_session_id": "stop-1",
            "status": "error",
            "reason": "raw write failure",
            "subject_ids": ["worker-subject"],
            "session_timestamp": "20260908_120000",
        },
    )

    assert callbacks == [
        {
            "stop_session_id": "stop-1",
            "status": "error",
            "reason": "raw write failure",
            "session_timestamp": "20260908_120000",
            "acks": {
                "master": {
                    "status": "ok",
                    "reason": None,
                    "subject_ids": ["master-subject"],
                    "session_timestamp": "20260908_120000",
                    "drained": True,
                },
                "worker-1": {
                    "status": "error",
                    "reason": "raw write failure",
                    "subject_ids": ["worker-subject"],
                    "session_timestamp": "20260908_120000",
                    "drained": True,
                },
            },
        }
    ]
    assert master._drain_tracking["stop-1"]["finalized"] is True


def test_failed_finalization_callback_can_be_retried() -> None:
    master = _master_with_active_subjects("subject-1")
    attempts = []

    def finalize(_drain_result):
        attempts.append("attempt")
        if len(attempts) == 1:
            raise RuntimeError("temporary archive failure")

    master.set_after_all_streams_drained(finalize)
    master._create_drain_record(
        "stop-2",
        scope="all",
        subject_ids=["subject-1"],
        expected_nodes=["master"],
    )
    payload = {
        "stop_session_id": "stop-2",
        "status": "ok",
        "subject_ids": ["subject-1"],
        "session_timestamp": "20260908_120001",
    }

    master._record_drain_ack("master", payload)
    assert master._drain_tracking["stop-2"]["finalized"] is False

    master._record_drain_ack("master", payload)

    assert attempts == ["attempt", "attempt"]
    assert master._drain_tracking["stop-2"]["finalized"] is True


def test_stop_exception_does_not_archive_an_undrained_node() -> None:
    master = _master_with_active_subjects("subject-1")
    callbacks = []
    master.set_after_all_streams_drained(callbacks.append)
    master._create_drain_record(
        "stop-3",
        scope="all",
        subject_ids=["subject-1"],
        expected_nodes=["worker-1"],
    )

    master._record_drain_ack(
        "worker-1",
        {
            "stop_session_id": "stop-3",
            "status": "error",
            "drained": False,
            "reason": "stop failed before local drain completed",
            "subject_ids": ["subject-1"],
            "session_timestamp": "20260908_120002",
        },
    )

    assert callbacks == []
    assert master._active_stream_subjects == {"subject-1"}
    assert master._drain_tracking["stop-3"]["finalized"] is False
