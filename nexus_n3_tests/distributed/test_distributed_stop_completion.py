"""Aggregate STOP results reuse the existing drain barrier."""
import threading
from unittest.mock import Mock

from nexus_n3.distributed.master_node import MasterNode


def master():
    node = MasterNode.__new__(MasterNode)
    node.node_id = 'master'
    node._drain_lock = threading.RLock()
    node._drain_tracking = {}
    node._active_stream_subjects = {'subject-a', 'subject-b'}
    node._after_all_streams_drained = Mock()
    node.system_event_bus = Mock()
    node._create_drain_record('stop-1', scope='subjects', subject_ids=['subject-a', 'subject-b'],
                              expected_nodes=['master', 'worker-01'])
    return node


def ack(subject, status='ok', drained=True):
    return {'stop_session_id': 'stop-1', 'subject_ids': [subject], 'status': status,
            'drained': drained, 'session_timestamp': 'session-1'}


def test_aggregate_completion_follows_all_nodes_and_archive_finalization_once():
    node = master()
    node._record_drain_ack('master', ack('subject-a'))
    node.system_event_bus.emit.assert_not_called()
    node._after_all_streams_drained.assert_not_called()

    def finalize(_):
        node.system_event_bus.emit.assert_not_called()

    node._after_all_streams_drained.side_effect = finalize
    node._record_drain_ack('worker-01', ack('subject-b'))
    node._after_all_streams_drained.assert_called_once()
    event = node.system_event_bus.emit.call_args.args[0]
    assert event['type'] == 'stream_drained' and event['node_id'] == 'master'
    assert event['payload']['aggregate'] is True
    assert event['payload']['status'] == 'ok'
    assert event['payload']['subject_ids'] == ['subject-a', 'subject-b']
    assert event['payload']['expected_node_ids'] == ['master', 'worker-01']
    assert event['payload']['all_streams_drained'] is True
    assert event['payload']['stop_session_id'] == 'stop-1'
    node._record_drain_ack('worker-01', ack('subject-b'))
    node.system_event_bus.emit.assert_called_once()


def test_unsuccessful_node_drain_never_reports_aggregate_success():
    node = master()
    node._record_drain_ack('master', ack('subject-a'))
    node._record_drain_ack('worker-01', ack('subject-b', status='error'))
    assert node.system_event_bus.emit.call_args.args[0]['payload']['status'] == 'error'


def test_undrained_node_keeps_barrier_pending():
    node = master()
    node._record_drain_ack('master', ack('subject-a'))
    node._record_drain_ack('worker-01', ack('subject-b', status='error', drained=False))
    node._after_all_streams_drained.assert_not_called()
    node.system_event_bus.emit.assert_not_called()


def test_archive_failure_is_reported_without_claiming_completion():
    node = master()
    node._after_all_streams_drained.side_effect = RuntimeError('archive failed')
    node._record_drain_ack('master', ack('subject-a'))
    node._record_drain_ack('worker-01', ack('subject-b'))
    payload = node.system_event_bus.emit.call_args.args[0]['payload']
    assert payload['aggregate'] is True and payload['status'] == 'error'
    assert payload['reason'] == 'archive failed'
    assert node._drain_tracking['stop-1']['finalized'] is False
