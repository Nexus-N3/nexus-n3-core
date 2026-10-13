"""Focused connection-management regressions; no sockets, BLE or mDNS traffic."""
import json
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import zmq

from nexus_n3.distributed import worker_node as worker_module
from nexus_n3.distributed.master_node import MasterNode
from nexus_n3.gateway.messaging import message_handler as handler_module


def worker():
    node = worker_module.WorkerNode.__new__(worker_module.WorkerNode)
    node.node_id = 'worker-01'
    node.ip = '127.0.0.1'
    node.capabilities = {}
    node.dealer = Mock(closed=False)
    node._dealer_lock = threading.RLock()
    node._running = True
    node._last_sent = 100.0
    node._last_heartbeat_sent = 0.0
    node._last_recv = 1.0
    node._last_reconnect_attempt = 0.0
    node._heartbeat_interval = 30
    node._reconnect_timeout = 90
    node._reconnect_retry_interval = 5
    node.handler = Mock(si=None)
    node.registry = Mock()
    return node


def receive(node, monkeypatch, steps):
    clock = [0.0]
    messages = []

    class Poller:
        def register(self, *args):
            pass

        def poll(self, **kwargs):
            now, message = steps.pop(0)
            clock[0] = now
            if not steps:
                node._running = False
            if message is None:
                return []
            messages.append([json.dumps(message).encode()])
            return [(node.dealer, zmq.POLLIN)]

    monkeypatch.setattr(worker_module.zmq, 'Poller', Poller)
    monkeypatch.setattr(worker_module.time, 'time', lambda: clock[0])
    node.dealer.recv_multipart.side_effect = lambda: messages.pop(0)
    node._recv_loop()


def test_heartbeats_continue_during_constant_outbound_streaming(monkeypatch):
    node = worker()
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'time', lambda: clock[0])

    def tick(_):
        clock[0] += 30
        node._last_sent = clock[0]  # Simulate continuous normal outbound events.
        if node.dealer.send_json.call_count == 4:
            node._running = False

    monkeypatch.setattr(worker_module.time, 'sleep', tick)
    node._heartbeat_loop()
    assert [call.args[0]['ts'] for call in node.dealer.send_json.call_args_list] == [100, 130, 160, 190]
    assert all(call.args[0]['type'] == 'HEARTBEAT' for call in node.dealer.send_json.call_args_list)


def test_master_acknowledges_worker_heartbeat_without_publishing_it():
    node = MasterNode.__new__(MasterNode)
    node.node_id = 'master'
    node._running = True
    node.registry = Mock()
    node.registry.get_nodes.return_value = {'worker-01': {'role': 'worker'}}
    node.system_event_bus = Mock()
    node.router = Mock()
    node.router.recv_multipart.side_effect = [
        [b'worker-01', b'', json.dumps({'type': 'HEARTBEAT', 'node_id': 'worker-01', 'ts': 30}).encode()],
        zmq.ZMQError('end test'),
    ]
    node._router_loop()
    node.registry.heartbeat.assert_called_once_with('worker-01')
    node.router.send_multipart.assert_called_once()
    frames = node.router.send_multipart.call_args.args[0]
    assert frames[:2] == [b'worker-01', b'']
    assert json.loads(frames[2]) == {'type': 'HEARTBEAT_ACK', 'node_id': 'master', 'ts': 30}
    node.system_event_bus.emit.assert_not_called()


def test_acknowledgements_prevent_false_reconnection_beyond_90_seconds(monkeypatch):
    node = worker()
    node._reconnect_to_master = Mock()
    receive(node, monkeypatch, [
        (30, {'type': 'HEARTBEAT_ACK'}), (60, {'type': 'HEARTBEAT_ACK'}),
        (90, {'type': 'HEARTBEAT_ACK'}), (120, {'type': 'HEARTBEAT_ACK'}), (121, None),
    ])
    assert node._last_recv == 120
    node._reconnect_to_master.assert_not_called()
    node.handler.handle.assert_not_called()


def test_missing_master_traffic_still_triggers_reconnection(monkeypatch):
    node = worker()
    node._reconnect_to_master = Mock(return_value=False)
    receive(node, monkeypatch, [(121, None)])
    node._reconnect_to_master.assert_called_once()


def test_initial_registration_sets_up_once_and_repeated_registration_retains_core(monkeypatch):
    node = worker()
    node.handler = handler_module.MessageHandler('test', Mock(), node_id='worker-01')
    node.handler._log_plugin_startup_summary = Mock()
    active_sensor = SimpleNamespace(connected=True, streaming=True)
    subjects = [SimpleNamespace(subject_id='worker-subject', sensors=[active_sensor])]
    core = SimpleNamespace(subjects=subjects, session_timestamp='active-session', set_file_path=Mock())
    constructor = Mock(return_value=core)
    monkeypatch.setattr(handler_module, 'Core', constructor)
    receive(node, monkeypatch, [
        (10, {'type': 'REGISTER_ACK', 'usb_path': '/original'}),
        (20, {'type': 'REGISTER_ACK', 'usb_path': '/changed'}),
    ])
    constructor.assert_called_once()
    core.set_file_path.assert_called_once_with('/original')
    assert node.handler.is_ready and node.handler.si is core
    assert node.handler.si.subjects is subjects
    assert active_sensor.connected and active_sensor.streaming
    assert node.handler.si.session_timestamp == 'active-session'


def test_genuine_reconnection_preserves_active_core_and_registration(monkeypatch):
    node = worker()
    core = SimpleNamespace(subjects=['worker-subject'], session_timestamp='active-session', sensors=['connected-dot'])
    node.handler.si = core
    node.ctx = Mock()
    old_dealer = node.dealer
    new_dealer = Mock(closed=False)
    node.ctx.socket.return_value = new_dealer
    node.master_ip, node.master_port = 'old-master', 5555
    monkeypatch.setattr(worker_module, 'discover_master', lambda: ('new-master', 5555))
    assert node._reconnect_to_master()
    new_dealer.connect.assert_called_once_with('tcp://new-master:5555')
    assert new_dealer.send_json.call_args.args[0]['type'] == 'REGISTER'
    old_dealer.close.assert_called_once()
    assert node.dealer is new_dealer
    receive(node, monkeypatch, [(200, {'type': 'REGISTER_ACK', 'usb_path': '/changed'})])
    node.handler.handle.assert_not_called()
    assert node.handler.si is core and core.subjects == ['worker-subject']
    assert core.sensors == ['connected-dot'] and core.session_timestamp == 'active-session'
