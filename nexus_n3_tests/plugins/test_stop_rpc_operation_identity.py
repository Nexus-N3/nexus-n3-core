"""Nested plugin RPC carries STOP identity without changing plugin contracts."""
import io
import json
import time
from unittest.mock import Mock

import pytest

from nexus_n3.plugins.common.jsonrpc import JsonRpcConnection
from nexus_n3.plugins.common.operations import SensorOperation, operation_scope


def test_rpc_timeout_retires_waiter_and_nested_call_keeps_operation_identity():
    writer = io.StringIO()
    rpc = JsonRpcConnection(io.StringIO(), writer, autostart=False)
    operation = SensorOperation(time.monotonic() + 1)
    with operation_scope(operation), pytest.raises(RuntimeError, match='timed out'):
        rpc.request('stop_stream', {}, timeout=0)
    request = json.loads(writer.getvalue())
    assert request['operation']['id'] == operation.operation_id
    assert not rpc._pending and not rpc._closed
    gateway_write = Mock()
    rpc.register_handler('adapter.write', gateway_write)
    operation.cancelled.set()
    rpc._handle_request({'id': 'late', 'method': 'adapter.write', 'params': {},
                         'operation': request['operation']})
    gateway_write.assert_not_called()
    reply = json.loads(writer.getvalue().splitlines()[-1])
    assert reply['id'] == 'late' and 'error' in reply
    rpc.close()


def test_host_side_nested_rpc_propagates_same_attempt():
    writer = io.StringIO()
    host = JsonRpcConnection(io.StringIO(), writer, autostart=False)
    def stop(_):
        host.notify('adapter.write', {'uuid': 'plugin-owned'})
        return {'ok': True}
    host.register_handler('stop_stream', stop)
    operation = SensorOperation(time.monotonic()+1)
    host._handle_request({'id': 'stop', 'method': 'stop_stream', 'params': {}, 'operation': operation.wire()})
    nested, response = [json.loads(line) for line in writer.getvalue().splitlines()]
    assert nested['operation'] == operation.wire()
    assert nested['params'] == {'uuid': 'plugin-owned'}
    assert response['result']['ok']
    host.close()
