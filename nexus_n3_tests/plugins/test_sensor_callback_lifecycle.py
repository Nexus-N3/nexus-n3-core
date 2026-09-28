from __future__ import annotations

import asyncio
import base64
import threading

from nexus_n3.plugins.runtime.sensor_host import HostAdapterProxy
from nexus_n3.plugins.runtime.sensor_runtime import InstalledSensorProxy, SensorHostClient


class FakeConnection:
    def __init__(self):
        self.handlers = {}
        self.requests: list[tuple[str, dict]] = []

    def register_handler(self, method, handler, **_kwargs):
        self.handlers[method] = handler

    def request(self, method, params):
        self.requests.append((method, params))
        return {"ok": True}


class FakeAdapter:
    def __init__(self):
        self.set_calls: list[tuple[str, object, bool]] = []
        self.unset_calls: list[str] = []
        self.callbacks: dict[str, object] = {}

    async def set_notify_callback(
        self,
        transport_client,
        uuid,
        callback_func,
        *,
        indicate=False,
    ):
        notify_uuid = str(uuid)
        self.set_calls.append((notify_uuid, callback_func, indicate))
        self.callbacks[notify_uuid] = callback_func

    async def unset_notify_callback(self, transport_client, uuid):
        notify_uuid = str(uuid)
        self.unset_calls.append(notify_uuid)
        self.callbacks.pop(notify_uuid, None)


class FakeCoreTransport:
    def __init__(self):
        self.notifications: list[tuple[str, dict]] = []

    def notify(self, method, params):
        self.notifications.append((method, params))


def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _build_core_proxy(loop: asyncio.AbstractEventLoop, adapter: FakeAdapter):
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    proxy._runtime_adapter = adapter
    proxy.transport_client = object()
    proxy._manager_loop = loop
    proxy._adapter_callbacks = {}
    return proxy


def test_host_callback_ids_are_monotonic_across_unsubscribe_and_resubscribe():
    connection = FakeConnection()
    adapter = HostAdapterProxy(connection)
    received: list[tuple[str, bytes]] = []

    async def exercise():
        await adapter.set_notify_callback(
            None,
            "measurement",
            lambda _sender, data: received.append(("measurement-1", data)),
        )
        await adapter.set_notify_callback(
            None,
            "control-point",
            lambda _sender, data: received.append(("control", data)),
            indicate=True,
        )
        await adapter.unset_notify_callback(None, "measurement")
        await adapter.set_notify_callback(
            None,
            "measurement",
            lambda _sender, data: received.append(("measurement-2", data)),
        )

    asyncio.run(exercise())

    subscribe_requests = [
        params for method, params in connection.requests if method == "adapter.subscribe"
    ]
    assert [request["callback_id"] for request in subscribe_requests] == [
        "cb-1",
        "cb-2",
        "cb-3",
    ]
    assert connection.requests[2] == (
        "adapter.unsubscribe",
        {"uuid": "measurement", "callback_id": "cb-1"},
    )
    assert adapter._callbacks_by_uuid == {
        "control-point": "cb-2",
        "measurement": "cb-3",
    }

    adapter._handle_notification(
        {
            "callback_id": "cb-2",
            "sender": "control-point",
            "data_b64": base64.b64encode(b"control-response").decode("ascii"),
        }
    )
    adapter._handle_notification(
        {
            "callback_id": "cb-3",
            "sender": "measurement",
            "data_b64": base64.b64encode(b"measurement-response").decode("ascii"),
        }
    )
    assert received == [
        ("control", b"control-response"),
        ("measurement-2", b"measurement-response"),
    ]


def test_core_unregisters_stale_callback_and_rebinds_repeat_subscription():
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=_run_loop, args=(loop,), daemon=True)
    loop_thread.start()
    adapter = FakeAdapter()
    proxy = _build_core_proxy(loop, adapter)
    transport = FakeCoreTransport()

    try:
        proxy._register_host_callback(
            callback_id="cb-1",
            notify_uuid="measurement",
            transport=transport,
        )
        proxy._register_host_callback(
            callback_id="cb-2",
            notify_uuid="control-point",
            indicate=True,
            transport=transport,
        )
        proxy._register_host_callback(
            callback_id="cb-2",
            notify_uuid="control-point",
            indicate=True,
            transport=transport,
        )
        assert len(adapter.set_calls) == 2

        proxy._unregister_host_callback(
            callback_id="cb-1",
            notify_uuid="measurement",
        )

        assert proxy._adapter_callbacks == {"cb-2": "control-point"}
        assert adapter.unset_calls == ["measurement"]

        proxy._register_host_callback(
            callback_id="cb-3",
            notify_uuid="measurement",
            transport=transport,
        )

        assert [uuid for uuid, _callback, _indicate in adapter.set_calls] == [
            "measurement",
            "control-point",
            "measurement",
        ]
        assert proxy._adapter_callbacks == {
            "cb-2": "control-point",
            "cb-3": "measurement",
        }

        adapter.callbacks["control-point"]("control-point", b"control-response")
        adapter.callbacks["measurement"]("measurement", b"measurement-response")
        assert [
            params["callback_id"] for method, params in transport.notifications
            if method == "adapter.notification"
        ] == ["cb-2", "cb-3"]
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2.0)
        loop.close()


def test_unsubscribe_rpc_forwards_callback_id_and_cleans_matching_uuid_entries():
    calls = []

    class FakeProxy:
        def _unregister_host_callback(self, **kwargs):
            calls.append(kwargs)

    client = SensorHostClient.__new__(SensorHostClient)
    client.proxy = FakeProxy()

    assert client._handle_adapter_unsubscribe(
        {"uuid": "measurement", "callback_id": "cb-3"}
    ) == {"ok": True}
    assert calls == [
        {"callback_id": "cb-3", "notify_uuid": "measurement"}
    ]

    adapter = FakeAdapter()
    proxy = InstalledSensorProxy.__new__(InstalledSensorProxy)
    proxy._adapter_callbacks = {
        "cb-stale-1": "measurement",
        "cb-stale-2": "measurement",
        "cb-control": "control-point",
    }
    proxy._adapter_request = lambda method, uuid: calls.append((method, uuid))
    proxy._unregister_host_callback(
        callback_id=None,
        notify_uuid="measurement",
    )

    assert proxy._adapter_callbacks == {"cb-control": "control-point"}
    assert calls[-1] == ("unsubscribe", "measurement")
