"""Latest-value routing under blocked consumers and lifecycle transitions."""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.sensor_handle import SensorBase


class RoutingSensor(SensorBase):
    sensor_type = type("SensorType", (), {"local_name": "Routing Test"})()

    def __init__(self, address, *, consumer=False):
        super().__init__(self.sensor_type, {
            "sensor": {"name": "Routing Test", "adapter": "BLE"},
            "events": ["on_data"],
            "inputs": [{"name": "value"}] if consumer else [],
            "outputs": [],
        })
        self.address = address
        self.routing_outputs = [] if consumer else [{"name": "value"}]
        self.consumed = []

    def consume_input(self, source_plugin_id, envelope):
        self.consumed.append((envelope.source_address, envelope.payload.value))


class NoopAdapter:
    adapter_type = "BLE"

    def close(self):
        pass


@pytest.fixture
def routing_manager(monkeypatch):
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.utils.remove_all_devices", lambda: None,
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class",
        lambda *args, **kwargs: NoopAdapter,
    )
    manager = SensorManager()
    manager._routing_executor.submit = Mock(wraps=manager._routing_executor.submit)
    source = RoutingSensor("source")
    target = RoutingSensor("target", consumer=True)
    manager.init_sensor_manager([source, target])
    manager.enable_routing()
    yield manager, source, target
    manager.stop_manager()
    manager.loop.close()


def emit(manager, source, value):
    manager._emit_to_client("on_data", {"address": source.address, "value": value})


def drain(manager):
    # Keep routing enabled: this must process (rather than discard) pending data.
    with manager._routing_condition:
        assert manager._routing_condition.wait_for(
            lambda: manager._active_routed_calls == 0, timeout=5,
        )
        assert manager._routing_pending == {}


@pytest.mark.parametrize("async_consumer", [False, True])
def test_concurrent_producers_coalesce_before_submission(routing_manager, async_consumer):
    manager, source, target = routing_manager
    started, release = threading.Event(), threading.Event()
    consume = target.consume_input
    client_samples = []
    manager.register_listener("on_data", client_samples.append)

    def blocking_consume(plugin_id, envelope):
        consume(plugin_id, envelope)
        if envelope.payload.value == 0:
            started.set()
            assert release.wait(5)

    async def async_consume(plugin_id, envelope):
        await asyncio.to_thread(blocking_consume, plugin_id, envelope)

    target.consume_input = async_consume if async_consumer else blocking_consume
    try:
        emit(manager, source, 0)
        assert started.wait(5)
        with ThreadPoolExecutor(max_workers=8) as producers:
            list(producers.map(lambda value: emit(manager, source, value), range(1, 201)))
        emit(manager, source, 999)
        assert manager._routing_executor.submit.call_count == 1
        assert manager._active_routed_calls == 1
        assert len(manager._routing_pending) == 1
        release.set()
        drain(manager)
        assert target.consumed == [("source", 0), ("source", 999)]
        assert manager._routing_executor.submit.call_count == 2
        # A retired route can start again.
        emit(manager, source, 1000)
        drain(manager)
        assert target.consumed[-1] == ("source", 1000)
        assert len(client_samples) == 203
    finally:
        release.set()


def test_queued_routes_coalesce_independently_and_yield_worker(routing_manager):
    manager, source, target = routing_manager
    source2 = RoutingSensor("source2")
    target2 = RoutingSensor("target2", consumer=True)
    manager.init_sensor_manager([source, source2, target, target2])
    manager.enable_routing()
    started, release = threading.Event(), threading.Event()
    consume = target.consume_input

    def blocking_consume(plugin_id, envelope):
        consume(plugin_id, envelope)
        if envelope.payload.value == 0:
            started.set()
            assert release.wait(5)

    target.consume_input = blocking_consume
    try:
        emit(manager, source, 0)
        assert started.wait(5)
        for value in range(1, 101):
            emit(manager, source, value)
            emit(manager, source2, value + 1000)
        assert manager._routing_executor.submit.call_count == 4
        assert manager._active_routed_calls == 4
        release.set()
        drain(manager)
        assert target.consumed == [("source", 0), ("source2", 1100), ("source", 100)]
        assert target2.consumed == [("source", 100), ("source2", 1100)]
        assert manager._routing_executor.submit.call_count == 5
    finally:
        release.set()


@pytest.mark.parametrize("action", [
    "disable_routing", "stop_all", "stop_specific_sensors", "reinitialize", "stop_manager",
])
def test_lifecycle_discards_pending_and_waits_for_active(routing_manager, monkeypatch, action):
    manager, source, target = routing_manager
    started, release, quiesced = (threading.Event() for _ in range(3))
    consume = target.consume_input

    def blocking_consume(plugin_id, envelope):
        consume(plugin_id, envelope)
        started.set()
        assert release.wait(5)

    target.consume_input = blocking_consume
    target2 = RoutingSensor("target2", consumer=True)
    manager.init_sensor_manager([source, target, target2])
    manager.enable_routing()
    original_quiesce = manager._quiesce_routing

    def quiesce():
        original_quiesce()
        quiesced.set()

    monkeypatch.setattr(manager, "_quiesce_routing", quiesce)
    dispatched = threading.Event()

    async def dispatch(message):
        dispatched.set()
        return True

    manager.controller.dispatch = dispatch
    new_source = RoutingSensor("source")
    new_target = RoutingSensor("new-target", consumer=True)

    def transition():
        if action == "reinitialize":
            manager.init_sensor_manager([new_source, new_target])
        elif action == "stop_specific_sensors":
            manager.stop_specific_sensors([target.address]).result(timeout=5)
        elif action == "stop_all":
            manager.stop_all().result(timeout=5)
        else:
            getattr(manager, action)()

    try:
        emit(manager, source, 0)
        assert started.wait(5)
        for value in range(1, 101):
            emit(manager, source, value)
        with ThreadPoolExecutor(max_workers=1) as lifecycle:
            completion = lifecycle.submit(transition)
            assert quiesced.wait(5)
            with manager._routing_condition:
                assert all(value is None for value in manager._routing_pending.values())
            emit(manager, source, 999)
            if action != "disable_routing":
                assert not completion.done()
                assert not dispatched.is_set()
            release.set()
            completion.result(timeout=5)
        drain(manager)
        assert target.consumed == [("source", 0)]
        assert target2.consumed == []  # A queued call never became active.
        assert manager._routing_executor.submit.call_count == 2
        if action != "stop_manager":
            manager.enable_routing()
            emit(manager, new_source if action == "reinitialize" else source, 1000)
            drain(manager)
            consumer = new_target if action == "reinitialize" else target
            assert consumer.consumed[-1] == ("source", 1000)
            if action == "reinitialize":
                assert target.consumed == [("source", 0)]
    finally:
        release.set()


def test_consumer_failure_still_delivers_latest_pending(routing_manager):
    manager, source, target = routing_manager
    started, release = threading.Event(), threading.Event()
    consume = target.consume_input

    def fail_first(plugin_id, envelope):
        if envelope.payload.value == 0:
            started.set()
            assert release.wait(5)
            raise TimeoutError("consumer timed out")
        consume(plugin_id, envelope)

    target.consume_input = fail_first
    try:
        emit(manager, source, 0)
        assert started.wait(5)
        emit(manager, source, 1)
        emit(manager, source, 2)
        release.set()
        drain(manager)
        assert target.consumed == [("source", 2)]
    finally:
        release.set()


def test_submission_failure_retires_route_and_allows_retry(routing_manager):
    manager, source, target = routing_manager
    submit = manager._routing_executor.submit
    submit.side_effect = RuntimeError("executor unavailable")
    with pytest.raises(RuntimeError, match="executor unavailable"):
        emit(manager, source, 1)
    drain(manager)
    submit.side_effect = None
    emit(manager, source, 2)
    drain(manager)
    assert target.consumed == [("source", 2)]


def test_reenable_while_active_accepts_only_fresh_pending_value(routing_manager):
    manager, source, target = routing_manager
    started, release = threading.Event(), threading.Event()
    consume = target.consume_input

    def blocking_consume(plugin_id, envelope):
        consume(plugin_id, envelope)
        if envelope.payload.value == 0:
            started.set()
            assert release.wait(5)

    target.consume_input = blocking_consume
    try:
        emit(manager, source, 0)
        assert started.wait(5)
        emit(manager, source, 1)
        manager.disable_routing()
        emit(manager, source, 2)
        manager.enable_routing()
        emit(manager, source, 3)
        emit(manager, source, 4)
        assert manager._routing_executor.submit.call_count == 1
        release.set()
        drain(manager)
        assert target.consumed == [("source", 0), ("source", 4)]
    finally:
        release.set()


def test_values_arriving_during_route_retirement_are_not_lost(routing_manager):
    manager, source, target = routing_manager
    # Fast consumers exercise updates racing with retirement/re-submission.
    # Every completed batch must eventually deliver its final value.
    with ThreadPoolExecutor(max_workers=8) as producers:
        for batch in range(20):
            list(producers.map(
                lambda value: emit(manager, source, value), range(100),
            ))
            emit(manager, source, 1000 + batch)
            drain(manager)
            assert target.consumed[-1] == ("source", 1000 + batch)


def test_resubmission_failure_retires_route(routing_manager):
    manager, source, target = routing_manager
    started, release = threading.Event(), threading.Event()
    consume = target.consume_input

    def blocking_consume(plugin_id, envelope):
        consume(plugin_id, envelope)
        if envelope.payload.value == 0:
            started.set()
            assert release.wait(5)

    target.consume_input = blocking_consume
    submit = manager._routing_executor.submit
    try:
        emit(manager, source, 0)
        assert started.wait(5)
        emit(manager, source, 1)
        submit.side_effect = RuntimeError("executor unavailable")
        release.set()
        drain(manager)
        assert target.consumed == [("source", 0)]
        submit.side_effect = None
        emit(manager, source, 2)
        drain(manager)
        assert target.consumed == [("source", 0), ("source", 2)]
    finally:
        release.set()
