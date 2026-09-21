from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
import sys
from unittest.mock import Mock

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nexus_n3.sensor_manager.adapter_pool import AdapterPool
from nexus_n3.sensor_manager.adapters.wifi.config import (
    ApAddressMode,
    WifiRuntimeConfig,
)
from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


def _wifi_config() -> WifiRuntimeConfig:
    return WifiRuntimeConfig(
        enabled=True,
        backend="fake",
        ap_address_mode=ApAddressMode.NETWORKMANAGER_SHARED,
        expected_ap_cidr="10.42.0.1/24",
    )


def test_wifi_sensors_share_one_configured_adapter():
    async def scenario():
        config = _wifi_config()
        pool = AdapterPool(
            ble_runtime_config=BLERuntimeConfig(backend="bleak"),
            wifi_runtime_config=config,
        )
        first = pool.for_sensor(SimpleNamespace(adapter="WIFI"))
        second = pool.for_sensor(SimpleNamespace(adapter="wifi"))

        assert first is second
        assert first.config is config
        assert list(pool.adapters) == ["WIFI"]

        await pool.initialize_all()
        await pool.initialize_all()
        assert first.backend.operations == ["initialize", "ensure_ap_active"]

        await pool.shutdown_all()
        await pool.shutdown_all()
        assert first.backend.operations[-1] == "shutdown"
        assert first.backend.operations.count("shutdown") == 1

    asyncio.run(scenario())


def test_reset_shuts_down_old_adapter_before_initializing_replacement():
    async def scenario():
        pool = AdapterPool(
            ble_runtime_config=BLERuntimeConfig(backend="bleak"),
            wifi_runtime_config=_wifi_config(),
        )
        old_adapter = pool.get_or_create("WIFI")
        await pool.initialize_all()

        pool.reset()
        new_adapter = pool.get_or_create("WIFI")
        assert new_adapter is not old_adapter

        await pool.initialize_all()

        assert old_adapter.backend.operations[-1] == "shutdown"
        assert new_adapter.backend.operations[:2] == [
            "initialize",
            "ensure_ap_active",
        ]
        await pool.shutdown_all()

    asyncio.run(scenario())


class _WifiSensor:
    adapter = "WIFI"
    name = "Test WiFi Sensor"
    listeners = {}
    spec = {}


def test_sensor_manager_initializes_wifi_before_later_queued_commands(monkeypatch):
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.platform.system",
        lambda: "Darwin",
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.threading.Thread",
        Mock(return_value=Mock()),
    )
    manager = SensorManager(
        ble_runtime_config=BLERuntimeConfig(backend="bleak"),
        wifi_runtime_config=_wifi_config(),
    )

    manager.init_sensor_manager([_WifiSensor()])
    adapter = manager.adapters["WIFI"]

    # Queue stop after the initialization command, then drive the same manager
    # loop deterministically without starting background threads in the test.
    manager.loop.call_soon_threadsafe(
        manager.queue.put_nowait,
        {"message": "__stop__"},
    )
    manager.loop.run_until_complete(manager._manager_loop())

    assert adapter.backend.operations[:2] == ["initialize", "ensure_ap_active"]

    class CompletedCall:
        @staticmethod
        def result(timeout=None):
            return None

    def run_on_manager_loop(coroutine, loop):
        loop.run_until_complete(coroutine)
        return CompletedCall()

    shutdown_before_join = []
    manager.thread.join.side_effect = lambda timeout=None: shutdown_before_join.append(
        adapter.backend.operations[-1]
    )
    monkeypatch.setattr(
        "nexus_n3.sensor_manager.SensorManager.asyncio.run_coroutine_threadsafe",
        run_on_manager_loop,
    )

    manager.stop_manager()

    assert adapter.backend.operations[-1] == "shutdown"
    assert shutdown_before_join == ["shutdown"]
    manager.loop.close()
