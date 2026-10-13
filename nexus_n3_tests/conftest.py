"""Pytest must never remove paired/cached devices from the host's BlueZ service."""
import pytest

from nexus_n3.sensor_manager.utils import utils


@pytest.fixture(autouse=True)
def isolate_host_bluetooth(monkeypatch):
    monkeypatch.setattr(utils, "remove_all_devices", lambda: None)
    monkeypatch.setattr(utils, "remove_devices", lambda *args, **kwargs: None)
