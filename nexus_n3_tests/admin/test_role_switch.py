from pathlib import Path

import httpx
import pytest

from nexus_n3.admin.app import AdminState, create_app
from nexus_n3.core.role_state import (
    load_role_override,
    resolve_role,
    save_role_override,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _app(*, role="standalone", idle=True, restart_handler=None):
    state = AdminState(
        project_root=Path(__file__).resolve().parents[2],
        role=role,
        site="test",
        gateway_name="zeromq_gateway",
        restart_handler=restart_handler,
        idle_provider=lambda: idle,
    )
    return create_app(state)


@pytest.mark.anyio
async def test_switch_role_uses_controlled_restart_when_idle():
    restarts = []
    app = _app(restart_handler=lambda bridge, role: restarts.append((bridge, role)))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.put("/api/server/role", json={"role": "master"})

    assert response.status_code == 200
    assert response.json() == {"role": "master", "restarting": True}
    assert restarts == [(None, "master")]


@pytest.mark.anyio
async def test_switch_role_rejects_busy_core_and_hidden_roles():
    app = _app(idle=False, restart_handler=lambda _bridge, _role: None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        busy = await client.put("/api/server/role", json={"role": "master"})
        worker = await client.put("/api/server/role", json={"role": "worker"})

    assert busy.status_code == 409
    assert worker.status_code == 400


def test_role_override_round_trip(tmp_path, monkeypatch):
    role_file = tmp_path / "role.json"
    monkeypatch.setenv("NEXUS_N3_ROLE_STATE_FILE", str(role_file))

    save_role_override("master")

    assert load_role_override() == "master"


def test_role_override_does_not_replace_worker_or_ai(tmp_path, monkeypatch):
    role_file = tmp_path / "role.json"
    monkeypatch.setenv("NEXUS_N3_ROLE_STATE_FILE", str(role_file))

    save_role_override("master")

    assert resolve_role("worker") == "worker"
    assert resolve_role("ai") == "ai"
    assert resolve_role("standalone") == "master"
    assert resolve_role("master") == "master"