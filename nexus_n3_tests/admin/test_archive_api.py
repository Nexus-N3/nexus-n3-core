from pathlib import Path

import httpx
import pytest

from nexus_n3.admin.app import AdminState, create_app


def _client(tmp_path, monkeypatch, status=None):
    output_root = tmp_path / "outputs"
    monkeypatch.setenv("NEXUS_N3_OUTPUT_ROOT", str(output_root))
    state = AdminState(
        project_root=Path(__file__).resolve().parents[2],
        role="standalone",
        site="test",
        gateway_name="zeromq_gateway",
        server_status_provider=lambda: status or {"usb_disk": {"present": False, "path": None}},
    )
    return create_app(state), output_root


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_archive_list_and_download_contract(tmp_path, monkeypatch):
    app, output_root = _client(tmp_path, monkeypatch)
    archive_bytes = b"PK-test-archive"
    archive_root = output_root / "test" / "sessions"
    archive_root.mkdir(parents=True)
    (archive_root / "session.zip").write_bytes(archive_bytes)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        listing = await client.get("/api/outputs", params={"site": "test"})
        assert listing.status_code == 200
        assert listing.headers["cache-control"] == "no-store"
        payload = listing.json()
        assert payload["storage_source"] == "internal"
        assert payload["site"] == "test"
        assert payload["archives"][0]["filename"] == "session.zip"
        assert "path" not in payload["archives"][0]
        params = {
            "archive_id": payload["archives"][0]["id"],
            "storage_source": "internal",
            "site": "test",
        }
        preflight = await client.head("/api/outputs/download", params=params)

    assert preflight.status_code == 200
    assert preflight.headers["content-length"] == str(len(archive_bytes))


@pytest.mark.anyio
async def test_archive_download_reports_source_change(tmp_path, monkeypatch):
    usb = tmp_path / "usb"
    usb.mkdir()
    app, _ = _client(
        tmp_path,
        monkeypatch,
        {"usb_disk": {"present": True, "path": str(usb)}},
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            "/api/outputs/download",
            params={
                "archive_id": "c2Vzc2lvbi56aXA",
                "storage_source": "internal",
                "site": "test",
            },
        )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "archive_source_changed"


@pytest.mark.anyio
async def test_archive_list_rejects_a_stale_site(tmp_path, monkeypatch):
    app, _ = _client(tmp_path, monkeypatch)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/outputs", params={"site": "old-site"})

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "archive_site_changed"
