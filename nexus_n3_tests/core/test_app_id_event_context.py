from nexus_n3.core.core import Core


class _StubFileManager:
    session_label = None
    session_name = None


class _StubStorage:
    file_manager = _StubFileManager()


def _make_core() -> Core:
    core = Core.__new__(Core)
    core.site = "local-test-site"
    core.session_timestamp = None
    core.app_id = None
    core.app_name = None
    core.storage = _StubStorage()
    return core


def test_event_context_omits_app_identity_when_unset() -> None:
    core = _make_core()

    payload = core._event_context()

    assert "app_id" not in payload
    assert "app_name" not in payload


def test_event_context_includes_app_identity_when_present() -> None:
    core = _make_core()
    core.app_id = "nexus"
    core.app_name = "Nexus"

    payload = core._event_context()

    assert payload["app_id"] == "nexus"
    assert payload["app_name"] == "Nexus"
