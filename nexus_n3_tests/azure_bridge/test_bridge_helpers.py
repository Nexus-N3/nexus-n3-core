from nexus_n3.azure_bridge.config import AzureBridgeConfig
from nexus_n3.azure_bridge.bridge import METHOD_NAME_TO_COMMAND, SAFE_READ_METHODS
from nexus_n3.core.runtime_env import reset_runtime_env
from nexus_n3.azure_bridge.state_store import BridgeStateStore
from nexus_n3.azure_bridge.telemetry_mapper import build_method_response_payload, map_event_for_cloud
from nexus_n3.gateway.messaging import message_types as mt


def test_map_event_for_cloud_preserves_shape_and_adds_metadata():
    event = {
        "type": "compute_result",
        "payload": {"value": 1},
        "site": "local-lab",
    }

    telemetry = map_event_for_cloud(
        event,
        device_id="device-1",
        customer_id="customer-dlr",
        site_id="site-lunar-facility",
        site="ignored-site",
        correlation_id="abc",
    )

    assert telemetry["type"] == "compute_result"
    assert telemetry["payload"] == {"value": 1}
    assert telemetry["site"] == "local-lab"
    assert telemetry["device_id"] == "device-1"
    assert telemetry["customer_id"] == "customer-dlr"
    assert telemetry["site_id"] == "site-lunar-facility"
    assert telemetry["correlation_id"] == "abc"
    assert "timestamp" in telemetry


def test_build_method_response_payload_is_consistent():
    payload = build_method_response_payload(status=403, message="blocked", correlation_id="abc", extra={"x": 1})

    assert payload == {
        "status": 403,
        "message": "blocked",
        "correlation_id": "abc",
        "x": 1,
    }


def test_state_store_can_run_without_file():
    store = BridgeStateStore()

    state = store.update(device_lock_state="locked_local_session", lock_owner="local_ui")

    assert state.device_lock_state == "locked_local_session"
    assert store.snapshot()["lock_owner"] == "local_ui"


def test_get_device_info_is_allowed_as_safe_read_method():
    assert mt.CMD_GET_DEVICE_INFO in SAFE_READ_METHODS
    assert METHOD_NAME_TO_COMMAND[mt.CMD_GET_DEVICE_INFO] == mt.CMD_GET_DEVICE_INFO


def test_control_center_direct_method_maps_to_local_forward_command():
    assert METHOD_NAME_TO_COMMAND[mt.EVT_CONTROL_CENTER_MESSAGE] == mt.CMD_FORWARD_CONTROL_CENTER_MESSAGE


def test_config_bool_parsing_from_env(monkeypatch):
    monkeypatch.setenv("AZURE_IOT_CONNECTION_STRING", "HostName=test;DeviceId=d1;SharedAccessKey=abc")
    monkeypatch.setenv("AZURE_IOT_DEVICE_ID", "d1")
    monkeypatch.setenv("AZURE_IOT_CUSTOMER_ID", "customer-dlr")
    monkeypatch.setenv("AZURE_IOT_SITE_ID", "site-lunar-facility")
    monkeypatch.setenv("AZURE_IOT_SITE_NAME", "Lunar Facility")
    monkeypatch.setenv("AZURE_BRIDGE_REMOTE_CONTROL_ENABLED", "true")
    monkeypatch.setenv("AZURE_BRIDGE_USE_WEBSOCKETS", "true")
    monkeypatch.setenv("AZURE_BRIDGE_KEEP_ALIVE", "120")
    monkeypatch.setenv("AZURE_BRIDGE_CONNECTION_RETRY_INTERVAL", "15")

    config = AzureBridgeConfig.from_env()

    assert config.remote_control_enabled is True
    assert config.device_id == "d1"
    assert config.customer_id == "customer-dlr"
    assert config.site_id == "site-lunar-facility"
    assert config.site_name == "Lunar Facility"
    assert config.websockets is True
    assert config.keep_alive == 120
    assert config.connection_retry_interval == 15


def test_config_loads_shared_runtime_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "AZURE_IOT_CONNECTION_STRING=HostName=test;DeviceId=d2;SharedAccessKey=xyz\n"
        "AZURE_IOT_DEVICE_ID=d2\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("AZURE_IOT_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_IOT_DEVICE_ID", raising=False)
    monkeypatch.setenv("NEXUS_N3_ENV_FILE", str(env_file))
    reset_runtime_env()

    config = AzureBridgeConfig.from_env()

    assert config.device_id == "d2"
