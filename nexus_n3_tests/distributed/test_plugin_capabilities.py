from nexus_n3.compute_manager.remote_compute_service import RemoteComputeService
from nexus_n3.distributed.ai_compute_node import AiComputeNode
from nexus_n3.distributed.master_node import MasterNode
from nexus_n3.distributed.registry import NodeRegistry


class _UsbStub:
    network_path = None


class _FakeRemoteResult:
    def __init__(self, payload):
        self._payload = payload

    def to_dict(self):
        return dict(self._payload)


class _FakeAlgorithmClient:
    def __init__(self, results):
        self.results = results
        self.calls = []

    def run_batch(self, **kwargs):
        self.calls.append(kwargs)
        return self.results


class _FakePluginRuntime:
    def __init__(self, client=None):
        self.client = client
        self.requested = []

    def get_algorithm_client(self, algorithm_name):
        self.requested.append(algorithm_name)
        return self.client

    def close(self):
        return None


def test_remote_compute_service_selects_ai_node_by_algorithm():
    registry = NodeRegistry()
    registry.register_node(
        "ai-1",
        ip="10.0.0.10",
        role="ai",
        compute_port=7001,
        capabilities={"supported_algorithms": ["algo-a"]},
    )
    registry.register_node(
        "ai-2",
        ip="10.0.0.11",
        role="ai",
        compute_port=7002,
        capabilities={"supported_algorithms": ["algo-b"]},
    )

    service = RemoteComputeService(on_result=lambda _result: None)
    service.set_registry(registry)

    assert service._select_ai_endpoint("algo-b") == "tcp://10.0.0.11:7002"
    assert service._select_ai_endpoint("missing") is None


def test_ai_compute_node_uses_plugin_runtime(monkeypatch):
    monkeypatch.setattr(
        "nexus_n3.distributed.ai_compute_node.get_local_ip",
        lambda: "127.0.0.1",
    )
    fake_client = _FakeAlgorithmClient(
        [
            _FakeRemoteResult(
                {
                    "algorithm_name": "algo-a",
                    "address": "sensor-1",
                    "value": 42,
                }
            )
        ]
    )
    fake_runtime = _FakePluginRuntime(fake_client)
    monkeypatch.setattr(
        "nexus_n3.distributed.ai_compute_node._build_plugin_runtime",
        lambda: fake_runtime,
    )
    node = AiComputeNode("ai-test", capabilities={"supported_algorithms": ["algo-a"]})

    response = node._handle_compute(
        {
            "request_id": "req-1",
            "algorithm_name": "algo-a",
            "samples": [{"timestamp": 1, "address": "sensor-1"}],
            "sampling_rate": 100,
            "input_parameters": {"alpha": 1},
            "address": "sensor-1",
            "subject_id": "subject-1",
            "location": "chest",
        }
    )

    assert response["algorithm_name"] == "algo-a"
    assert response["result"]["value"] == 42
    assert fake_runtime.requested == ["algo-a"]
    assert fake_client.calls[0]["subject_id"] == "subject-1"


def test_master_assign_subjects_requires_node_capabilities(monkeypatch):
    monkeypatch.setattr(
        "nexus_n3.distributed.master_node.get_local_ip",
        lambda: "127.0.0.1",
    )
    registry = NodeRegistry()
    master = MasterNode(
        registry,
        _UsbStub(),
        router_port=6123,
        local_capabilities={
            "supported_sensors": ["movella-dot"],
            "supported_algorithms": ["standard-loading-intensity", "pass_through"],
        },
    )
    try:
        registry.register_node(
            "worker-1",
            ip="10.0.0.20",
            role="worker",
            identity=b"worker-1",
            capabilities={
                "supported_sensors": ["movesense"],
                "supported_algorithms": ["pass_through"],
            },
        )

        assigned = master.assign_subjects(
            [
                {
                    "subject_id": "subject-1",
                    "sensors": [
                        {
                            "local_name": "movella-dot",
                            "number_of": 1,
                            "compute_algorithm": {"name": "standard-loading-intensity"},
                        }
                    ],
                }
            ]
        )

        assert assigned[0]["subject_id"] == "subject-1"
        assert registry.get_subjects()["subject-1"]["assigned_node"] == "master"
    finally:
        master.stop()
