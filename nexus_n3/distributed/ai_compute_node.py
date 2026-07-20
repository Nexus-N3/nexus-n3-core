"""AI compute-only node for direct algorithm execution."""

import pickle
import os
import socket
import threading
import time
import zmq
from zeroconf import Zeroconf, ServiceBrowser

from nexus_n3.distributed.capabilities import build_node_capabilities
from nexus_n3.distributed.shared_utils import get_local_ip
from nexus_n3.logger.logger import get_module_logger

logger = get_module_logger("AI Compute Node")


class MasterListener:
    """mDNS listener that captures master IP and port."""
    def __init__(self):
        self.master_ip = None
        self.master_port = None

    def add_service(self, zc, type_, name):
        info = zc.get_service_info(type_, name)
        if info:
            self.master_ip = socket.inet_ntoa(info.addresses[0])
            self.master_port = info.port

    def update_service(self, zc, type_, name):
        pass

    def remove_service(self, zc, type_, name):
        pass


def discover_master(timeout=15):
    """Discover the master node via mDNS."""
    zc = Zeroconf()
    listener = MasterListener()
    ServiceBrowser(zc, "_nexusn3._tcp.local.", listener)
    t0 = time.time()
    while time.time() - t0 < timeout:
        if listener.master_ip:
            return listener.master_ip, listener.master_port
        time.sleep(0.1)
    raise RuntimeError("Master not found via mDNS")


class AiComputeNode:
    """
    AI compute-only node.

    - Registers with master as role=ai
    - Exposes a direct ZeroMQ ROUTER endpoint for compute requests
    - Does not manage subjects or sensors
    """

    def __init__(self, node_id: str, compute_port: int = 7001, capabilities=None):
        self.node_id = node_id
        self.compute_port = compute_port
        self.capabilities = capabilities or build_node_capabilities(
            include_sensors=False,
            include_algorithms=True,
        )

        self.master_ip = None
        self.master_port = None
        self.ip = get_local_ip()

        self.ctx = zmq.Context()
        self.dealer = None
        self.compute_router = None

        self._running = False
        self._recv_thread = None
        self._heartbeat_thread = None
        self._compute_thread = None

        self._last_sent = 0.0
        self._heartbeat_interval = 30
        self._plugin_runtime = _build_plugin_runtime()
        self._perf_enabled = _env_flag("NEXUS_PERF_LOG", default=False)

    def start(self):
        """Start the AI compute node."""
        logger.info("[AI] Starting AI compute node")
        self._running = True

        self.master_ip, self.master_port = discover_master()
        logger.info("[AI] Master found at %s:%s", self.master_ip, self.master_port)

        # Register with master
        self.dealer = self.ctx.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IDENTITY, self.node_id.encode())
        self.dealer.connect(f"tcp://{self.master_ip}:{self.master_port}")
        self._register_with_master()

        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        # Start compute server
        self.compute_router = self.ctx.socket(zmq.ROUTER)
        self.compute_router.bind(f"tcp://*:{self.compute_port}")
        self._compute_thread = threading.Thread(target=self._compute_loop, daemon=True)
        self._compute_thread.start()

        logger.info("[AI] Compute router bound on port %s", self.compute_port)

    def stop(self):
        """Stop the AI compute node."""
        self._running = False
        if self.dealer:
            self.dealer.setsockopt(zmq.LINGER, 0)
            self.dealer.close()
        if self.compute_router:
            self.compute_router.setsockopt(zmq.LINGER, 0)
            self.compute_router.close()
        self._plugin_runtime.close()
        self.ctx.term()
        logger.info("[AI] AI compute node stopped")

    def _register_with_master(self):
        self.dealer.send_json({
            "type": "REGISTER",
            "node_id": self.node_id,
            "ip": self.ip,
            "role": "ai",
            "compute_port": self.compute_port,
            "capabilities": self.capabilities,
        })
        self._record_sent()

    def _heartbeat_loop(self):
        while self._running:
            if time.time() - self._last_sent >= self._heartbeat_interval:
                try:
                    self.dealer.send_json({
                        "type": "HEARTBEAT",
                        "node_id": self.node_id,
                        "ts": time.time(),
                    })
                    self._record_sent()
                except Exception as exc:
                    logger.error("[AI] Heartbeat failed: %s", exc)
            time.sleep(1)

    def _record_sent(self):
        self._last_sent = time.time()

    def _compute_loop(self):
        poller = zmq.Poller()
        poller.register(self.compute_router, zmq.POLLIN)
        while self._running:
            try:
                events = dict(poller.poll(timeout=1000))
                if self.compute_router in events:
                    frames = self.compute_router.recv_multipart()
                    identity = frames[0]
                    msg_bytes = frames[-1]
                    msg = pickle.loads(msg_bytes)
                    response = self._handle_compute(msg)
                    self.compute_router.send_multipart(
                        [identity, b"", pickle.dumps(response)]
                    )
            except Exception as exc:
                logger.error("[AI] Compute loop error: %s", exc)

    def _handle_compute(self, msg: dict):
        request_id = msg.get("request_id")
        algo_name = msg.get("algorithm_name")
        samples = msg.get("samples") or []
        sampling_rate = msg.get("sampling_rate")
        input_parameters = msg.get("input_parameters")
        address = msg.get("address")
        subject_id = msg.get("subject_id")
        location = msg.get("location")

        try:
            runtime_client = self._plugin_runtime.get_algorithm_client(algo_name)
            if runtime_client is None:
                raise RuntimeError(f"AI compute node cannot resolve algorithm plugin '{algo_name}'")
            t0 = time.perf_counter()
            request_address = f"{address or 'remote'}::{request_id or time.time()}"
            results = runtime_client.run_batch(
                address=request_address,
                samples=samples,
                sampling_rate=sampling_rate,
                input_parameters=input_parameters,
                subject_id=subject_id,
                location=location,
            )
            if not results:
                raise RuntimeError("Algorithm produced no results")

            result = results[-1].to_dict()
            if self._perf_enabled:
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                logger.info(
                    "perf ai compute time algo=%s address=%s samples=%s ms=%.2f",
                    algo_name,
                    address,
                    len(samples),
                    elapsed_ms,
                )
            return {
                "type": "ALGO_RESULT",
                "request_id": request_id,
                "algorithm_name": algo_name,
                "result": result,
            }
        except Exception as exc:
            return {
                "type": "ALGO_RESULT",
                "request_id": request_id,
                "algorithm_name": algo_name,
                "status": "error",
                "error": str(exc),
            }


def _env_flag(name, default=False):
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _build_plugin_runtime():
    from nexus_n3.plugins.runtime.runtime import PluginRuntimeManager

    return PluginRuntimeManager()
