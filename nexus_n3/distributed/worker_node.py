"""Worker node implementation for distributed Nexus N3 Core."""
from pathlib import Path
import zmq
import threading
import json
import socket
import time
import subprocess
import os
import weakref
import gc
from zeroconf import Zeroconf, ServiceBrowser
from nexus_n3.distributed.capabilities import build_node_capabilities
from nexus_n3.distributed.registry import NodeRegistry
from nexus_n3.distributed.registry_messages import AI_NODE_REGISTRY
from nexus_n3.gateway.event_bus.system_event_bus import SystemEventBus
from nexus_n3.gateway.messaging.message_handler import MessageHandler
from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.distributed.shared_utils import get_local_ip



logger = get_module_logger("WorkerNode")


class MasterListener:
    """mDNS listener that captures master IP and port."""
    def __init__(self):
        """Initialize empty discovery fields."""
        self.master_ip = None
        self.master_port = None

    def add_service(self, zc, type_, name):
        """Handle service add events from Zeroconf."""
        info = zc.get_service_info(type_, name)
        if info:
            self.master_ip = socket.inet_ntoa(info.addresses[0])
            self.master_port = info.port

    def update_service(self, zc, type_, name):
        """Handle service update events (unused)."""
        pass

    def remove_service(self, zc, type_, name):
        """Handle service remove events (unused)."""
        pass


def discover_master(timeout=15):
    """
    Discover the master node via mDNS.

    Args:
        timeout: Max seconds to wait for discovery.

    Returns:
        Tuple of (master_ip, master_port).

    Raises:
        RuntimeError: If the master is not found.
    """
    zc = Zeroconf()
    listener = MasterListener()
    browser = ServiceBrowser(zc, "_nexusn3._tcp.local.", listener)

    wr = weakref.ref(browser)
    t0 = time.time()
    while time.time() - t0 < timeout:
        if wr() is None:
            print("ServiceBrowser GC'D")
        if listener.master_ip:
            return listener.master_ip, listener.master_port
        time.sleep(0.1)

    raise RuntimeError("Master not found via mDNS")


class WorkerNode:
    """
    Worker node for Nexus N3 Core.

    Sends periodic heartbeats to the master when idle so `last_seen` stays fresh.
    """

    def __init__(
        self,
        node_id: str,
        site: str,
        *,
        customer_id: str | None = None,
        site_id: str | None = None,
        site_name: str | None = None,
        registry: NodeRegistry = None,
    ):
        """
        Initialize a worker node.

        Args:
            node_id: Unique worker identifier.
            site: Site identifier for data storage.
            registry: Optional NodeRegistry instance.
        """
        self.node_id = node_id
        self.site = site
        self.registry = registry or NodeRegistry()
        self.capabilities = build_node_capabilities(
            include_sensors=True,
            include_algorithms=True,
        )
        self.master_ip = None
        self.master_port = None

        # ZeroMQ context & DEALER socket
        self.ctx = zmq.Context()
        
        self.dealer = None

        # Event bus for emitting system events
        self.system_event_bus = SystemEventBus(
            deployment_context={
                "customer_id": customer_id,
                "site_id": site_id or site,
                "site_name": site_name or site,
            }
        )
        self.system_event_bus.subscribe(self.send_event)
        self.handler = MessageHandler(self.site, self.system_event_bus)
        self.handler.registry = self.registry

        self._running = False
        self._recv_thread = None
        self._heartbeat_thread = None
        self._last_sent = 0.0
        self._last_recv = 0.0
        self._heartbeat_interval = 30
        self._reconnect_timeout = 90

        # Worker metadata
        self.ip = get_local_ip()

        # worker mount path
        self._exports_path = Path("/exports")
        self._exports_path.mkdir(parents=True, exist_ok=True)

    def start(self):
        """
        Start the worker node.

        Discovers the master, registers over ZMQ, and starts the receive loop.
        """
        print(f"[WORKER] Worker '{self.node_id}' started and registering with master.")
        self._running = True
        result = {}

        def discover():
            """Resolve master address via mDNS in a worker thread."""
            result["addr"] = discover_master()

        t = threading.Thread(target=discover, daemon=True)
        t.start()
        t.join(timeout=5)

        if "addr" not in result:
            raise RuntimeError("Master not found via mDNS")

        self.master_ip, self.master_port = result["addr"]
        logger.info(f"[WORKER {self.node_id}] Master found at {self.master_ip}")
        print(f"[WORKER {self.node_id}] Master found at {self.master_ip}")

        
        # Setup DEALER socket with ZMQ_IDENTITY
        self.dealer = self.ctx.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IDENTITY, self.node_id.encode())
        #self.dealer.setsockopt(zmq.IMMEDIATE, 1)
        #self.dealer.setsockopt(zmq.LINGER, 0)

        try:
            self.dealer.connect(f"tcp://{self.master_ip}:{self.master_port}")  # master ROUTER port
        except Exception as e:
            print(f"[WORKER {self.node_id}] Failed to connect to master: {e}")

        # Start background thread to receive messages
        
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()
        time.sleep(2)
        # Register with master
        self._register_with_master()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        self._last_recv = time.time()

        logger.info(f"[WORKER {self.node_id}] WorkerNode started.")

    def stop(self):
        """Stop the worker node and close sockets."""
        self._running = False
        if self.dealer:
            self.dealer.setsockopt(zmq.LINGER, 0)  # flush unsent messages immediately
            self.dealer.close()
        self.ctx.term()
        logger.info(f"[WORKER {self.node_id}] WorkerNode stopped.")


    # ------------------- Registration -------------------
    def _register_with_master(self):
        """Send a registration message to the master."""
        print("[WORKER] sending REGISTER", flush=True)
        #self.dealer.send_multipart([zmq.IDENTITY, json.dumps({
        #    "type": "REGISTER",
        #    "node_id": self.node_id,
        #    "ip": self.ip
        #}).encode()])
        self.dealer.send_json({
            "type": "REGISTER",
            "node_id": self.node_id,
            "ip": self.ip,
            "role": "worker",
            "capabilities": self.capabilities,
        })
        self._record_sent()
        
        print("[WORKER] REGISTER sent", flush=True)

    def _heartbeat_loop(self):
        """Send lightweight heartbeats when idle."""
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
                    logger.error(f"[WORKER {self.node_id}] Heartbeat failed: {exc}")
            time.sleep(1)

    def _record_sent(self):
        self._last_sent = time.time()
    
    def _record_recv(self):
        self._last_recv = time.time()

    def _reconnect_to_master(self):
        """Reconnect to master if connection appears stale."""
        try:
            logger.info(f"[WORKER {self.node_id}] Reconnecting to master...")
            if self.dealer:
                self.dealer.setsockopt(zmq.LINGER, 0)
                self.dealer.close()
            result = discover_master()
            self.master_ip, self.master_port = result
            self.dealer = self.ctx.socket(zmq.DEALER)
            self.dealer.setsockopt(zmq.IDENTITY, self.node_id.encode())
            self.dealer.connect(f"tcp://{self.master_ip}:{self.master_port}")
            self._register_with_master()
            logger.info(f"[WORKER {self.node_id}] Reconnected to master at {self.master_ip}:{self.master_port}")
            self._last_recv = time.time()
            return True
        except Exception as exc:
            logger.error(f"[WORKER {self.node_id}] Reconnect failed: {exc}")
            return False

    # ------------------- Receiving commands -------------------
    def _recv_loop(self):
        """Receive messages from the master and dispatch to the handler."""
        poller = zmq.Poller()
        poller.register(self.dealer, zmq.POLLIN)
        while self._running:
            try:
                events = dict(poller.poll(timeout=1000))
                if self.dealer in events:
                    msg_frames = self.dealer.recv_multipart()
                    msg_bytes = next((f for f in msg_frames if f), b"")
                    if not msg_bytes:
                        continue
                    msg = json.loads(msg_bytes.decode())
                    self._record_recv()
                    if msg.get("type") == "REGISTER_ACK":
                        # we need to do the setup
                        self.handler.handle({
                            "type": mt.CMD_SYSTEM_SETUP,
                            "payload": {"file_path": msg.get("usb_path")}
                        })

                    elif msg.get("type") == AI_NODE_REGISTRY:
                        payload = msg.get("payload") or {}
                        ai_nodes = payload.get("nodes") or {}
                        self.registry.set_ai_nodes(ai_nodes)

                    elif msg.get("type") == mt.CMD_UPDATE_FILE_PATH:
                        # Always forward to handler so fallback logic applies
                        self.handler.handle(msg)

                    else:
                        self.handler.handle(msg)
                else:
                    if self._last_recv and (time.time() - self._last_recv) > self._reconnect_timeout:
                        if self._reconnect_to_master():
                            poller = zmq.Poller()
                            poller.register(self.dealer, zmq.POLLIN)
            #except zmq.error.Again:
            #    continue
            except Exception as e:
                logger.error(f"[WORKER {self.node_id}] Error receiving message: {e}")

    # ------------------- Sending events/results -------------------
    def send_event(self, event: dict):
        """
        Send a system event back to the master.

        Args:
            event: Event dictionary with type and payload.
        """
        if not self._running:
            return
    
        event["node_id"] = self.node_id
        logger.info(f"emiting system event from {self.node_id}:  {event}")
        msg_bytes = json.dumps(event).encode()
        self.dealer.send(msg_bytes)
        self._record_sent()
        logger.info(f"[WORKER {self.node_id}] Sent event to master: {event}")
