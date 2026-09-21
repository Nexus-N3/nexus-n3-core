"""Master node for routing commands and aggregating events."""

import zmq
import threading
import json
import socket
import time
import uuid
from copy import deepcopy
from datetime import datetime
from zeroconf import Zeroconf, ServiceInfo
from nexus_n3.distributed.capabilities import build_node_capabilities, supports_algorithm, supports_sensor
from nexus_n3.distributed.registry import NodeRegistry
from nexus_n3.distributed.registry_messages import AI_NODE_REGISTRY
from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.distributed.shared_utils import get_local_ip
from nexus_n3.logger.logger import get_module_logger

logger = get_module_logger("Master Node")

class MasterNode:
    """
    Master node for Nexus N3 Core.

    Responsibilities:
        - Track registered worker nodes via NodeRegistry
        - Route incoming commands to workers (broadcast or targeted)
        - Receive results from workers and make them available for injection
          into SystemEventBus (handled by nexus_server)
        - Advertise itself via mDNS for worker discovery
        - Update worker last_seen on any inbound message (including heartbeats)
    """

    def __init__(
        self,
        registry: NodeRegistry,
        usb_disk_manager,
        router_port: int = 6000,
        mdns_port: int = 5556,
        mdns_hostname: str | None = None,
        local_capabilities: dict | None = None,
    ):
        """
        Initialize the master node.

        Args:
            registry (NodeRegistry): Tracks workers and subjects.
            router_port (int): TCP port to bind the internal ZeroMQ ROUTER for worker communication.
            mdns_port (int): Port for mDNS advertisement.
        """
        self.usb_disk_manager = usb_disk_manager
        self.registry = registry
        self.router_port = router_port
        self.mdns_port = mdns_port
        self.mdns_hostname = mdns_hostname
        self.system_event_bus = None

        self.MAX_TOTAL_SENSORS_PER_NODE = 8

        # ZeroMQ ROUTER for sending commands to workers
        self.ctx = zmq.Context()
        self.router = self.ctx.socket(zmq.ROUTER)
        self.router.bind(f"tcp://*:{self.router_port}")

        self._running = False
        self._router_thread = None

        # Zeroconf for mDNS
        self.zeroconf = None
        self.service_info = None
        self._mdns_thread = None
        self._self_heartbeat_thread = None
        self._last_ai_registry_broadcast = 0.0
        self._drain_lock = threading.RLock()
        self._drain_tracking = {}
        self._official_start_lock = threading.RLock()
        self._official_start_tracking = {}
        self._active_stream_subjects = set()
        self._after_all_streams_drained = None
        self._local_capabilities = local_capabilities or build_node_capabilities(
            include_sensors=True,
            include_algorithms=True,
        )

        # In MasterNode.__init__ or start()
        self.node_id = "master"  # fixed ID for the master node
        self.registry.register_node(
            node_id=self.node_id,
            ip=get_local_ip(),
            role="master",
            identity=None,  # no ZMQ identity needed if master doesn't route internally
            capabilities=self._local_capabilities,
        )
        print(f"[MASTER] Registered master node with ID '{self.node_id}' in NodeRegistry")
        logger.info(f"[MASTER] Registered master node with ID '{self.node_id}' in NodeRegistry")

    def set_after_all_streams_drained(self, callback):
        """Register a callback to run after the final active stream has drained."""
        self._after_all_streams_drained = callback

    def set_system_event_bus(self, system_event_bus):
        """Attach the master event bus and track local distributed lifecycle events."""
        self.system_event_bus = system_event_bus
        # Track master-local acknowledgements before the gateway publishes them.
        # A coordinating client may respond immediately to the final READY event.
        self.system_event_bus.subscribe(self._handle_local_system_event, prepend=True)

    def _handle_local_system_event(self, event: dict):
        """Handle master-local lifecycle events relevant to distributed coordination."""
        event_type = event.get("type")
        if (
            event_type == mt.EVT_STREAM_DRAINED
            and not event.get("node_id")
        ):
            self.record_local_stream_drained(event.get("payload"))
        elif event_type in {
            mt.EVT_STREAM_READY_FOR_OFFICIAL,
            mt.EVT_STREAM_OFFICIAL_STARTED,
        } and event.get("node_id", self.node_id) == self.node_id:
            self._record_official_start_event(self.node_id, event_type, event.get("payload"))

    def record_local_stream_drained(self, payload: dict | None):
        """Record the master's local stream-drained acknowledgement."""
        self._record_drain_ack(self.node_id, payload)

    def _create_official_start_record(
        self,
        start_session_id: str,
        *,
        session_timestamp: str,
        subject_ids: list[str],
        expected_nodes: list[str],
    ) -> None:
        """Snapshot the participants in one distributed official-start barrier."""
        with self._official_start_lock:
            self._official_start_tracking = {
                start_session_id: {
                    "session_timestamp": session_timestamp,
                    "subject_ids": list(subject_ids),
                    "expected_nodes": set(expected_nodes),
                    "ready_nodes": set(),
                    "official_nodes": set(),
                    "ready_announced": False,
                    "commit_dispatched": False,
                }
            }

    def _record_official_start_event(
        self,
        node_id: str | None,
        event_type: str,
        payload: dict | None,
    ) -> None:
        """Record node-local readiness and official-start acknowledgements."""
        payload = payload or {}
        start_session_id = payload.get("start_session_id")
        if not node_id or not start_session_id:
            return
        ready_payload = None
        with self._official_start_lock:
            record = self._official_start_tracking.get(start_session_id)
            if not record or node_id not in record["expected_nodes"]:
                return
            if str(payload.get("session_timestamp")) != str(record["session_timestamp"]):
                return
            if event_type == mt.EVT_STREAM_READY_FOR_OFFICIAL:
                record["ready_nodes"].add(node_id)
                if (
                    record["ready_nodes"] == record["expected_nodes"]
                    and not record["ready_announced"]
                ):
                    record["ready_announced"] = True
                    ready_payload = {
                        "start_session_id": start_session_id,
                        "session_timestamp": record["session_timestamp"],
                        "subject_ids": list(record["subject_ids"]),
                        "expected_nodes": sorted(record["expected_nodes"]),
                        "ready_nodes": sorted(record["ready_nodes"]),
                    }
            elif event_type == mt.EVT_STREAM_OFFICIAL_STARTED:
                record["official_nodes"].add(node_id)

        if ready_payload and self.system_event_bus:
            self.system_event_bus.emit(
                {
                    "type": mt.EVT_DISTRIBUTED_READY_FOR_OFFICIAL,
                    "node_id": self.node_id,
                    "payload": ready_payload,
                }
            )

    def _emit_dispatch_error(self, message: str, payload: dict | None = None) -> None:
        if not self.system_event_bus:
            return
        error_payload = {"message": message}
        correlation_id = (payload or {}).get("correlation_id")
        if correlation_id:
            error_payload["correlation_id"] = correlation_id
        self.system_event_bus.emit({"type": mt.EVT_ERROR, "payload": error_payload})

    def _create_drain_record(self, stop_session_id: str, *, scope: str, subject_ids: list[str], expected_nodes: list[str]):
        with self._drain_lock:
            self._drain_tracking[stop_session_id] = {
                "scope": scope,
                "subject_ids": list(subject_ids),
                "expected_nodes": set(expected_nodes),
                "acks": {},
                "finalizing": False,
                "finalized": False,
            }

    def _record_drain_ack(self, node_id: str, payload: dict | None):
        payload = payload or {}
        stop_session_id = payload.get("stop_session_id")
        if not stop_session_id:
            return
        all_streams_drained = False
        with self._drain_lock:
            record = self._drain_tracking.get(stop_session_id)
            if not record:
                return
            record["acks"][node_id] = {
                "status": payload.get("status", "ok"),
                "reason": payload.get("reason"),
                "subject_ids": list(payload.get("subject_ids") or []),
                "session_timestamp": payload.get("session_timestamp"),
                "drained": payload.get("drained", True),
            }

            expected_nodes = record["expected_nodes"]
            acked_nodes = set(record["acks"].keys())
            all_expected_acked = expected_nodes.issubset(acked_nodes)
            all_expected_drained = all(
                record["acks"][expected_node].get("drained", True)
                for expected_node in expected_nodes
                if expected_node in record["acks"]
            )
            should_finalize = (
                all_expected_acked
                and all_expected_drained
                and not record["finalizing"]
                and not record["finalized"]
            )
            if should_finalize:
                drained_subjects = set()
                for ack in record["acks"].values():
                    drained_subjects.update(ack.get("subject_ids", []))
                self._active_stream_subjects.difference_update(drained_subjects)
                all_streams_drained = not self._active_stream_subjects
                record["finalizing"] = all_streams_drained

                statuses = [ack.get("status", "ok") for ack in record["acks"].values()]
                reasons = [ack.get("reason") for ack in record["acks"].values() if ack.get("reason")]
                session_timestamps = {
                    ack.get("session_timestamp")
                    for ack in record["acks"].values()
                    if ack.get("session_timestamp")
                }
                drain_result = {
                    "stop_session_id": stop_session_id,
                    "status": "error" if any(status != "ok" for status in statuses) else "ok",
                    "reason": "; ".join(reasons) or None,
                    "session_timestamp": (
                        next(iter(session_timestamps))
                        if len(session_timestamps) == 1
                        else None
                    ),
                    "acks": deepcopy(record["acks"]),
                }

                if not all_streams_drained:
                    record["finalized"] = True

        if should_finalize and all_streams_drained and not self._after_all_streams_drained:
            with self._drain_lock:
                record = self._drain_tracking.get(stop_session_id)
                if record:
                    record["finalizing"] = False
                    record["finalized"] = True
            return

        if should_finalize and all_streams_drained and self._after_all_streams_drained:
            try:
                self._after_all_streams_drained(drain_result)
            except Exception:
                with self._drain_lock:
                    record = self._drain_tracking.get(stop_session_id)
                    if record:
                        record["finalizing"] = False
                logger.exception(
                    "Distributed session finalization failed for stop_session_id=%s",
                    stop_session_id,
                )
                return

            with self._drain_lock:
                record = self._drain_tracking.get(stop_session_id)
                if record:
                    record["finalizing"] = False
                    record["finalized"] = True

    def start(self):
        """Start the master node: ROUTER loop + mDNS advertisement."""
        # Start ROUTER loop
        self._running = True
        self._router_thread = threading.Thread(target=self._router_loop, daemon=True)
        self._router_thread.start()
        print(f"[MASTER] Internal ROUTER bound on port {self.router_port} for worker communication.")

        # Keep master's last_seen fresh for admin UI
        self._self_heartbeat_thread = threading.Thread(
            target=self._self_heartbeat_loop,
            daemon=True
        )
        self._self_heartbeat_thread.start()

        # Start mDNS in a separate daemon thread
        self._mdns_thread = threading.Thread(
                 target=self._advertise_master_threaded,
                daemon=True
        )

        self._mdns_thread.start()

    def _self_heartbeat_loop(self):
        """Update master's last_seen so it doesn't show offline."""
        tick = 0
        while self._running:
            try:
                self.registry.heartbeat(self.node_id)
                tick += 1
                if tick % 12 == 0:
                    logger.info(f"[MASTER] Self heartbeat at {time.time()}")
            except Exception as exc:
                logger.error(f"[MASTER] Self heartbeat failed: {exc}")
            time.sleep(30)


    def stop(self):
        """Stop the master node: ROUTER loop + mDNS cleanup."""
        self._running = False

        # Close ZeroMQ ROUTER
        self.router.close(0)
        self.ctx.term()

        # Stop mDNS
        if self.zeroconf and self.service_info:
            self.zeroconf.unregister_service(self.service_info)
            self.zeroconf.close()

        print("[MASTER] MasterNode stopped.")

    # ------------------ ROUTER loop for commands ------------------
    def _router_loop(self):
        """Main loop to handle messages from workers."""
        while self._running:
            try:
                #identity, empty, msg_bytes = self.router.recv_multipart()
                frames = self.router.recv_multipart()
                print("[MASTER] RAW FRAMES:", frames)

                identity = frames[0]
                msg_bytes = frames[-1]   # works for DEALER and REQ
                msg = json.loads(msg_bytes.decode())
                msg_type = msg.get("type")
                node_id = msg.get("node_id")

                # --- Worker Registration ---
                if msg_type == "REGISTER":
                    node_id = msg["node_id"]
                    worker_ip = msg.get("ip")
                    role = msg.get("role", "worker")
                    compute_port = msg.get("compute_port")
                    capabilities = msg.get("capabilities", {})
                    self.registry.register_node(
                        node_id=node_id,
                        ip=worker_ip,
                        role=role,
                        identity=identity,
                        compute_port=compute_port,
                        capabilities=capabilities
                    )
                    print(f"[MASTER] Registered node {node_id} (role={role}) with identity {identity.decode()}")
                    logger.info(f"[MASTER] Registered node {node_id} (role={role}) with identity {identity.decode()}")
                    if role == "ai":
                        self._broadcast_ai_registry(force=True)
                   
                    # Send USB path as part of the registration response
                    usb_path = str(self.usb_disk_manager.network_path) if self.usb_disk_manager.network_path else None
                    response = {
                        "type": "REGISTER_ACK",
                        "status": "OK",
                        "usb_path": usb_path
                    }
                    self.router.send_multipart([identity, b"", json.dumps(response).encode()])
                    continue
                if node_id:
                    self.registry.heartbeat(node_id)

                if msg_type == "HEARTBEAT":
                    if node_id and self.registry.get_nodes().get(node_id, {}).get("role") == "ai":
                        self._broadcast_ai_registry()
                    continue
                if msg_type == mt.EVT_STREAM_DRAINED:
                    self._record_drain_ack(node_id, msg.get("payload"))
                elif msg_type in {
                    mt.EVT_STREAM_READY_FOR_OFFICIAL,
                    mt.EVT_STREAM_OFFICIAL_STARTED,
                }:
                    self._record_official_start_event(node_id, msg_type, msg.get("payload"))
                # emit worker events after any internal tracking updates
                self.system_event_bus.emit(msg)

            except zmq.error.ZMQError:
                break
            except Exception as e:
                print(f"[MASTER] Error in router loop: {e}")

    def _broadcast_ai_registry(self, force: bool = False):
        """
        Broadcast AI node registry snapshot to all workers.
        """
        now = time.time()
        if not force and (now - self._last_ai_registry_broadcast) < 10:
            return
        ai_nodes = self.registry.get_ai_nodes()
        payload = {
            "nodes": {
                nid: {
                    "ip": data.get("ip"),
                    "compute_port": data.get("compute_port"),
                    "last_seen": data.get("last_seen"),
                    "capabilities": data.get("capabilities", []),
                }
                for nid, data in ai_nodes.items()
            }
        }
        msg = {"type": AI_NODE_REGISTRY, "payload": payload}
        self.send_command(msg)
        self._last_ai_registry_broadcast = now

    def _validate_subjects_per_node(self, subjects):
        """
        Validate that the total sensors for a list of subjects do not exceed the maximum allowed.
        Raises ValueError if the total exceeds MAX_TOTAL_SENSORS.
        """
        

        total_sensors = sum(
            sum(s.get("number_of", 1) for s in sub.get("sensors", []))
            for sub in subjects
        )

        if total_sensors > self.MAX_TOTAL_SENSORS_PER_NODE:
            subject_ids = [sub["subject_id"] for sub in subjects]
            
            raise ValueError(
                f"Node would be assigned {total_sensors} sensors across subjects {subject_ids}, "
                f"which exceeds the maximum allowed ({self.MAX_TOTAL_SENSORS_PER_NODE})."
            )


    def assign_subjects(self, subjects):
        """
        Assigns subjects to nodes.
        Returns subjects assigned to this master node.
        Pre-checks that each node’s total sensors do not exceed limits.
        """
        assigned_to_master = []
        nodes = self.registry.get_nodes()
        execution_nodes = [
            (nid, node)
            for nid, node in nodes.items()
            if node.get("role") in {"master", "worker"}
        ]
        if not execution_nodes:
            raise ValueError("No execution-capable nodes are registered")

        node_subjects_map = {nid: [] for nid, _ in execution_nodes}
        node_sensor_counts = {nid: 0 for nid, _ in execution_nodes}

        for sub in subjects:
            sensor_count = sum(s.get("number_of", 1) for s in sub.get("sensors", []))
            eligible_nodes = []
            for node_id, node in execution_nodes:
                if not self._node_supports_subject(node, sub):
                    continue
                if node_sensor_counts[node_id] + sensor_count > self.MAX_TOTAL_SENSORS_PER_NODE:
                    continue
                eligible_nodes.append((node_id, node))
            if not eligible_nodes:
                requirements = self._subject_requirements(sub)
                raise ValueError(
                    f"No execution-capable node can host subject '{sub.get('subject_id')}'. "
                    f"Required sensors={requirements['sensors']} algorithms={requirements['algorithms']}"
                )
            selected_node_id, _selected_node = min(
                eligible_nodes,
                key=lambda item: (node_sensor_counts[item[0]], item[0] != self.node_id, item[0]),
            )
            node_subjects_map[selected_node_id].append(sub)
            node_sensor_counts[selected_node_id] += sensor_count

        for node_id, node_subjects in node_subjects_map.items():
            if not node_subjects:
                continue
            self._validate_subjects_per_node(node_subjects)
            for sub in node_subjects:
                self.registry.assign_subject(sub["subject_id"], node_id)
                if node_id == self.node_id:
                    assigned_to_master.append(sub)

        return assigned_to_master

    def _subject_requirements(self, subject: dict) -> dict[str, list[str]]:
        sensors = []
        algorithms = []
        for sensor_conf in subject.get("sensors", []):
            sensor_name = str(sensor_conf.get("local_name") or "").strip()
            if sensor_name:
                sensors.append(sensor_name)
            compute_algorithm = sensor_conf.get("compute_algorithm") or {}
            algorithm_name = str(compute_algorithm.get("name") or "").strip()
            if algorithm_name:
                algorithms.append(algorithm_name)
        return {
            "sensors": sensors,
            "algorithms": algorithms,
        }

    def _node_supports_subject(self, node: dict, subject: dict) -> bool:
        capabilities = node.get("capabilities") or {}
        requirements = self._subject_requirements(subject)
        for sensor_name in requirements["sensors"]:
            if not supports_sensor(capabilities, sensor_name):
                return False
        for algorithm_name in requirements["algorithms"]:
            if not supports_algorithm(capabilities, algorithm_name):
                return False
        return True



    def dispatch_command(self, msg: dict, message_handler=None):
        """
        Route a command to the correct node(s).

        Args:
            msg: Command message dict with type and payload.
            message_handler: Optional MessageHandler for local execution.
        """
        print(f"[MASTER] dispatch_command called with msg: {msg}")
        payload = msg.get("payload", {})
        msg_type = msg.get("type")

        # Get list of worker IDs
        workers = [nid for nid, n in self.registry.get_nodes().items() if n["role"] == "worker"]
        #print("[MASTER] dispatch_command:", msg_type, payload)

        # --- Worker Wide Commands ---
        if msg_type == mt.CMD_UPDATE_FILE_PATH:
            # Apply the master's local path update before propagating the
            # worker-facing network path so distributed mode stays consistent.
            if message_handler:
                message_handler._handle_local(
                    mt.CMD_UPDATE_FILE_PATH,
                    {"file_path": payload.get("file_path")},
                )

            print(f"[MASTER] Updating file path to {payload['file_path']} on all workers")
            for worker_id in workers:
                self.send_command({
                    "type": mt.CMD_UPDATE_FILE_PATH,
                    "payload": {
                        "file_path": payload.get("network_path")
                    }
                }, target_node_id=worker_id)
            return  # skip all other dispatch logic

        # ------------------- Master-only commands -------------------
        # Always handled locally, never sent to workers
        master_only = {
            mt.CMD_IS_SERVER_READY,
            mt.CMD_SYSTEM_SETUP,
            mt.CMD_USB_MOUNT,
            mt.CMD_USB_SAFE_UNMOUNT,
            mt.CMD_GET_USB_STATUS,
            mt.CMD_ROBOT_MOTION,
            mt.CMD_ROBOT_STOP,
        }
        if msg_type in master_only:
            if message_handler:
                message_handler._handle_local(msg_type, payload)
            else:
                print(f"[MASTER] No message_handler to handle master-only command: {msg_type}")
            return  # skip all other dispatch logic

        # ------------------- Special case: INIT_SYSTEM -------------------
        if msg_type == mt.CMD_INIT_SYSTEM:
            subjects = payload.get("subjects", [])

            # Assign subjects to nodes
            try:
                self.assign_subjects(subjects)
            except ValueError as exc:
                if self.system_event_bus:
                    self.system_event_bus.emit({
                        "type": mt.EVT_ERROR,
                        "payload": str(exc),
                    })
                logger.error(str(exc))
                return

            # Forward each subject to its assigned node
            per_worker_payloads = {}  # worker_id -> list of subjects
            master_subjects = []

            for sub in subjects:
                assigned_node = self.registry.get_subjects().get(sub["subject_id"], {}).get("assigned_node", "master")
                if assigned_node == "master":
                    master_subjects.append(sub)
                else:
                    per_worker_payloads.setdefault(assigned_node, []).append(sub)

            # Handle master subjects locally
            if master_subjects and message_handler:
                master_payload = dict(payload)
                master_payload["subjects"] = master_subjects
                message_handler._handle_local(mt.CMD_INIT_SYSTEM, master_payload)

            # Send worker subjects
            for worker_id, subs in per_worker_payloads.items():
                worker_payload = dict(payload)
                worker_payload["subjects"] = subs
                self.send_command({
                    "type": mt.CMD_INIT_SYSTEM,
                    "payload": worker_payload
                }, target_node_id=worker_id)

            return  # skip general dispatch

        # ------------------- General dispatch -------------------
        subject_ids = payload.get("subject_ids")
        session_timestamp = None
        start_session_id = None
        if msg_type in (mt.CMD_START_STREAM_FOR_ALL, mt.CMD_START_STREAM_FOR_SUBJECTS):
            session_timestamp = payload.get("session_timestamp") or datetime.now().strftime("%Y%m%d_%H%M%S")
            start_session_id = payload.get("start_session_id") or uuid.uuid4().hex
        stop_session_id = None
        if msg_type in (mt.CMD_STOP_STREAM_FOR_ALL, mt.CMD_STOP_STREAM_FOR_SUBJECTS):
            stop_session_id = payload.get("stop_session_id") or uuid.uuid4().hex

        # Distributed commands bypass MessageHandler.handle_message() and are
        # routed directly from here. Prepare coordinator-owned storage once,
        # before sending physical-start commands to any participant. This is
        # required even when the master has no locally assigned subjects.
        if msg_type in (mt.CMD_START_STREAM_FOR_ALL, mt.CMD_START_STREAM_FOR_SUBJECTS):
            if message_handler:
                message_handler.prepare_stream_start()

        if msg_type == mt.CMD_START_OFFICIAL_STREAM:
            coordinator_received_monotonic_ns = time.monotonic_ns()
            coordinator_received_utc_ns = time.time_ns()
            requested_start_id = payload.get("start_session_id")
            with self._official_start_lock:
                record = self._official_start_tracking.get(requested_start_id)
                if not record:
                    self._emit_dispatch_error("Unknown or stale distributed start session", payload)
                    return
                if record["ready_nodes"] != record["expected_nodes"]:
                    missing = sorted(record["expected_nodes"] - record["ready_nodes"])
                    self._emit_dispatch_error(
                        f"Distributed stream is not ready for official acquisition; waiting for {missing}",
                        payload,
                    )
                    return
                record["commit_dispatched"] = True
                expected_nodes = sorted(record["expected_nodes"])
                commit_payload = dict(payload)
                commit_payload["session_timestamp"] = record["session_timestamp"]

            # Fan the commit out to remote participants before doing the
            # master's synchronous local persistence setup. ZeroMQ sends are
            # non-blocking at this level, so workers can begin activation while
            # the master activates locally instead of waiting behind it.
            dispatch_nodes = [
                node_id for node_id in expected_nodes if node_id != self.node_id
            ]
            if self.node_id in expected_nodes:
                dispatch_nodes.append(self.node_id)

            for dispatch_sequence, node_id in enumerate(dispatch_nodes, start=1):
                dispatch_monotonic_ns = time.monotonic_ns()
                dispatch_utc_ns = time.time_ns()
                node_payload = dict(commit_payload)
                node_payload["official_start_timing"] = {
                    "coordinator_node_id": self.node_id,
                    "coordinator_command_received_utc_ns": coordinator_received_utc_ns,
                    "coordinator_command_received_monotonic_ns": coordinator_received_monotonic_ns,
                    "coordinator_dispatch_utc_ns": dispatch_utc_ns,
                    "coordinator_dispatch_monotonic_ns": dispatch_monotonic_ns,
                    "coordinator_dispatch_offset_ms": round(
                        (dispatch_monotonic_ns - coordinator_received_monotonic_ns) / 1_000_000.0,
                        3,
                    ),
                    "dispatch_sequence": dispatch_sequence,
                    "target_node_id": node_id,
                }
                logger.info(
                    "Official-start dispatch start_session_id=%s target=%s sequence=%d offset_ms=%.3f",
                    requested_start_id,
                    node_id,
                    dispatch_sequence,
                    node_payload["official_start_timing"]["coordinator_dispatch_offset_ms"],
                )
                if node_id == self.node_id:
                    if message_handler:
                        message_handler._handle_local(msg_type, node_payload)
                else:
                    self.send_command(
                        {"type": msg_type, "payload": node_payload},
                        target_node_id=node_id,
                    )
            return

        if subject_ids:
            if msg_type in (mt.CMD_START_STREAM_FOR_ALL, mt.CMD_START_STREAM_FOR_SUBJECTS):
                with self._drain_lock:
                    self._active_stream_subjects.update(subject_ids)
            nodes_to_subjects = {}
            for subject_id in subject_ids:
                assigned_node = self.registry.get_subjects().get(subject_id, {}).get("assigned_node", "master")
                nodes_to_subjects.setdefault(assigned_node, []).append(subject_id)

            if start_session_id:
                self._create_official_start_record(
                    start_session_id,
                    session_timestamp=session_timestamp,
                    subject_ids=list(subject_ids),
                    expected_nodes=list(nodes_to_subjects.keys()),
                )

            if stop_session_id:
                self._create_drain_record(
                    stop_session_id,
                    scope="subjects",
                    subject_ids=subject_ids,
                    expected_nodes=list(nodes_to_subjects.keys()),
                )

            node_payloads = {}

            for node_id, ids in nodes_to_subjects.items():
                node_payload = dict(payload)
                node_payload["subject_ids"] = ids

                if session_timestamp:
                    node_payload["session_timestamp"] = session_timestamp

                if start_session_id:
                    node_payload["start_session_id"] = start_session_id
                    node_payload["official_start_mode"] = "coordinated"

                if stop_session_id:
                    node_payload["stop_session_id"] = stop_session_id

                node_payloads[node_id] = node_payload

            if stop_session_id:
                # Fan STOP out to remote participants before entering the
                # master's synchronous local stop path.
                for node_id, node_payload in node_payloads.items():
                    if node_id == self.node_id:
                        continue

                    self.send_command(
                        {
                            "type": msg_type,
                            "payload": node_payload,
                        },
                        target_node_id=node_id,
                    )

                master_payload = node_payloads.get(self.node_id)

                if master_payload and message_handler:
                    message_handler._handle_local(
                        msg_type,
                        master_payload,
                        stream_start_prepared=True,
                    )

                return

            for node_id, node_payload in node_payloads.items():
                if node_id == self.node_id:
                    if message_handler:
                        message_handler._handle_local(
                            msg_type,
                            node_payload,
                            stream_start_prepared=True,
                        )
                else:
                    self.send_command(
                        {
                            "type": msg_type,
                            "payload": node_payload,
                        },
                        target_node_id=node_id,
                    )

            return
        else:
            nodes_with_subjects = set()
            all_subject_ids = []
            for sub_data in self.registry.get_subjects().values():
                nodes_with_subjects.add(sub_data["assigned_node"])
            all_subject_ids = list(self.registry.get_subjects().keys())

            if msg_type in (mt.CMD_START_STREAM_FOR_ALL, mt.CMD_START_STREAM_FOR_SUBJECTS):
                with self._drain_lock:
                    self._active_stream_subjects.update(all_subject_ids)

            if start_session_id:
                self._create_official_start_record(
                    start_session_id,
                    session_timestamp=session_timestamp,
                    subject_ids=all_subject_ids,
                    expected_nodes=list(nodes_with_subjects),
                )

            if stop_session_id:
                self._create_drain_record(
                    stop_session_id,
                    scope="all",
                    subject_ids=all_subject_ids,
                    expected_nodes=list(nodes_with_subjects),
                )

            node_payloads = {}

            for node_id in nodes_with_subjects:
                node_payload = dict(payload)

                if session_timestamp:
                    node_payload["session_timestamp"] = session_timestamp

                if start_session_id:
                    node_payload["start_session_id"] = start_session_id
                    node_payload["official_start_mode"] = "coordinated"

                if stop_session_id:
                    node_payload["stop_session_id"] = stop_session_id

                node_payloads[node_id] = node_payload

            if stop_session_id:
                # Fan STOP out to remote participants before entering the
                # master's synchronous local stop path.
                for node_id, node_payload in node_payloads.items():
                    if node_id == self.node_id:
                        continue

                    self.send_command(
                        {
                            "type": msg_type,
                            "payload": node_payload,
                        },
                        target_node_id=node_id,
                    )

                master_payload = node_payloads.get(self.node_id)

                if master_payload and message_handler:
                    message_handler._handle_local(
                        msg_type,
                        master_payload,
                        stream_start_prepared=True,
                    )

                return

            for node_id, node_payload in node_payloads.items():
                if node_id == self.node_id:
                    if message_handler:
                        message_handler._handle_local(
                            msg_type,
                            node_payload,
                            stream_start_prepared=True,
                        )
                else:
                    self.send_command(
                        {
                            "type": msg_type,
                            "payload": node_payload,
                        },
                        target_node_id=node_id,
                    )


    def send_command(self, msg: dict, target_node_id: str = None):
        """
        Send a command to workers.

        Args:
            msg (dict): Command to send
            target_node_id (str, optional): Node ID to target; broadcast if None
        """
        msg_bytes = json.dumps(msg).encode()

        if target_node_id:
            identity = self.registry.get_identity(target_node_id)
            if identity:
                self.router.send_multipart([identity, b"", msg_bytes])
            else:
                print(f"[MASTER] Unknown worker: {target_node_id}")
        else:
            # Broadcast to all connected workers
            for worker_id in self.registry.get_worker_ids():
                identity = self.registry.get_identity(worker_id)
                if identity:
                    self.router.send_multipart([identity, b"", msg_bytes])

    # ------------------ mDNS advertisement ------------------
    def _advertise_master_threaded(self):
        """Advertise master node via mDNS in a separate thread to avoid blocking asyncio."""

        properties = {}
        if self.usb_disk_manager.network_path:
            properties["usb_path"] = str(self.usb_disk_manager.network_path)

        local_ip = socket.inet_aton(get_local_ip())
        hostname = self.mdns_hostname or socket.gethostname()
        if not hostname.endswith(".local."):
            hostname = f"{hostname}.local."

        service_name = f"{hostname.replace('.local.', '')}._nexusn3._tcp.local."
        self.service_info = ServiceInfo(
            "_nexusn3._tcp.local.",
            service_name,
            addresses=[local_ip],
            port=self.router_port,
            properties=properties,
            server=hostname,
        )

        self.zeroconf = Zeroconf()
        self.zeroconf.register_service(self.service_info)
        print(f"[MASTER] mDNS advertised on {get_local_ip()}:{self.router_port} as {hostname}")

    
    
