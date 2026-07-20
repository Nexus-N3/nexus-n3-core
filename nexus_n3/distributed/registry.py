"""Node and subject registry for distributed coordination."""

import threading
import time

class NodeRegistry:
    """Track nodes and subject assignments in the distributed system."""
    def __init__(self):
        """Initialize the registry with empty node and subject maps."""
        # Nodes tracked separately from subjects
        self._lock = threading.RLock()
        self.nodes = {}       # node_id -> {"ip": ..., "role": ..., "identity": ..., "last_seen": ...}
        self.subjects = {}    # subject_id -> {"assigned_node": node_id, "meta": {...}}

    # ---------------- Nodes ----------------
    def register_node(self, node_id, ip=None, role="worker", identity=None, compute_port=None, capabilities=None):
        """
        Register a node (master or worker).
        """
        with self._lock:
            self.nodes[node_id] = {
                "ip": ip,
                "role": role,
                "identity": identity,
                "last_seen": time.time(),
                "compute_port": compute_port,
                "capabilities": capabilities or {}
            }

    def get_nodes(self):
        """Return a snapshot of the node registry."""
        with self._lock:
            return {
                node_id: dict(data)
                for node_id, data in self.nodes.items()
            }

    def heartbeat(self, node_id):
        """Update the last_seen timestamp for a node."""
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id]["last_seen"] = time.time()

    # Convenience methods for MasterNode
    def get_worker_ids(self):
        """Return a list of registered worker node IDs."""
        with self._lock:
            return [nid for nid, data in self.nodes.items() if data["role"] == "worker"]

    def get_ai_nodes(self):
        """Return a dict of registered AI nodes."""
        with self._lock:
            return {
                nid: dict(data)
                for nid, data in self.nodes.items()
                if data["role"] == "ai"
            }

    def set_ai_nodes(self, ai_nodes: dict):
        """
        Replace AI nodes in the registry with the provided snapshot.
        """
        with self._lock:
            # Remove existing AI nodes
            self.nodes = {nid: data for nid, data in self.nodes.items() if data["role"] != "ai"}
            # Add snapshot
            for node_id, data in ai_nodes.items():
                self.nodes[node_id] = {
                    "ip": data.get("ip"),
                    "role": "ai",
                    "identity": None,
                    "last_seen": data.get("last_seen", time.time()),
                    "compute_port": data.get("compute_port"),
                    "capabilities": data.get("capabilities", {}),
                }

    def get_identity(self, node_id):
        """Return the ZMQ identity for a worker node."""
        with self._lock:
            node = self.nodes.get(node_id)
            return node.get("identity") if node else None

    # ---------------- Subjects ----------------
    def assign_subject(self, subject_id, node_id, meta=None):
        """Assign a subject to a node."""
        with self._lock:
            self.subjects[subject_id] = {
                "assigned_node": node_id,
                "meta": meta or {}
            }

    def get_subjects(self):
        """Return a snapshot of the subject assignment map."""
        with self._lock:
            return {
                subject_id: {
                    "assigned_node": data.get("assigned_node"),
                    "meta": dict(data.get("meta", {})),
                }
                for subject_id, data in self.subjects.items()
            }
