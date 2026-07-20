

# --- Node Registry --- ## 
import time

class NodeRegistry:
    def __init__(self):
        self.nodes = {}  #the actual registry

    def register(self, node_id, ip):
        self.nodes[node_id] = {
            "ip": ip,
            "last_seen": time.time()
        }

    def get_nodes(self):
        return self.nodes

    def heartbeat(self, node_id):
        if node_id in self.nodes:
            self.nodes[node_id]["last_seen"] = time.time()


