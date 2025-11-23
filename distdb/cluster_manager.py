"""Cluster manager for node discovery and health monitoring."""

import threading
import time
from typing import List, Dict, Set, Optional, Callable
from dataclasses import dataclass


@dataclass
class NodeInfo:
    """Information about a cluster node."""
    node_id: str
    host: str
    port: int
    last_seen: float
    is_alive: bool = True


class ClusterManager:
    """Manage cluster membership and health."""
    
    def __init__(self, node_id: str, host: str, port: int, heartbeat_interval: float = 1.0):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval
        
        # Cluster state
        self.nodes: Dict[str, NodeInfo] = {}
        self.nodes[node_id] = NodeInfo(node_id, host, port, time.time())
        
        # Callbacks
        self.on_node_added: Optional[Callable[[str], None]] = None
        self.on_node_removed: Optional[Callable[[str], None]] = None
        
        # Threading
        self.lock = threading.RLock()
        self.monitor_thread: Optional[threading.Thread] = None
        self.running = False
    
    def start(self):
        """Start cluster monitoring."""
        with self.lock:
            if self.running:
                return
            
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
    
    def stop(self):
        """Stop cluster monitoring."""
        with self.lock:
            self.running = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=2.0)
    
    def add_node(self, node_id: str, host: str, port: int):
        """Add a node to the cluster."""
        with self.lock:
            if node_id not in self.nodes:
                self.nodes[node_id] = NodeInfo(node_id, host, port, time.time())
                if self.on_node_added:
                    self.on_node_added(node_id)
            else:
                # Update existing node
                self.nodes[node_id].host = host
                self.nodes[node_id].port = port
                self.nodes[node_id].last_seen = time.time()
                self.nodes[node_id].is_alive = True
    
    def remove_node(self, node_id: str):
        """Remove a node from the cluster."""
        with self.lock:
            if node_id in self.nodes and node_id != self.node_id:
                del self.nodes[node_id]
                if self.on_node_removed:
                    self.on_node_removed(node_id)
    
    def update_heartbeat(self, node_id: str):
        """Update heartbeat timestamp for a node."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].last_seen = time.time()
                if not self.nodes[node_id].is_alive:
                    self.nodes[node_id].is_alive = True
                    if self.on_node_added:
                        self.on_node_added(node_id)
    
    def get_alive_nodes(self) -> List[str]:
        """Get list of alive nodes."""
        with self.lock:
            return [nid for nid, info in self.nodes.items() if info.is_alive]
    
    def get_node_info(self, node_id: str) -> Optional[NodeInfo]:
        """Get information about a node."""
        with self.lock:
            return self.nodes.get(node_id)
    
    def get_all_nodes(self) -> List[str]:
        """Get all known nodes (alive or not)."""
        with self.lock:
            return list(self.nodes.keys())
    
    def _monitor_loop(self):
        """Monitor node health."""
        while self.running:
            time.sleep(self.heartbeat_interval)
            
            with self.lock:
                now = time.time()
                timeout = self.heartbeat_interval * 3  # 3x heartbeat interval
                
                for node_id, info in list(self.nodes.items()):
                    if node_id == self.node_id:
                        continue
                    
                    # Check if node has timed out
                    if info.is_alive and (now - info.last_seen) > timeout:
                        info.is_alive = False
                        if self.on_node_removed:
                            self.on_node_removed(node_id)
