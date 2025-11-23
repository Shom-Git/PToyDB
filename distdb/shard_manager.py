"""Shard manager for data distribution using consistent hashing."""

import bisect
import threading
from typing import List, Dict, Set, Optional
from .utils import VirtualNode, hash_key


class ConsistentHashRing:
    """Consistent hash ring for distributing data across nodes."""
    
    def __init__(self, num_virtual_nodes: int = 150):
        self.num_virtual_nodes = num_virtual_nodes
        self.ring: List[VirtualNode] = []
        self.nodes: Set[str] = set()
        self.lock = threading.RLock()
    
    def add_node(self, node_id: str):
        """Add a node to the ring."""
        with self.lock:
            if node_id in self.nodes:
                return
            
            self.nodes.add(node_id)
            
            # Add virtual nodes
            for i in range(self.num_virtual_nodes):
                vnode = VirtualNode(node_id, i)
                bisect.insort(self.ring, vnode)
    
    def remove_node(self, node_id: str):
        """Remove a node from the ring."""
        with self.lock:
            if node_id not in self.nodes:
                return
            
            self.nodes.discard(node_id)
            
            # Remove virtual nodes
            self.ring = [vn for vn in self.ring if vn.node_id != node_id]
    
    def get_node(self, key: str) -> Optional[str]:
        """Get the node responsible for a key."""
        with self.lock:
            if not self.ring:
                return None
            
            key_hash = hash_key(key)
            
            # Binary search for the first virtual node >= key_hash
            idx = bisect.bisect_right(self.ring, type('obj', (object,), {'hash_value': key_hash})())
            
            if idx == len(self.ring):
                idx = 0
            
            return self.ring[idx].node_id
    
    def get_nodes_for_replication(self, key: str, replication_factor: int) -> List[str]:
        """Get multiple nodes for replication."""
        with self.lock:
            if not self.ring:
                return []
            
            key_hash = hash_key(key)
            idx = bisect.bisect_right(self.ring, type('obj', (object,), {'hash_value': key_hash})())
            
            if idx == len(self.ring):
                idx = 0
            
            nodes = []
            seen = set()
            
            # Walk the ring to find unique nodes
            for i in range(len(self.ring)):
                vnode = self.ring[(idx + i) % len(self.ring)]
                if vnode.node_id not in seen:
                    nodes.append(vnode.node_id)
                    seen.add(vnode.node_id)
                    if len(nodes) >= replication_factor:
                        break
            
            return nodes
    
    def get_all_nodes(self) -> List[str]:
        """Get all nodes in the cluster."""
        with self.lock:
            return list(self.nodes)


class ShardManager:
    """Manage data sharding across cluster nodes."""
    
    def __init__(self, node_id: str, replication_factor: int = 3):
        self.node_id = node_id
        self.replication_factor = replication_factor
        self.hash_ring = ConsistentHashRing()
        self.lock = threading.RLock()
        
        # Add self to ring
        self.hash_ring.add_node(node_id)
    
    def add_node(self, node_id: str):
        """Add a node to the cluster."""
        with self.lock:
            self.hash_ring.add_node(node_id)
    
    def remove_node(self, node_id: str):
        """Remove a node from the cluster."""
        with self.lock:
            self.hash_ring.remove_node(node_id)
    
    def get_primary_node(self, key: str) -> str:
        """Get the primary node for a key."""
        with self.lock:
            node = self.hash_ring.get_node(key)
            return node if node else self.node_id
    
    def get_replica_nodes(self, key: str) -> List[str]:
        """Get all replica nodes for a key (including primary)."""
        with self.lock:
            return self.hash_ring.get_nodes_for_replication(key, self.replication_factor)
    
    def is_responsible_for(self, key: str) -> bool:
        """Check if this node is responsible for a key."""
        with self.lock:
            replicas = self.get_replica_nodes(key)
            return self.node_id in replicas
    
    def is_primary_for(self, key: str) -> bool:
        """Check if this node is the primary for a key."""
        with self.lock:
            return self.get_primary_node(key) == self.node_id
    
    def get_all_nodes(self) -> List[str]:
        """Get all nodes in the cluster."""
        with self.lock:
            return self.hash_ring.get_all_nodes()
