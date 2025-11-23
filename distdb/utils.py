"""Utility functions for DistDB."""

import logging
import hashlib
import msgpack
from typing import Any, Dict


def setup_logging(level: int = logging.INFO) -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def serialize(obj: Any) -> bytes:
    """Serialize an object to bytes using msgpack."""
    return msgpack.packb(obj, use_bin_type=True)


def deserialize(data: bytes) -> Any:
    """Deserialize bytes to an object using msgpack."""
    return msgpack.unpackb(data, raw=False)


def hash_key(key: str) -> int:
    """Compute hash of a key for consistent hashing."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


def get_shard(key: str, num_shards: int) -> int:
    """Determine which shard a key belongs to."""
    return hash_key(key) % num_shards


class VirtualNode:
    """Virtual node for consistent hashing."""
    
    def __init__(self, node_id: str, virtual_id: int):
        self.node_id = node_id
        self.virtual_id = virtual_id
        self.hash_value = hash_key(f"{node_id}:{virtual_id}")
    
    def __lt__(self, other):
        return self.hash_value < other.hash_value
    
    def __repr__(self):
        return f"VirtualNode({self.node_id}, {self.virtual_id})"
