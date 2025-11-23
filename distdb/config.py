"""Configuration management for DistDB."""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    """Database configuration."""
    
    # Node settings
    node_id: str = "node1"
    host: str = "localhost"
    port: int = 5000
    
    # Cluster settings
    cluster_nodes: List[str] = field(default_factory=list)
    replication_factor: int = 3
    
    # Storage settings
    data_dir: str = "./data"
    wal_dir: str = "./wal"
    snapshot_interval: int = 1000  # Operations between snapshots
    
    # Performance settings
    max_batch_size: int = 100
    heartbeat_interval: float = 0.5  # seconds
    election_timeout_min: float = 1.5  # seconds
    election_timeout_max: float = 3.0  # seconds
    
    # Indexing settings
    enable_auto_index: bool = True
    max_index_memory: int = 100 * 1024 * 1024  # 100MB
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        return cls(
            node_id=os.getenv('DISTDB_NODE_ID', 'node1'),
            host=os.getenv('DISTDB_HOST', 'localhost'),
            port=int(os.getenv('DISTDB_PORT', '5000')),
            cluster_nodes=os.getenv('DISTDB_CLUSTER_NODES', '').split(',') if os.getenv('DISTDB_CLUSTER_NODES') else [],
            replication_factor=int(os.getenv('DISTDB_REPLICATION_FACTOR', '3')),
            data_dir=os.getenv('DISTDB_DATA_DIR', './data'),
            wal_dir=os.getenv('DISTDB_WAL_DIR', './wal'),
        )
