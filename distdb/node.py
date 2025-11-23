"""Main node class coordinating all components."""

import threading
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from .config import Config
from .storage_engine import StorageEngine
from .index_manager import IndexManager
from .sql_parser import SQLParser
from .query_executor import QueryExecutor
from .shard_manager import ShardManager
from .replication import ReplicationManager
from .cluster_manager import ClusterManager
from .utils import setup_logging


logger = logging.getLogger(__name__)


class Node:
    """Database node coordinating all components."""
    
    def __init__(self, config: Config):
        self.config = config
        self.node_id = config.node_id
        
        # Setup logging
        setup_logging()
        logger.info(f"Initializing node {self.node_id}")
        
        # Create directories
        Path(config.data_dir).mkdir(parents=True, exist_ok=True)
        Path(config.wal_dir).mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.storage = StorageEngine(
            config.data_dir,
            config.wal_dir,
            config.snapshot_interval
        )
        
        self.index_manager = IndexManager()
        self.sql_parser = SQLParser()
        self.query_executor = QueryExecutor(self.storage, self.index_manager)
        
        # Distribution components
        self.shard_manager = ShardManager(
            config.node_id,
            config.replication_factor
        )
        
        # Cluster management
        self.cluster_manager = ClusterManager(
            config.node_id,
            config.host,
            config.port,
            config.heartbeat_interval
        )
        
        # Setup cluster callbacks
        self.cluster_manager.on_node_added = self._on_node_added
        self.cluster_manager.on_node_removed = self._on_node_removed
        
        # Add initial cluster nodes
        for node_spec in config.cluster_nodes:
            if node_spec:
                try:
                    # Expected format: node_id@host:port
                    parts = node_spec.split('@')
                    if len(parts) == 2:
                        node_id = parts[0]
                        host_port = parts[1].split(':')
                        if len(host_port) == 2:
                            host = host_port[0]
                            port = int(host_port[1])
                            self.cluster_manager.add_node(node_id, host, port)
                            self.shard_manager.add_node(node_id)
                except Exception as e:
                    logger.error(f"Error adding node {node_spec}: {e}")
        
        # Replication (simplified - single node becomes leader)
        cluster_nodes = self.cluster_manager.get_all_nodes()
        self.replication_manager = ReplicationManager(
            config.node_id,
            cluster_nodes,
            config
        )
        
        # Set replication callback
        self.replication_manager.set_apply_callback(self._apply_replicated_command)
        
        self.running = False
        self.lock = threading.RLock()
        
        logger.info(f"Node {self.node_id} initialized")
    
    def start(self):
        """Start the node."""
        with self.lock:
            if self.running:
                return
            
            logger.info(f"Starting node {self.node_id}")
            self.running = True
            self.cluster_manager.start()
            logger.info(f"Node {self.node_id} started")
    
    def stop(self):
        """Stop the node."""
        with self.lock:
            if not self.running:
                return
            
            logger.info(f"Stopping node {self.node_id}")
            self.running = False
            self.cluster_manager.stop()
            self.replication_manager.shutdown()
            self.storage.close()
            logger.info(f"Node {self.node_id} stopped")
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query."""
        try:
            # Parse the query
            parsed_query = self.sql_parser.parse(sql)
            
            # For write operations, replicate via Raft
            if parsed_query.query_type in ('INSERT', 'UPDATE', 'DELETE', 'CREATE_TABLE', 'DROP_TABLE'):
                if not self.replication_manager.is_leader():
                    return {
                        'status': 'error',
                        'message': 'Not the leader, cannot write',
                        'is_leader': False
                    }
                
                # Replicate the write
                command = {
                    'type': 'sql',
                    'sql': sql
                }
                success = self.replication_manager.replicate_write(command)
                
                if not success:
                    return {
                        'status': 'error',
                        'message': 'Failed to replicate write'
                    }
            
            # Execute the query
            result = self.query_executor.execute(parsed_query)
            result['node_id'] = self.node_id
            result['is_leader'] = self.replication_manager.is_leader()
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing query: {e}", exc_info=True)
            return {
                'status': 'error',
                'message': str(e),
                'node_id': self.node_id
            }
    
    def _apply_replicated_command(self, command: Dict[str, Any]):
        """Called when a replicated command is committed."""
        try:
            if command.get('type') == 'sql':
                sql = command.get('sql')
                if sql:
                    parsed_query = self.sql_parser.parse(sql)
                    self.query_executor.execute(parsed_query)
        except Exception as e:
            logger.error(f"Error applying replicated command: {e}", exc_info=True)
    
    def _on_node_added(self, node_id: str):
        """Called when a node is added to the cluster."""
        logger.info(f"Node added: {node_id}")
        self.shard_manager.add_node(node_id)
    
    def _on_node_removed(self, node_id: str):
        """Called when a node is removed from the cluster."""
        logger.info(f"Node removed: {node_id}")
        self.shard_manager.remove_node(node_id)
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status."""
        with self.lock:
            return {
                'node_id': self.node_id,
                'running': self.running,
                'is_leader': self.replication_manager.is_leader(),
                'cluster_nodes': self.cluster_manager.get_alive_nodes(),
                'tables': self.storage.list_tables()
            }
