"""Tests for replication."""

import pytest
import time
from distdb.replication import RaftNode, ReplicationManager
from distdb.config import Config


def test_raft_node_initialization():
    """Test Raft node initialization."""
    node = RaftNode('node1', ['node1', 'node2', 'node3'])
    
    assert node.node_id == 'node1'
    assert node.current_term == 0
    assert node.voted_for is None
    assert len(node.log) == 0
    
    node.shutdown()


def test_raft_single_node_becomes_leader():
    """Test single node becomes leader."""
    node = RaftNode('node1', ['node1'])
    
    # Wait a bit for election
    time.sleep(2)
    
    # Single node should become leader
    assert node.is_leader()
    
    node.shutdown()


def test_raft_append_entry():
    """Test appending entries as leader."""
    node = RaftNode('node1', ['node1'])
    time.sleep(2)  # Become leader
    
    # Append entries
    command = {'type': 'put', 'key': 'key1', 'value': 'value1'}
    success = node.append_entry(command)
    
    assert success
    assert len(node.log) > 0
    
    node.shutdown()


def test_raft_apply_callback():
    """Test apply callback is called."""
    applied_commands = []
    
    def callback(command):
        applied_commands.append(command)
    
    node = RaftNode('node1', ['node1'])
    node.set_apply_callback(callback)
    
    time.sleep(2)  # Become leader
    
    # Append entry
    command = {'type': 'put', 'key': 'key1'}
    node.append_entry(command)
    
    # Wait for apply
    time.sleep(0.5)
    
    assert len(applied_commands) > 0
    assert applied_commands[0]['type'] == 'put'
    
    node.shutdown()


def test_replication_manager():
    """Test replication manager."""
    config = Config()
    manager = ReplicationManager('node1', ['node1'], config)
    
    time.sleep(2)  # Become leader
    
    assert manager.is_leader()
    
    # Replicate write
    operation = {'type': 'insert', 'table': 'users'}
    success = manager.replicate_write(operation)
    assert success
    
    manager.shutdown()
