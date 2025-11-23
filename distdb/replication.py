"""Simplified Raft-based replication for consistency."""

import time
import random
import threading
from enum import Enum
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass


class NodeState(Enum):
    """Raft node states."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """Raft log entry."""
    term: int
    index: int
    command: Dict[str, Any]
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


class RaftNode:
    """Simplified Raft consensus implementation."""
    
    def __init__(self, node_id: str, all_nodes: List[str], 
                 election_timeout_min: float = 1.5,
                 election_timeout_max: float = 3.0,
                 heartbeat_interval: float = 0.5):
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        
        # Leader state (volatile)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Callbacks
        self.apply_callback: Optional[Callable] = None
        
        # Threading
        self.lock = threading.RLock()
        self.election_timer: Optional[threading.Timer] = None
        self.heartbeat_timer: Optional[threading.Timer] = None
        self.last_heartbeat = time.time()
        
        self._reset_election_timer()
    
    def set_apply_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Set callback for applying committed entries."""
        self.apply_callback = callback
    
    def append_entry(self, command: Dict[str, Any]) -> bool:
        """Append an entry to the log (leader only)."""
        with self.lock:
            if self.state != NodeState.LEADER:
                return False
            
            entry = LogEntry(
                term=self.current_term,
                index=len(self.log),
                command=command
            )
            self.log.append(entry)
            
            # In a real implementation, would replicate to followers
            # For simplicity, we'll auto-commit after majority  
            self._try_commit()
            
            return True
    
    def _try_commit(self):
        """Try to commit entries."""
        with self.lock:
            if self.state == NodeState.LEADER:
                # Simplified: immediately commit as we're the only node
                # In real Raft, would wait for majority replication
                if len(self.log) > self.commit_index:
                    self.commit_index = len(self.log)
                    self._apply_committed_entries()
    
    def _apply_committed_entries(self):
        """Apply committed but not yet applied entries."""
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                if self.last_applied <= len(self.log):
                    entry = self.log[self.last_applied - 1]
                    if self.apply_callback:
                        self.apply_callback(entry.command)
    
    def request_vote(self, candidate_term: int, candidate_id: str, 
                    last_log_index: int, last_log_term: int) -> bool:
        """Handle RequestVote RPC."""
        with self.lock:
            # Update term if necessary
            if candidate_term > self.current_term:
                self.current_term = candidate_term
                self.voted_for = None
                self.state = NodeState.FOLLOWER
            
            # Vote if we haven't voted or voted for this candidate
            if candidate_term == self.current_term:
                if self.voted_for is None or self.voted_for == candidate_id:
                    # Check if candidate's log is at least as up-to-date
                    our_last_index = len(self.log) - 1
                    our_last_term = self.log[-1].term if self.log else 0
                    
                    if (last_log_term > our_last_term or 
                        (last_log_term == our_last_term and last_log_index >= our_last_index)):
                        self.voted_for = candidate_id
                        self._reset_election_timer()
                        return True
            
            return False
    
    def append_entries(self, leader_term: int, leader_id: str, 
                      prev_log_index: int, prev_log_term: int,
                      entries: List[LogEntry], leader_commit: int) -> bool:
        """Handle AppendEntries RPC (heartbeat)."""
        with self.lock:
            # Update term if necessary
            if leader_term > self.current_term:
                self.current_term = leader_term
                self.voted_for = None
                self.state = NodeState.FOLLOWER
            
            # Reject if term is old
            if leader_term < self.current_term:
                return False
            
            # Reset election timer (we got a heartbeat from leader)
            self.last_heartbeat = time.time()
            self._reset_election_timer()
            
            # In real implementation would append entries
            # For simplicity, just update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self._apply_committed_entries()
            
            return True
    
    def _start_election(self):
        """Start a new election."""
        with self.lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            
            # In real implementation, would send RequestVote to all nodes
            # For simplicity, just become leader if alone
            if len(self.all_nodes) == 1:
                self._become_leader()
            else:
                # Reset timer and try again
                self._reset_election_timer()
    
    def _become_leader(self):
        """Become the leader."""
        with self.lock:
            self.state = NodeState.LEADER
            
            # Initialize leader state
            for node in self.all_nodes:
                if node != self.node_id:
                    self.next_index[node] = len(self.log)
                    self.match_index[node] = 0
            
            # Start sending heartbeats
            self._send_heartbeat()
    
    def _send_heartbeat(self):
        """Send heartbeat to followers."""
        with self.lock:
            if self.state == NodeState.LEADER:
                # In real implementation, would send AppendEntries to all followers
                # Schedule next heartbeat
                self.heartbeat_timer = threading.Timer(
                    self.heartbeat_interval, 
                    self._send_heartbeat
                )
                self.heartbeat_timer.daemon = True
                self.heartbeat_timer.start()
    
    def _reset_election_timer(self):
        """Reset the election timeout timer."""
        if self.election_timer:
            self.election_timer.cancel()
        
        timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
        self.election_timer = threading.Timer(timeout, self._on_election_timeout)
        self.election_timer.daemon = True
        self.election_timer.start()
    
    def _on_election_timeout(self):
        """Called when election timeout expires."""
        with self.lock:
            if self.state != NodeState.LEADER:
                self._start_election()
    
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        with self.lock:
            return self.state == NodeState.LEADER
    
    def get_leader_id(self) -> Optional[str]:
        """Get the current leader ID."""
        with self.lock:
            if self.state == NodeState.LEADER:
                return self.node_id
            # In real implementation, would track leader
            return None
    
    def shutdown(self):
        """Shutdown the Raft node."""
        with self.lock:
            if self.election_timer:
                self.election_timer.cancel()
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()


class ReplicationManager:
    """Manage replication across nodes."""
    
    def __init__(self, node_id: str, cluster_nodes: List[str], config):
        self.node_id = node_id
        self.cluster_nodes = cluster_nodes
        self.config = config
        
        # Create Raft node
        self.raft = RaftNode(
            node_id,
            cluster_nodes,
            config.election_timeout_min,
            config.election_timeout_max,
            config.heartbeat_interval
        )
        
        self.lock = threading.RLock()
    
    def replicate_write(self, operation: Dict[str, Any]) -> bool:
        """Replicate a write operation."""
        with self.lock:
            if not self.raft.is_leader():
                return False
            
            return self.raft.append_entry(operation)
    
    def set_apply_callback(self, callback: Callable):
        """Set callback for when entries are committed."""
        self.raft.set_apply_callback(callback)
    
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self.raft.is_leader()
    
    def shutdown(self):
        """Shutdown replication."""
        self.raft.shutdown()
