"""Core storage engine with WAL and recovery."""

import os
import json
import threading
import time
from typing import Any, Dict, Optional, List, Tuple
from collections import defaultdict
from pathlib import Path

from .utils import serialize, deserialize


class WriteAheadLog:
    """Write-Ahead Log for durability."""
    
    def __init__(self, wal_dir: str):
        self.wal_dir = Path(wal_dir)
        self.wal_dir.mkdir(parents=True, exist_ok=True)
        self.current_log_file = None
        self.log_index = 0
        self.lock = threading.Lock()
        self._open_new_log()
    
    def _open_new_log(self):
        """Open a new log file."""
        self.log_index += 1
        log_path = self.wal_dir / f"wal_{self.log_index:010d}.log"
        self.current_log_file = open(log_path, 'ab')
    
    def append(self, operation: str, table: str, key: str, value: Any) -> int:
        """Append an operation to the WAL."""
        with self.lock:
            entry = {
                'timestamp': time.time(),
                'operation': operation,
                'table': table,
                'key': key,
                'value': value
            }
            serialized = serialize(entry)
            # Write length prefix followed by data
            length = len(serialized)
            self.current_log_file.write(length.to_bytes(4, 'big'))
            self.current_log_file.write(serialized)
            self.current_log_file.flush()
            os.fsync(self.current_log_file.fileno())
            return self.log_index
    
    def read_all_entries(self) -> List[Dict]:
        """Read all entries from all log files."""
        entries = []
        for log_file in sorted(self.wal_dir.glob("wal_*.log")):
            with open(log_file, 'rb') as f:
                while True:
                    length_bytes = f.read(4)
                    if not length_bytes:
                        break
                    length = int.from_bytes(length_bytes, 'big')
                    data = f.read(length)
                    if len(data) != length:
                        break
                    entries.append(deserialize(data))
        return entries
    
    def truncate(self):
        """Remove old log files."""
        with self.lock:
            if self.current_log_file:
                self.current_log_file.close()
            for log_file in self.wal_dir.glob("wal_*.log"):
                log_file.unlink()
            self.log_index = 0
            self._open_new_log()
    
    def close(self):
        """Close the WAL."""
        with self.lock:
            if self.current_log_file:
                self.current_log_file.close()


class StorageEngine:
    """In-memory key-value storage engine with persistence."""
    
    def __init__(self, data_dir: str, wal_dir: str, snapshot_interval: int = 1000):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Multi-table storage: table_name -> key -> value
        self.tables: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Schema storage: table_name -> {column_name: type}
        self.schemas: Dict[str, Dict[str, str]] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # WAL
        self.wal = WriteAheadLog(wal_dir)
        self.snapshot_interval = snapshot_interval
        self.operations_since_snapshot = 0
        
        # Recovery
        self._recover()
    
    def _recover(self):
        """Recover from snapshot and WAL."""
        # Load snapshot if exists
        snapshot_path = self.data_dir / "snapshot.json"
        if snapshot_path.exists():
            with open(snapshot_path, 'r') as f:
                snapshot = json.load(f)
                self.tables = defaultdict(dict, {
                    table: dict(data) for table, data in snapshot.get('tables', {}).items()
                })
                self.schemas = snapshot.get('schemas', {})
        
        # Replay WAL
        entries = self.wal.read_all_entries()
        for entry in entries:
            operation = entry['operation']
            table = entry['table']
            key = entry['key']
            value = entry['value']
            
            if operation == 'PUT':
                self.tables[table][key] = value
            elif operation == 'DELETE':
                self.tables[table].pop(key, None)
            elif operation == 'CREATE_TABLE':
                self.schemas[table] = value
            elif operation == 'DROP_TABLE':
                self.tables.pop(table, None)
                self.schemas.pop(table, None)
    
    def create_table(self, table_name: str, schema: Dict[str, str]):
        """Create a new table with schema."""
        with self.lock:
            if table_name in self.schemas:
                raise ValueError(f"Table {table_name} already exists")
            
            self.schemas[table_name] = schema
            self.tables[table_name] = {}
            self.wal.append('CREATE_TABLE', table_name, '', schema)
            self._check_snapshot()
    
    def drop_table(self, table_name: str):
        """Drop a table."""
        with self.lock:
            if table_name not in self.schemas:
                raise ValueError(f"Table {table_name} does not exist")
            
            del self.schemas[table_name]
            del self.tables[table_name]
            self.wal.append('DROP_TABLE', table_name, '', None)
            self._check_snapshot()
    
    def get_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """Get table schema."""
        with self.lock:
            return self.schemas.get(table_name)
    
    def list_tables(self) -> List[str]:
        """List all tables."""
        with self.lock:
            return list(self.schemas.keys())
    
    def put(self, table: str, key: str, value: Dict[str, Any]):
        """Store a key-value pair in a table."""
        with self.lock:
            if table not in self.schemas:
                raise ValueError(f"Table {table} does not exist")
            
            # Validate against schema
            schema = self.schemas[table]
            for col_name in value.keys():
                if col_name not in schema:
                    raise ValueError(f"Column {col_name} not in schema for table {table}")
            
            self.tables[table][key] = value
            self.wal.append('PUT', table, key, value)
            self.operations_since_snapshot += 1
            self._check_snapshot()
    
    def get(self, table: str, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve a value by key from a table."""
        with self.lock:
            return self.tables.get(table, {}).get(key)
    
    def delete(self, table: str, key: str) -> bool:
        """Delete a key-value pair from a table."""
        with self.lock:
            if table in self.tables and key in self.tables[table]:
                del self.tables[table][key]
                self.wal.append('DELETE', table, key, None)
                self.operations_since_snapshot += 1
                self._check_snapshot()
                return True
            return False
    
    def scan(self, table: str) -> List[Tuple[str, Dict[str, Any]]]:
        """Scan all key-value pairs in a table."""
        with self.lock:
            if table not in self.tables:
                return []
            return list(self.tables[table].items())
    
    def _check_snapshot(self):
        """Create snapshot if needed."""
        if self.operations_since_snapshot >= self.snapshot_interval:
            self.snapshot()
    
    def snapshot(self):
        """Create a snapshot of current state."""
        with self.lock:
            snapshot_path = self.data_dir / "snapshot.json"
            temp_path = self.data_dir / "snapshot.json.tmp"
            
            snapshot = {
                'tables': {table: dict(data) for table, data in self.tables.items()},
                'schemas': self.schemas,
                'timestamp': time.time()
            }
            
            with open(temp_path, 'w') as f:
                json.dump(snapshot, f)
            
            # Atomic rename
            temp_path.rename(snapshot_path)
            
            # Clear WAL after successful snapshot
            self.wal.truncate()
            self.operations_since_snapshot = 0
    
    def close(self):
        """Close the storage engine."""
        with self.lock:
            self.snapshot()
            self.wal.close()
