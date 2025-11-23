"""Index manager for fast lookups."""

import threading
from typing import Any, Dict, List, Optional, Set, Tuple
from sortedcontainers import SortedDict
from collections import defaultdict


class Index:
    """Base class for indexes."""
    
    def __init__(self, table: str, columns: List[str]):
        self.table = table
        self.columns = columns
        self.lock = threading.RLock()
    
    def insert(self, key: str, row: Dict[str, Any]):
        """Insert a row into the index."""
        raise NotImplementedError
    
    def delete(self, key: str, row: Dict[str, Any]):
        """Delete a row from the index."""
        raise NotImplementedError
    
    def lookup(self, **conditions) -> Set[str]:
        """Lookup rows matching conditions."""
        raise NotImplementedError
    
    def range_scan(self, column: str, start: Any, end: Any) -> Set[str]:
        """Range scan for sorted indexes."""
        raise NotImplementedError


class HashIndex(Index):
    """Hash index for exact-match lookups."""
    
    def __init__(self, table: str, columns: List[str]):
        super().__init__(table, columns)
        # index_value -> set of row keys
        self.index: Dict[Any, Set[str]] = defaultdict(set)
    
    def _get_index_key(self, row: Dict[str, Any]) -> Optional[Tuple]:
        """Get index key from row."""
        try:
            return tuple(row.get(col) for col in self.columns)
        except (KeyError, TypeError):
            return None
    
    def insert(self, key: str, row: Dict[str, Any]):
        """Insert a row into the hash index."""
        with self.lock:
            index_key = self._get_index_key(row)
            if index_key is not None:
                self.index[index_key].add(key)
    
    def delete(self, key: str, row: Dict[str, Any]):
        """Delete a row from the hash index."""
        with self.lock:
            index_key = self._get_index_key(row)
            if index_key is not None and index_key in self.index:
                self.index[index_key].discard(key)
                if not self.index[index_key]:
                    del self.index[index_key]
    
    def lookup(self, **conditions) -> Set[str]:
        """Lookup rows matching exact conditions."""
        with self.lock:
            # Build lookup key from conditions
            try:
                lookup_key = tuple(conditions.get(col) for col in self.columns)
                return self.index.get(lookup_key, set()).copy()
            except (KeyError, TypeError):
                return set()
    
    def range_scan(self, column: str, start: Any, end: Any) -> Set[str]:
        """Hash indexes don't support range scans."""
        raise NotImplementedError("Hash indexes don't support range scans")


class BTreeIndex(Index):
    """B-tree index for range queries."""
    
    def __init__(self, table: str, columns: List[str]):
        super().__init__(table, columns)
        # Only single column for B-tree in this implementation
        if len(columns) != 1:
            raise ValueError("BTreeIndex only supports single column")
        
        # value -> set of row keys
        self.index: SortedDict = SortedDict()
    
    def _get_index_value(self, row: Dict[str, Any]) -> Any:
        """Get index value from row."""
        try:
            return row.get(self.columns[0])
        except (KeyError, TypeError):
            return None
    
    def insert(self, key: str, row: Dict[str, Any]):
        """Insert a row into the B-tree index."""
        with self.lock:
            value = self._get_index_value(row)
            if value is not None:
                if value not in self.index:
                    self.index[value] = set()
                self.index[value].add(key)
    
    def delete(self, key: str, row: Dict[str, Any]):
        """Delete a row from the B-tree index."""
        with self.lock:
            value = self._get_index_value(row)
            if value is not None and value in self.index:
                self.index[value].discard(key)
                if not self.index[value]:
                    del self.index[value]
    
    def lookup(self, **conditions) -> Set[str]:
        """Lookup rows matching exact value."""
        with self.lock:
            column = self.columns[0]
            if column in conditions:
                value = conditions[column]
                return self.index.get(value, set()).copy()
            return set()
    
    def range_scan(self, column: str, start: Any = None, end: Any = None) -> Set[str]:
        """Range scan on the indexed column."""
        with self.lock:
            if column != self.columns[0]:
                raise ValueError(f"Index is on {self.columns[0]}, not {column}")
            
            result = set()
            
            # Determine range
            if start is None and end is None:
                # Full scan
                for keys in self.index.values():
                    result.update(keys)
            elif start is None:
                # Scan up to end
                for value in self.index.irange(maximum=end):
                    result.update(self.index[value])
            elif end is None:
                # Scan from start
                for value in self.index.irange(minimum=start):
                    result.update(self.index[value])
            else:
                # Scan from start to end
                for value in self.index.irange(minimum=start, maximum=end):
                    result.update(self.index[value])
            
            return result


class IndexManager:
    """Manages all indexes for the database."""
    
    def __init__(self):
        # table -> index_name -> Index
        self.indexes: Dict[str, Dict[str, Index]] = defaultdict(dict)
        self.lock = threading.RLock()
    
    def create_index(self, index_name: str, table: str, columns: List[str], index_type: str = 'btree') -> Index:
        """Create a new index."""
        with self.lock:
            if index_name in self.indexes[table]:
                raise ValueError(f"Index {index_name} already exists on table {table}")
            
            if index_type == 'hash':
                index = HashIndex(table, columns)
            elif index_type == 'btree':
                index = BTreeIndex(table, columns)
            else:
                raise ValueError(f"Unknown index type: {index_type}")
            
            self.indexes[table][index_name] = index
            return index
    
    def drop_index(self, table: str, index_name: str):
        """Drop an index."""
        with self.lock:
            if table in self.indexes and index_name in self.indexes[table]:
                del self.indexes[table][index_name]
    
    def get_index(self, table: str, index_name: str) -> Optional[Index]:
        """Get an index by name."""
        with self.lock:
            return self.indexes.get(table, {}).get(index_name)
    
    def get_indexes_for_table(self, table: str) -> Dict[str, Index]:
        """Get all indexes for a table."""
        with self.lock:
            return dict(self.indexes.get(table, {}))
    
    def insert_row(self, table: str, key: str, row: Dict[str, Any]):
        """Update all indexes when a row is inserted."""
        with self.lock:
            for index in self.indexes.get(table, {}).values():
                index.insert(key, row)
    
    def delete_row(self, table: str, key: str, row: Dict[str, Any]):
        """Update all indexes when a row is deleted."""
        with self.lock:
            for index in self.indexes.get(table, {}).values():
                index.delete(key, row)
    
    def find_best_index(self, table: str, conditions: Dict[str, Any]) -> Optional[Index]:
        """Find the best index for given conditions."""
        with self.lock:
            best_index = None
            best_score = -1
            
            for index in self.indexes.get(table, {}).values():
                # Calculate how many columns match
                score = sum(1 for col in index.columns if col in conditions)
                if score > best_score:
                    best_score = score
                    best_index = index
            
            return best_index if best_score > 0 else None
