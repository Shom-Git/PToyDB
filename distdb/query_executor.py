"""Query executor for executing parsed SQL queries."""

import threading
import uuid
from typing import List, Dict, Any, Optional
from .storage_engine import StorageEngine
from .index_manager import IndexManager
from .sql_parser import ParsedQuery


class QueryExecutor:
    """Execute parsed queries against the storage engine."""
    
    def __init__(self, storage: StorageEngine, index_manager: IndexManager):
        self.storage = storage
        self.index_manager = index_manager
        self.lock = threading.RLock()
    
    def execute(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute a parsed query and return results."""
        with self.lock:
            if query.query_type == 'CREATE_TABLE':
                return self._execute_create_table(query)
            elif query.query_type == 'DROP_TABLE':
                return self._execute_drop_table(query)
            elif query.query_type == 'CREATE_INDEX':
                return self._execute_create_index(query)
            elif query.query_type == 'DROP_INDEX':
                return self._execute_drop_index(query)
            elif query.query_type == 'INSERT':
                return self._execute_insert(query)
            elif query.query_type == 'SELECT':
                return self._execute_select(query)
            elif query.query_type == 'UPDATE':
                return self._execute_update(query)
            elif query.query_type == 'DELETE':
                return self._execute_delete(query)
            else:
                raise ValueError(f"Unknown query type: {query.query_type}")
    
    def _execute_create_table(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute CREATE TABLE."""
        self.storage.create_table(query.table, query.schema)
        return {
            'status': 'success',
            'message': f"Table {query.table} created",
            'rows_affected': 0
        }
    
    def _execute_drop_table(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute DROP TABLE."""
        self.storage.drop_table(query.table)
        # Also drop all indexes for this table
        indexes = list(self.index_manager.get_indexes_for_table(query.table).keys())
        for index_name in indexes:
            self.index_manager.drop_index(query.table, index_name)
        
        return {
            'status': 'success',
            'message': f"Table {query.table} dropped",
            'rows_affected': 0
        }
    
    def _execute_create_index(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute CREATE INDEX."""
        index = self.index_manager.create_index(
            query.index_name, 
            query.table, 
            query.index_columns,
            query.index_type
        )
        
        # Build index from existing data
        for key, row in self.storage.scan(query.table):
            index.insert(key, row)
        
        return {
            'status': 'success',
            'message': f"Index {query.index_name} created on {query.table}",
            'rows_affected': 0
        }
    
    def _execute_drop_index(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute DROP INDEX."""
        self.index_manager.drop_index(query.table, query.index_name)
        return {
            'status': 'success',
            'message': f"Index {query.index_name} dropped",
            'rows_affected': 0
        }
    
    def _execute_insert(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute INSERT."""
        # Generate a unique key
        key = str(uuid.uuid4())
        
        # Insert into storage
        self.storage.put(query.table, key, query.values)
        
        # Update indexes
        self.index_manager.insert_row(query.table, key, query.values)
        
        return {
            'status': 'success',
            'message': 'Row inserted',
            'rows_affected': 1,
            'inserted_key': key
        }
    
    def _execute_select(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute SELECT."""
        # Get candidate rows
        if query.conditions:
            # Try to use an index
            index = self.index_manager.find_best_index(query.table, query.conditions)
            if index:
                # Use index
                candidate_keys = index.lookup(**query.conditions)
                rows = []
                for key in candidate_keys:
                    row = self.storage.get(query.table, key)
                    if row and self._matches_conditions(row, query.conditions):
                        rows.append({**row, '_key': key})
            else:
                # Full table scan
                rows = []
                for key, row in self.storage.scan(query.table):
                    if self._matches_conditions(row, query.conditions):
                        rows.append({**row, '_key': key})
        else:
            # No conditions, scan all
            rows = [{**row, '_key': key} for key, row in self.storage.scan(query.table)]
        
        # Apply ORDER BY
        if query.order_by:
            for col, direction in reversed(query.order_by):
                reverse = (direction == 'DESC')
                rows.sort(key=lambda r: r.get(col, ''), reverse=reverse)
        
        # Apply LIMIT
        if query.limit is not None:
            rows = rows[:query.limit]
        
        # Project columns
        if query.columns and query.columns != ['*']:
            projected_rows = []
            for row in rows:
                projected_row = {col: row.get(col) for col in query.columns if col in row}
                projected_rows.append(projected_row)
            rows = projected_rows
        
        return {
            'status': 'success',
            'rows': rows,
            'row_count': len(rows)
        }
    
    def _execute_update(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute UPDATE."""
        rows_affected = 0
        
        # Find matching rows
        for key, row in self.storage.scan(query.table):
            if self._matches_conditions(row, query.conditions):
                # Update indexes - remove old entry
                self.index_manager.delete_row(query.table, key, row)
                
                # Update row
                updated_row = {**row, **query.values}
                self.storage.put(query.table, key, updated_row)
                
                # Update indexes - add new entry
                self.index_manager.insert_row(query.table, key, updated_row)
                
                rows_affected += 1
        
        return {
            'status': 'success',
            'message': f'{rows_affected} rows updated',
            'rows_affected': rows_affected
        }
    
    def _execute_delete(self, query: ParsedQuery) -> Dict[str, Any]:
        """Execute DELETE."""
        rows_affected = 0
        
        # Find matching rows
        keys_to_delete = []
        for key, row in self.storage.scan(query.table):
            if self._matches_conditions(row, query.conditions):
                keys_to_delete.append((key, row))
        
        # Delete them
        for key, row in keys_to_delete:
            self.storage.delete(query.table, key)
            self.index_manager.delete_row(query.table, key, row)
            rows_affected += 1
        
        return {
            'status': 'success',
            'message': f'{rows_affected} rows deleted',
            'rows_affected': rows_affected
        }
    
    def _matches_conditions(self, row: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """Check if a row matches the WHERE conditions."""
        for col, value in conditions.items():
            if row.get(col) != value:
                return False
        return True
