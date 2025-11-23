"""Client library for connecting to DistDB."""

import logging
from typing import Dict, Any, List, Optional

from .config import Config
from .node import Node


logger = logging.getLogger(__name__)


class Client:
    """Client for interacting with DistDB."""
    
    def __init__(self, node_address: Optional[str] = None):
        """
        Initialize client.
        
        Args:
            node_address: Address of node to connect to (node_id@host:port)
                         If None, starts a local node.
        """
        self.node_address = node_address
        self.local_node: Optional[Node] = None
        
        if node_address is None:
            # Start a local node
            config = Config()
            self.local_node = Node(config)
            self.local_node.start()
        else:
            # TODO: In a full implementation, would connect via gRPC
            # For now, only support local node
            raise NotImplementedError("Remote connections not yet implemented")
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """
        Execute a SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            Query result dictionary
        """
        if self.local_node:
            return self.local_node.execute_query(sql)
        else:
            # TODO: Send query via gRPC
            raise NotImplementedError("Remote queries not yet implemented")
    
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return rows.
        
        Args:
            sql: SELECT query string
            
        Returns:
            List of result rows
        """
        result = self.execute(sql)
        if result.get('status') == 'success' and 'rows' in result:
            return result['rows']
        else:
            raise Exception(result.get('message', 'Query failed'))
    
    def execute_many(self, sql_statements: List[str]) -> List[Dict[str, Any]]:
        """
        Execute multiple SQL statements.
        
        Args:
            sql_statements: List of SQL query strings
            
        Returns:
            List of results
        """
        results = []
        for sql in sql_statements:
            result = self.execute(sql)
            results.append(result)
        return results
    
    def create_table(self, table_name: str, schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Create a table.
        
        Args:
            table_name: Name of the table
            schema: Dictionary mapping column names to types
            
        Returns:
            Result dictionary
        """
        # Build CREATE TABLE SQL
        columns = [f"{col} {dtype}" for col, dtype in schema.items()]
        sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        return self.execute(sql)
    
    def insert(self, table_name: str, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert a row.
        
        Args:
            table_name: Name of the table
            values: Dictionary of column values
            
        Returns:
            Result dictionary
        """
        columns = ', '.join(values.keys())
        value_strs = []
        for v in values.values():
            if isinstance(v, str):
                value_strs.append(f"'{v}'")
            else:
                value_strs.append(str(v))
        values_str = ', '.join(value_strs)
        
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"
        return self.execute(sql)
    
    def select(self, table_name: str, where: Optional[Dict[str, Any]] = None,
              order_by: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Select rows from a table.
        
        Args:
            table_name: Name of the table
            where: WHERE conditions (column: value)
            order_by: ORDER BY column
            limit: LIMIT number
            
        Returns:
            List of result rows
        """
        sql = f"SELECT * FROM {table_name}"
        
        if where:
            conditions = []
            for col, val in where.items():
                if isinstance(val, str):
                    conditions.append(f"{col} = '{val}'")
                else:
                    conditions.append(f"{col} = {val}")
            sql += " WHERE " + " AND ".join(conditions)
        
        if order_by:
            sql += f" ORDER BY {order_by}"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.query(sql)
    
    def update(self, table_name: str, values: Dict[str, Any], 
              where: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Update rows in a table.
        
        Args:
            table_name: Name of the table
            values: Columns to update
            where: WHERE conditions
            
        Returns:
            Result dictionary
        """
        set_parts = []
        for col, val in values.items():
            if isinstance(val, str):
                set_parts.append(f"{col} = '{val}'")
            else:
                set_parts.append(f"{col} = {val}")
        
        sql = f"UPDATE {table_name} SET {', '.join(set_parts)}"
        
        if where:
            conditions = []
            for col, val in where.items():
                if isinstance(val, str):
                    conditions.append(f"{col} = '{val}'")
                else:
                    conditions.append(f"{col} = {val}")
            sql += " WHERE " + " AND ".join(conditions)
        
        return self.execute(sql)
    
    def delete(self, table_name: str, where: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Delete rows from a table.
        
        Args:
            table_name: Name of the table
            where: WHERE conditions
            
        Returns:
            Result dictionary
        """
        sql = f"DELETE FROM {table_name}"
        
        if where:
            conditions = []
            for col, val in where.items():
                if isinstance(val, str):
                    conditions.append(f"{col} = '{val}'")
                else:
                    conditions.append(f"{col} = {val}")
            sql += " WHERE " + " AND ".join(conditions)
        
        return self.execute(sql)
    
    def create_index(self, index_name: str, table_name: str, 
                    columns: List[str], index_type: str = 'btree') -> Dict[str, Any]:
        """
        Create an index.
        
        Args:
            index_name: Name of the index
            table_name: Name of the table
            columns: Columns to index
            index_type: 'btree' or 'hash'
            
        Returns:
            Result dictionary
        """
        columns_str = ', '.join(columns)
        sql = f"CREATE INDEX {index_name} ON {table_name} ({columns_str})"
        if index_type == 'hash':
            sql += " USING HASH"
        return self.execute(sql)
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status."""
        if self.local_node:
            return self.local_node.get_status()
        else:
            raise NotImplementedError("Remote status not yet implemented")
    
    def close(self):
        """Close the client and stop local node if running."""
        if self.local_node:
            self.local_node.stop()
