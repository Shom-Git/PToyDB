"""SQL parser for converting SQL to internal query representation."""

import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Token
from sqlparse.tokens import Keyword, DML
from typing import Dict, List, Any, Optional, Tuple


class ParsedQuery:
    """Represents a parsed SQL query."""
    
    def __init__(self, query_type: str):
        self.query_type = query_type  # SELECT, INSERT, UPDATE, DELETE, CREATE_TABLE, etc.
        self.table: Optional[str] = None
        self.columns: List[str] = []
        self.conditions: Dict[str, Any] = {}
        self.values: Dict[str, Any] = {}
        self.schema: Dict[str, str] = {}
        self.order_by: List[Tuple[str, str]] = []  # [(column, direction)]
        self.limit: Optional[int] = None
        self.index_name: Optional[str] = None
        self.index_columns: List[str] = []
        self.index_type: str = 'btree'
    
    def __repr__(self):
        return f"ParsedQuery(type={self.query_type}, table={self.table})"


class SQLParser:
    """Parse SQL queries into internal representation."""
    
    def parse(self, sql: str) -> ParsedQuery:
        """Parse SQL string into ParsedQuery."""
        # Parse the SQL
        statements = sqlparse.parse(sql)
        if not statements:
            raise ValueError("No SQL statement found")
        
        statement = statements[0]
        
        # Determine query type
        query_type = self._get_query_type(statement)
        query = ParsedQuery(query_type)
        
        # Route to appropriate parser
        if query_type == 'SELECT':
            self._parse_select(statement, query)
        elif query_type == 'INSERT':
            self._parse_insert(statement, query)
        elif query_type == 'UPDATE':
            self._parse_update(statement, query)
        elif query_type == 'DELETE':
            self._parse_delete(statement, query)
        elif query_type == 'CREATE_TABLE':
            self._parse_create_table(statement, query)
        elif query_type == 'DROP_TABLE':
            self._parse_drop_table(statement, query)
        elif query_type == 'CREATE_INDEX':
            self._parse_create_index(statement, query)
        elif query_type == 'DROP_INDEX':
            self._parse_drop_index(statement, query)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")
        
        return query
    
    def _get_query_type(self, statement) -> str:
        """Determine the type of SQL statement."""
        first_token = statement.token_first(skip_ws=True, skip_cm=True)
        if first_token:
            value = first_token.value.upper()
            if value == 'SELECT':
                return 'SELECT'
            elif value == 'INSERT':
                return 'INSERT'
            elif value == 'UPDATE':
                return 'UPDATE'
            elif value == 'DELETE':
                return 'DELETE'
            elif value == 'CREATE':
                # Check if CREATE TABLE or CREATE INDEX
                tokens = [t for t in statement.tokens if not t.is_whitespace]
                if len(tokens) > 1:
                    second = tokens[1].value.upper()
                    if second == 'TABLE':
                        return 'CREATE_TABLE'
                    elif second == 'INDEX':
                        return 'CREATE_INDEX'
            elif value == 'DROP':
                tokens = [t for t in statement.tokens if not t.is_whitespace]
                if len(tokens) > 1:
                    second = tokens[1].value.upper()
                    if second == 'TABLE':
                        return 'DROP_TABLE'
                    elif second == 'INDEX':
                        return 'DROP_INDEX'
        raise ValueError("Unknown query type")
    
    def _parse_select(self, statement, query: ParsedQuery):
        """Parse SELECT statement."""
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        
        i = 1  # Skip SELECT
        
        # Parse columns
        if i < len(tokens):
            if isinstance(tokens[i], IdentifierList):
                query.columns = [str(ident).strip() for ident in tokens[i].get_identifiers()]
            elif isinstance(tokens[i], Identifier):
                query.columns = [str(tokens[i]).strip()]
            elif tokens[i].ttype is None and str(tokens[i]).strip() == '*':
                query.columns = ['*']
            i += 1
        
        # Parse FROM
        while i < len(tokens):
            if tokens[i].ttype is Keyword and tokens[i].value.upper() == 'FROM':
                i += 1
                if i < len(tokens):
                    query.table = str(tokens[i]).strip()
                    i += 1
                break
            i += 1
        
        # Parse WHERE
        while i < len(tokens):
            if isinstance(tokens[i], Where):
                query.conditions = self._parse_where(tokens[i])
                i += 1
            elif tokens[i].ttype is Keyword:
                keyword = tokens[i].value.upper()
                if keyword == 'ORDER':
                    # Parse ORDER BY
                    i += 1
                    if i < len(tokens) and tokens[i].value.upper() == 'BY':
                        i += 1
                        if i < len(tokens):
                            order_str = str(tokens[i]).strip()
                            query.order_by = self._parse_order_by(order_str)
                            i += 1
                elif keyword == 'LIMIT':
                    i += 1
                    if i < len(tokens):
                        query.limit = int(str(tokens[i]).strip())
                        i += 1
                else:
                    i += 1
            else:
                i += 1
    
    def _parse_insert(self, statement, query: ParsedQuery):
        """Parse INSERT statement."""
        # Simple regex-based parsing for INSERT
        sql = str(statement).strip()
        
        # Extract table name
        match = re.search(r'INSERT\s+INTO\s+(\w+)', sql, re.IGNORECASE)
        if match:
            query.table = match.group(1)
        
        # Extract columns and values
        columns_match = re.search(r'\(([^)]+)\)\s+VALUES\s+\(([^)]+)\)', sql, re.IGNORECASE)
        if columns_match:
            columns = [c.strip() for c in columns_match.group(1).split(',')]
            values_str = [v.strip().strip("'\"") for v in columns_match.group(2).split(',')]
            
            query.values = {}
            for col, val in zip(columns, values_str):
                # Try to convert to appropriate type
                query.values[col] = self._convert_value(val)
    
    def _parse_update(self, statement, query: ParsedQuery):
        """Parse UPDATE statement."""
        sql = str(statement).strip()
        
        # Extract table name
        match = re.search(r'UPDATE\s+(\w+)', sql, re.IGNORECASE)
        if match:
            query.table = match.group(1)
        
        # Extract SET clause
        set_match = re.search(r'SET\s+(.+?)(?:WHERE|$)', sql, re.IGNORECASE)
        if set_match:
            set_str = set_match.group(1).strip()
            query.values = self._parse_set_clause(set_str)
        
        # Extract WHERE clause
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        for token in tokens:
            if isinstance(token, Where):
                query.conditions = self._parse_where(token)
    
    def _parse_delete(self, statement, query: ParsedQuery):
        """Parse DELETE statement."""
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        
        i = 1  # Skip DELETE
        
        # Skip FROM
        while i < len(tokens):
            if tokens[i].ttype is Keyword and tokens[i].value.upper() == 'FROM':
                i += 1
                if i < len(tokens):
                    query.table = str(tokens[i]).strip()
                    i += 1
                break
            i += 1
        
        # Parse WHERE
        for token in tokens[i:]:
            if isinstance(token, Where):
                query.conditions = self._parse_where(token)
    
    def _parse_create_table(self, statement, query: ParsedQuery):
        """Parse CREATE TABLE statement."""
        sql = str(statement).strip()
        
        # Extract table name and schema
        match = re.search(r'CREATE\s+TABLE\s+(\w+)\s*\(([^)]+)\)', sql, re.IGNORECASE)
        if match:
            query.table = match.group(1)
            schema_str = match.group(2)
            
            # Parse schema
            query.schema = {}
            for col_def in schema_str.split(','):
                col_def = col_def.strip()
                parts = col_def.split()
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = parts[1].upper()
                    query.schema[col_name] = col_type
    
    def _parse_drop_table(self, statement, query: ParsedQuery):
        """Parse DROP TABLE statement."""
        sql = str(statement).strip()
        match = re.search(r'DROP\s+TABLE\s+(\w+)', sql, re.IGNORECASE)
        if match:
            query.table = match.group(1)
    
    def _parse_create_index(self, statement, query: ParsedQuery):
        """Parse CREATE INDEX statement."""
        sql = str(statement).strip()
        
        # CREATE INDEX idx_name ON table (columns)
        match = re.search(r'CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)', sql, re.IGNORECASE)
        if match:
            query.index_name = match.group(1)
            query.table = match.group(2)
            query.index_columns = [c.strip() for c in match.group(3).split(',')]
        
        # Check for index type (USING HASH or USING BTREE)
        if 'USING HASH' in sql.upper():
            query.index_type = 'hash'
        else:
            query.index_type = 'btree'
    
    def _parse_drop_index(self, statement, query: ParsedQuery):
        """Parse DROP INDEX statement."""
        sql = str(statement).strip()
        
        # DROP INDEX idx_name ON table
        match = re.search(r'DROP\s+INDEX\s+(\w+)(?:\s+ON\s+(\w+))?', sql, re.IGNORECASE)
        if match:
            query.index_name = match.group(1)
            if match.group(2):
                query.table = match.group(2)
    
    def _parse_where(self, where_clause) -> Dict[str, Any]:
        """Parse WHERE clause into conditions."""
        conditions = {}
        where_str = str(where_clause).replace('WHERE', '', 1).strip()
        
        # Simple parsing for basic conditions (column = value AND column2 = value2)
        # Split by AND
        parts = re.split(r'\s+AND\s+', where_str, flags=re.IGNORECASE)
        
        for part in parts:
            # Parse column = value or column > value, etc.
            match = re.search(r'(\w+)\s*=\s*(.+)', part.strip())
            if match:
                column = match.group(1).strip()
                value = match.group(2).strip().strip("'\"")
                conditions[column] = self._convert_value(value)
        
        return conditions
    
    def _parse_order_by(self, order_str: str) -> List[Tuple[str, str]]:
        """Parse ORDER BY clause."""
        result = []
        for part in order_str.split(','):
            part = part.strip()
            if ' ' in part:
                col, direction = part.rsplit(None, 1)
                direction = direction.upper()
                if direction not in ('ASC', 'DESC'):
                    col = part
                    direction = 'ASC'
            else:
                col = part
                direction = 'ASC'
            result.append((col, direction))
        return result
    
    def _parse_set_clause(self, set_str: str) -> Dict[str, Any]:
        """Parse SET clause from UPDATE."""
        values = {}
        for assignment in set_str.split(','):
            if '=' in assignment:
                col, val = assignment.split('=', 1)
                col = col.strip()
                val = val.strip().strip("'\"")
                values[col] = self._convert_value(val)
        return values
    
    def _convert_value(self, value: str) -> Any:
        """Convert string value to appropriate Python type."""
        # Try int
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Keep as string
        return value
