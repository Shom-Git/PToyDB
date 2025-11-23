"""Tests for query executor."""

import pytest
from distdb.storage_engine import StorageEngine
from distdb.index_manager import IndexManager
from distdb.sql_parser import SQLParser
from distdb.query_executor import QueryExecutor


@pytest.fixture
def executor(storage_engine, index_manager):
    return QueryExecutor(storage_engine, index_manager)


@pytest.fixture
def parser():
    return SQLParser()


def test_execute_create_table(executor, parser):
    """Test executing CREATE TABLE."""
    sql = "CREATE TABLE users (id INTEGER, name TEXT)"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert 'users' in executor.storage.list_tables()


def test_execute_insert(executor, parser):
    """Test executing INSERT."""
    # Create table first
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    
    # Insert data
    sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['rows_affected'] == 1


def test_execute_select(executor, parser):
    """Test executing SELECT."""
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
    
    # Select
    sql = "SELECT * FROM users"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['row_count'] == 2


def test_execute_select_where(executor, parser):
    """Test executing SELECT with WHERE."""
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
    
    # Select with WHERE
    sql = "SELECT * FROM users WHERE id = 1"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['row_count'] == 1
    assert result['rows'][0]['name'] == 'Alice'


def test_execute_select_order_by(executor, parser):
    """Test executing SELECT with ORDER BY."""
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (3, 'Charlie')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
    
    # Select with ORDER BY
    sql = "SELECT * FROM users ORDER BY id ASC"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['rows'][0]['id'] == 1
    assert result['rows'][1]['id'] == 2
    assert result['rows'][2]['id'] == 3


def test_execute_select_limit(executor, parser):
    """Test executing SELECT with LIMIT."""
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (3, 'Charlie')"))
    
    # Select with LIMIT
    sql = "SELECT * FROM users LIMIT 2"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['row_count'] == 2


def test_execute_update(executor, parser):
    """Test executing UPDATE."""
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    
    # Update
    sql = "UPDATE users SET name = 'Alicia' WHERE id = 1"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['rows_affected'] == 1
    
    # Verify update
    rows = executor.storage.scan('users')
    assert rows[0][1]['name'] == 'Alicia'


def test_execute_delete(executor, parser):
    """Test executing DELETE."""
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
    
    # Delete
    sql = "DELETE FROM users WHERE id = 1"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['rows_affected'] == 1
    
    # Verify deletion
    rows = executor.storage.scan('users')
    assert len(rows) == 1


def test_execute_with_index(executor, parser):
    """Test query execution uses indexes."""
    # Setup table and index
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER, name TEXT)"))
    executor.execute(parser.parse("CREATE INDEX idx_id ON users (id)"))
    
    # Insert data
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    executor.execute(parser.parse("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
    
    # Select should use index
    sql = "SELECT * FROM users WHERE id = 1"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert result['row_count'] == 1


def test_execute_drop_table(executor, parser):
    """Test executing DROP TABLE."""
    # Create table
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER)"))
    assert 'users' in executor.storage.list_tables()
    
    # Drop table
    sql = "DROP TABLE users"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result['status'] == 'success'
    assert 'users' not in executor.storage.list_tables()
