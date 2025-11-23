"""Tests for SQL parser."""

import pytest
from distdb.sql_parser import SQLParser


@pytest.fixture
def parser():
    return SQLParser()


def test_parse_create_table(parser):
    """Test parsing CREATE TABLE."""
    sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
    query = parser.parse(sql)
    
    assert query.query_type == 'CREATE_TABLE'
    assert query.table == 'users'
    assert query.schema == {'id': 'INTEGER', 'name': 'TEXT', 'age': 'INTEGER'}


def test_parse_drop_table(parser):
    """Test parsing DROP TABLE."""
    sql = "DROP TABLE users"
    query = parser.parse(sql)
    
    assert query.query_type == 'DROP_TABLE'
    assert query.table == 'users'


def test_parse_insert(parser):
    """Test parsing INSERT."""
    sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"
    query = parser.parse(sql)
    
    assert query.query_type == 'INSERT'
    assert query.table == 'users'
    assert query.values == {'id': 1, 'name': 'Alice', 'age': 25}


def test_parse_select_all(parser):
    """Test parsing SELECT *."""
    sql = "SELECT * FROM users"
    query = parser.parse(sql)
    
    assert query.query_type == 'SELECT'
    assert query.table == 'users'
    assert query.columns == ['*']


def test_parse_select_columns(parser):
    """Test parsing SELECT with specific columns."""
    sql = "SELECT id, name FROM users"
    query = parser.parse(sql)
    
    assert query.query_type == 'SELECT'
    assert query.table == 'users'
    assert 'id' in query.columns
    assert 'name' in query.columns


def test_parse_select_where(parser):
    """Test parsing SELECT with WHERE clause."""
    sql = "SELECT * FROM users WHERE age = 25"
    query = parser.parse(sql)
    
    assert query.query_type == 'SELECT'
    assert query.table == 'users'
    assert query.conditions == {'age': 25}


def test_parse_select_order_by(parser):
    """Test parsing SELECT with ORDER BY."""
    sql = "SELECT * FROM users ORDER BY age DESC"
    query = parser.parse(sql)
    
    assert query.query_type == 'SELECT'
    assert len(query.order_by) > 0
    assert query.order_by[0][0] == 'age'
    assert query.order_by[0][1] == 'DESC'


def test_parse_select_limit(parser):
    """Test parsing SELECT with LIMIT."""
    sql = "SELECT * FROM users LIMIT 10"
    query = parser.parse(sql)
    
    assert query.query_type == 'SELECT'
    assert query.limit == 10


def test_parse_update(parser):
    """Test parsing UPDATE."""
    sql = "UPDATE users SET age = 26 WHERE id = 1"
    query = parser.parse(sql)
    
    assert query.query_type == 'UPDATE'
    assert query.table == 'users'
    assert query.values == {'age': 26}
    assert query.conditions == {'id': 1}


def test_parse_delete(parser):
    """Test parsing DELETE."""
    sql = "DELETE FROM users WHERE age = 25"
    query = parser.parse(sql)
    
    assert query.query_type == 'DELETE'
    assert query.table == 'users'
    assert query.conditions == {'age': 25}


def test_parse_create_index(parser):
    """Test parsing CREATE INDEX."""
    sql = "CREATE INDEX idx_age ON users (age)"
    query = parser.parse(sql)
    
    assert query.query_type == 'CREATE_INDEX'
    assert query.index_name == 'idx_age'
    assert query.table == 'users'
    assert query.index_columns == ['age']
    assert query.index_type == 'btree'


def test_parse_create_hash_index(parser):
    """Test parsing CREATE INDEX with HASH."""
    sql = "CREATE INDEX idx_name ON users (name) USING HASH"
    query = parser.parse(sql)
    
    assert query.query_type == 'CREATE_INDEX'
    assert query.index_name == 'idx_name'
    assert query.table == 'users'
    assert query.index_columns == ['name']
    assert query.index_type == 'hash'


def test_parse_drop_index(parser):
    """Test parsing DROP INDEX."""
    sql = "DROP INDEX idx_age ON users"
    query = parser.parse(sql)
    
    assert query.query_type == 'DROP_INDEX'
    assert query.index_name == 'idx_age'
    assert query.table == 'users'
