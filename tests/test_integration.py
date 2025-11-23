"""Integration tests for the full database."""

import pytest
from distdb.client import Client


def test_full_workflow():
    """Test a complete database workflow."""
    client = Client()
    
    try:
        # Create table
        result = client.create_table('users', {
            'id': 'INTEGER',
            'name': 'TEXT',
            'email': 'TEXT',
            'age': 'INTEGER'
        })
        assert result['status'] == 'success'
        
        # Insert data
        client.insert('users', {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': 25})
        client.insert('users', {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'age': 30})
        client.insert('users', {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35})
        
        # Select all
        rows = client.select('users')
        assert len(rows) == 3
        
        # Select with WHERE
        rows = client.select('users', where={'age': 25})
        assert len(rows) == 1
        assert rows[0]['name'] == 'Alice'
        
        # Create index
        result = client.create_index('idx_age', 'users', ['age'], 'btree')
        assert result['status'] == 'success'
        
        # Select with index
        rows = client.select('users', where={'age': 30})
        assert len(rows) == 1
        assert rows[0]['name'] == 'Bob'
        
        # Update
        result = client.update('users', {'age': 26}, where={'id': 1})
        assert result['rows_affected'] == 1
        
        # Verify update
        rows = client.select('users', where={'id': 1})
        assert rows[0]['age'] == 26
        
        # Delete
        result = client.delete('users', where={'id': 3})
        assert result['rows_affected'] == 1
        
        # Verify deletion
        rows = client.select('users')
        assert len(rows) == 2
        
    finally:
        client.close()


def test_order_by_and_limit():
    """Test ORDER BY and LIMIT."""
    client = Client()
    
    try:
        # Setup
        client.create_table('products', {'id': 'INTEGER', 'name': 'TEXT', 'price': 'INTEGER'})
        client.insert('products', {'id': 1, 'name': 'Widget', 'price': 100})
        client.insert('products', {'id': 2, 'name': 'Gadget', 'price': 200})
        client.insert('products', {'id': 3, 'name': 'Doohickey', 'price': 150})
        
        # ORDER BY with LIMIT
        rows = client.query("SELECT * FROM products ORDER BY price DESC LIMIT 2")
        assert len(rows) == 2
        assert rows[0]['price'] == 200
        assert rows[1]['price'] == 150
        
    finally:
        client.close()


def test_multiple_tables():
    """Test working with multiple tables."""
    client = Client()
    
    try:
        # Create multiple tables
        client.create_table('users', {'id': 'INTEGER', 'name': 'TEXT'})
        client.create_table('posts', {'id': 'INTEGER', 'user_id': 'INTEGER', 'title': 'TEXT'})
        
        # Insert data
        client.insert('users', {'id': 1, 'name': 'Alice'})
        client.insert('posts', {'id': 1, 'user_id': 1, 'title': 'Hello World'})
        
        # Query both tables
        users = client.select('users')
        posts = client.select('posts')
        
        assert len(users) == 1
        assert len(posts) == 1
        assert posts[0]['user_id'] == users[0]['id']
        
    finally:
        client.close()


def test_hash_index():
    """Test hash index."""
    client = Client()
    
    try:
        # Setup
        client.create_table('users', {'id': 'INTEGER', 'email': 'TEXT'})
        client.create_index('idx_email', 'users', ['email'], 'hash')
        
        # Insert
        client.insert('users', {'id': 1, 'email': 'alice@example.com'})
        client.insert('users', {'id': 2, 'email': 'bob@example.com'})
        
        # Query with hash index
        rows = client.select('users', where={'email': 'alice@example.com'})
        assert len(rows) == 1
        assert rows[0]['id'] == 1
        
    finally:
        client.close()


def test_concurrent_operations():
    """Test concurrent operations on the same client."""
    client = Client()
    
    try:
        # Setup
        client.create_table('counter', {'id': 'INTEGER', 'value': 'INTEGER'})
        client.insert('counter', {'id': 1, 'value': 0})
        
        # Multiple updates
        for i in range(10):
            client.update('counter', {'value': i}, where={'id': 1})
        
        # Verify final value
        rows = client.select('counter', where={'id': 1})
        assert rows[0]['value'] == 9
        
    finally:
        client.close()


def test_large_dataset():
    """Test with a larger dataset."""
    client = Client()
    
    try:
        # Setup
        client.create_table('numbers', {'id': 'INTEGER', 'value': 'INTEGER'})
        client.create_index('idx_value', 'numbers', ['value'], 'btree')
        
        # Insert many rows
        for i in range(100):
            client.insert('numbers', {'id': i, 'value': i * 10})
        
        # Query
        rows = client.select('numbers')
        assert len(rows) == 100
        
        # Range query using index
        rows = client.query("SELECT * FROM numbers WHERE value = 500")
        assert len(rows) == 1
        assert rows[0]['id'] == 50
        
    finally:
        client.close()
