"""Tests for index manager."""

import pytest
from distdb.index_manager import IndexManager, HashIndex, BTreeIndex


def test_create_hash_index(index_manager):
    """Test creating a hash index."""
    index = index_manager.create_index('idx_name', 'users', ['name'], 'hash')
    assert isinstance(index, HashIndex)
    assert index.table == 'users'
    assert index.columns == ['name']


def test_create_btree_index(index_manager):
    """Test creating a B-tree index."""
    index = index_manager.create_index('idx_age', 'users', ['age'], 'btree')
    assert isinstance(index, BTreeIndex)
    assert index.table == 'users'
    assert index.columns == ['age']


def test_hash_index_insert_and_lookup(index_manager):
    """Test hash index insert and lookup."""
    index = index_manager.create_index('idx_name', 'users', ['name'], 'hash')
    
    # Insert rows
    index.insert('user1', {'name': 'Alice', 'age': 25})
    index.insert('user2', {'name': 'Bob', 'age': 30})
    index.insert('user3', {'name': 'Alice', 'age': 28})
    
    # Lookup
    results = index.lookup(name='Alice')
    assert len(results) == 2
    assert 'user1' in results
    assert 'user3' in results


def test_btree_index_insert_and_lookup(index_manager):
    """Test B-tree index insert and lookup."""
    index = index_manager.create_index('idx_age', 'users', ['age'], 'btree')
    
    # Insert rows
    index.insert('user1', {'name': 'Alice', 'age': 25})
    index.insert('user2', {'name': 'Bob', 'age': 30})
    index.insert('user3', {'name': 'Charlie', 'age': 25})
    
    # Exact lookup
    results = index.lookup(age=25)
    assert len(results) == 2
    assert 'user1' in results
    assert 'user3' in results


def test_btree_range_scan(index_manager):
    """Test B-tree range scan."""
    index = index_manager.create_index('idx_age', 'users', ['age'], 'btree')
    
    # Insert rows
    index.insert('user1', {'name': 'Alice', 'age': 25})
    index.insert('user2', {'name': 'Bob', 'age': 30})
    index.insert('user3', {'name': 'Charlie', 'age': 35})
    index.insert('user4', {'name': 'David', 'age': 40})
    
    # Range scan
    results = index.range_scan('age', 28, 36)
    assert len(results) == 2
    assert 'user2' in results  # age 30
    assert 'user3' in results  # age 35


def test_index_delete(index_manager):
    """Test deleting from index."""
    index = index_manager.create_index('idx_name', 'users', ['name'], 'hash')
    
    # Insert and delete
    index.insert('user1', {'name': 'Alice', 'age': 25})
    index.insert('user2', {'name': 'Bob', 'age': 30})
    
    results = index.lookup(name='Alice')
    assert len(results) == 1
    
    index.delete('user1', {'name': 'Alice', 'age': 25})
    
    results = index.lookup(name='Alice')
    assert len(results) == 0


def test_find_best_index(index_manager):
    """Test finding the best index for conditions."""
    index_name = index_manager.create_index('idx_name', 'users', ['name'], 'hash')
    index_age = index_manager.create_index('idx_age', 'users', ['age'], 'btree')
    
    # Should find name index
    best = index_manager.find_best_index('users', {'name': 'Alice'})
    assert best == index_name
    
    # Should find age index
    best = index_manager.find_best_index('users', {'age': 25})
    assert best == index_age
    
    # Should find any index when both match
    best = index_manager.find_best_index('users', {'name': 'Alice', 'age': 25})
    assert best in [index_name, index_age]


def test_index_manager_insert_row(index_manager):
    """Test IndexManager insert_row updates all indexes."""
    index_manager.create_index('idx_name', 'users', ['name'], 'hash')
    index_manager.create_index('idx_age', 'users', ['age'], 'btree')
    
    # Insert via manager
    index_manager.insert_row('users', 'user1', {'name': 'Alice', 'age': 25})
    
    # Verify both indexes updated
    name_idx = index_manager.get_index('users', 'idx_name')
    age_idx = index_manager.get_index('users', 'idx_age')
    
    assert 'user1' in name_idx.lookup(name='Alice')
    assert 'user1' in age_idx.lookup(age=25)


def test_drop_index(index_manager):
    """Test dropping an index."""
    index_manager.create_index('idx_test', 'users', ['name'], 'hash')
    assert index_manager.get_index('users', 'idx_test') is not None
    
    index_manager.drop_index('users', 'idx_test')
    assert index_manager.get_index('users', 'idx_test') is None
