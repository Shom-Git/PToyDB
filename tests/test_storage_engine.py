"""Tests for storage engine."""

import pytest
from distdb.storage_engine import StorageEngine


def test_create_table(storage_engine):
    """Test creating a table."""
    schema = {'id': 'INTEGER', 'name': 'TEXT', 'age': 'INTEGER'}
    storage_engine.create_table('users', schema)
    
    assert 'users' in storage_engine.list_tables()
    assert storage_engine.get_schema('users') == schema


def test_create_duplicate_table(storage_engine):
    """Test creating duplicate table raises error."""
    schema = {'id': 'INTEGER'}
    storage_engine.create_table('test', schema)
    
    with pytest.raises(ValueError):
        storage_engine.create_table('test', schema)


def test_put_and_get(storage_engine):
    """Test storing and retrieving data."""
    schema = {'id': 'INTEGER', 'name': 'TEXT'}
    storage_engine.create_table('users', schema)
    
    storage_engine.put('users', 'user1', {'id': 1, 'name': 'Alice'})
    
    result = storage_engine.get('users', 'user1')
    assert result == {'id': 1, 'name': 'Alice'}


def test_put_invalid_column(storage_engine):
    """Test putting data with invalid column."""
    schema = {'id': 'INTEGER', 'name': 'TEXT'}
    storage_engine.create_table('users', schema)
    
    with pytest.raises(ValueError):
        storage_engine.put('users', 'user1', {'id': 1, 'invalid': 'value'})


def test_delete(storage_engine):
    """Test deleting data."""
    schema = {'id': 'INTEGER'}
    storage_engine.create_table('test', schema)
    
    storage_engine.put('test', 'key1', {'id': 1})
    assert storage_engine.get('test', 'key1') is not None
    
    storage_engine.delete('test', 'key1')
    assert storage_engine.get('test', 'key1') is None


def test_scan(storage_engine):
    """Test scanning a table."""
    schema = {'id': 'INTEGER', 'name': 'TEXT'}
    storage_engine.create_table('users', schema)
    
    storage_engine.put('users', 'user1', {'id': 1, 'name': 'Alice'})
    storage_engine.put('users', 'user2', {'id': 2, 'name': 'Bob'})
    storage_engine.put('users', 'user3', {'id': 3, 'name': 'Charlie'})
    
    results = storage_engine.scan('users')
    assert len(results) == 3


def test_snapshot_and_recovery(test_config):
    """Test snapshot creation and recovery."""
    # Create engine and add data
    engine1 = StorageEngine(
        test_config.data_dir,
        test_config.wal_dir,
        snapshot_interval=2
    )
    
    schema = {'id': 'INTEGER', 'value': 'TEXT'}
    engine1.create_table('test', schema)
    engine1.put('test', 'key1', {'id': 1, 'value': 'one'})
    engine1.put('test', 'key2', {'id': 2, 'value': 'two'})
    
    # Force snapshot
    engine1.snapshot()
    engine1.close()
    
    # Create new engine and verify recovery
    engine2 = StorageEngine(
        test_config.data_dir,
        test_config.wal_dir,
        snapshot_interval=2
    )
    
    assert 'test' in engine2.list_tables()
    assert engine2.get('test', 'key1') == {'id': 1, 'value': 'one'}
    assert engine2.get('test', 'key2') == {'id': 2, 'value': 'two'}
    
    engine2.close()


def test_wal_recovery(test_config):
    """Test WAL recovery without snapshot."""
    # Create engine and add data
    engine1 = StorageEngine(
        test_config.data_dir,
        test_config.wal_dir,
        snapshot_interval=1000  # Large interval so no snapshot
    )
    
    schema = {'id': 'INTEGER'}
    engine1.create_table('test', schema)
    engine1.put('test', 'key1', {'id': 1})
    engine1.close()
    
    # Create new engine and verify WAL recovery
    engine2 = StorageEngine(
        test_config.data_dir,
        test_config.wal_dir,
        snapshot_interval=1000
    )
    
    assert 'test' in engine2.list_tables()
    assert engine2.get('test', 'key1') == {'id': 1}
    
    engine2.close()


def test_drop_table(storage_engine):
    """Test dropping a table."""
    schema = {'id': 'INTEGER'}
    storage_engine.create_table('test', schema)
    assert 'test' in storage_engine.list_tables()
    
    storage_engine.drop_table('test')
    assert 'test' not in storage_engine.list_tables()
