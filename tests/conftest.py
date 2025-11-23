"""Pytest configuration and fixtures."""

import pytest
import tempfile
import shutil
from pathlib import Path

from distdb.config import Config
from distdb.storage_engine import StorageEngine
from distdb.index_manager import IndexManager
from distdb.node import Node
from distdb.client import Client


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp = tempfile.mkdtemp()
    yield temp
    shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def test_config(temp_dir):
    """Create a test configuration."""
    config = Config()
    config.node_id = "test_node"
    config.data_dir = str(Path(temp_dir) / "data")
    config.wal_dir = str(Path(temp_dir) / "wal")
    config.snapshot_interval = 10
    return config


@pytest.fixture
def storage_engine(test_config):
    """Create a storage engine for tests."""
    engine = StorageEngine(
        test_config.data_dir,
        test_config.wal_dir,
        test_config.snapshot_interval
    )
    yield engine
    engine.close()


@pytest.fixture
def index_manager():
    """Create an index manager for tests."""
    return IndexManager()


@pytest.fixture
def node(test_config):
    """Create a node for tests."""
    node = Node(test_config)
    node.start()
    yield node
    node.stop()


@pytest.fixture
def client(test_config):
    """Create a client for tests."""
    client = Client()
    yield client
    client.close()
