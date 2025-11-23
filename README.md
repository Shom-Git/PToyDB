# DistDB - Scalable Distributed Database

A scalable distributed key-value database with SQL query support, built from scratch in Python.

## Features

- **SQL Interface**: Support for CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
- **Indexing**: Hash and B-tree indexes for fast lookups and range queries
- **Durability**: Write-Ahead Log (WAL) and snapshots for crash recovery
- **Distribution**: Consistent hashing for data distribution across nodes
- **Consistency**: Raft-based consensus protocol for strong consistency
- **High Availability**: Configurable replication factor

## Installation

```bash
# Clone the repository
cd /home/aslan/Desktop/vibe

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

### Basic Usage

```python
from distdb import Client

# Create a client (starts a local node)
client = Client()

# Create a table
client.create_table('users', {
    'id': 'INTEGER',
    'name': 'TEXT',
    'email': 'TEXT',
    'age': 'INTEGER'
})

# Insert data
client.insert('users', {
    'id': 1,
    'name': 'Alice',
    'email': 'alice@example.com',
    'age': 25
})

# Query data
users = client.select('users', where={'age': 25})
print(users)

# Create an index for faster queries
client.create_index('idx_age', 'users', ['age'], 'btree')

# Update data
client.update('users', {'age': 26}, where={'id': 1})

# Delete data
client.delete('users', where={'id': 1})

# Close the client
client.close()
```

### Using SQL Directly

```python
from distdb import Client

client = Client()

# Execute SQL queries
result = client.execute("CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)")
result = client.execute("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)")

# Query with SQL
rows = client.query("SELECT * FROM products WHERE price > 50 ORDER BY price DESC")

client.close()
```

## Architecture

### Components

1. **Storage Engine** (`storage_engine.py`)
   - In-memory key-value store with multi-table support
   - Write-Ahead Log for durability
   - Snapshot and recovery mechanisms

2. **Index Manager** (`index_manager.py`)
   - Hash indexes for exact-match lookups
   - B-tree indexes for range queries
   - Automatic index selection for query optimization

3. **SQL Parser** (`sql_parser.py`)
   - Converts SQL statements to internal query representation
   - Supports CREATE, INSERT, SELECT, UPDATE, DELETE
   - WHERE, ORDER BY, and LIMIT clauses

4. **Query Executor** (`query_executor.py`)
   - Executes parsed queries against storage engine
   - Leverages indexes for performance
   - Transaction support

5. **Shard Manager** (`shard_manager.py`)
   - Consistent hashing for data distribution
   - Configurable replication factor
   - Automatic rebalancing

6. **Replication** (`replication.py`)
   - Simplified Raft consensus implementation
   - Leader election and failover
   - Log replication for consistency

7. **Cluster Manager** (`cluster_manager.py`)
   - Node discovery and membership
   - Health monitoring
   - Failure detection

8. **Node** (`node.py`)
   - Main coordinator for all components
   - Handles query routing
   - Manages distributed operations

9. **Client** (`client.py`)
   - High-level Python API
   - Connection management
   - Query execution

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_storage_engine.py -v

# Run with coverage
pytest tests/ --cov=distdb --cov-report=html
```

## Configuration

Configure the database using environment variables or the `Config` class:

```python
from distdb import Config, Node

config = Config()
config.node_id = "node1"
config.host = "localhost"
config.port = 5000
config.replication_factor = 3
config.data_dir = "./data"
config.wal_dir = "./wal"

node = Node(config)
node.start()
```

### Environment Variables

- `DISTDB_NODE_ID`: Node identifier
- `DISTDB_HOST`: Host address
- `DISTDB_PORT`: Port number
- `DISTDB_CLUSTER_NODES`: Comma-separated list of cluster nodes (format: `node_id@host:port`)
- `DISTDB_REPLICATION_FACTOR`: Number of replicas
- `DISTDB_DATA_DIR`: Data directory path
- `DISTDB_WAL_DIR`: WAL directory path

## Supported SQL Features

### Data Definition Language (DDL)
- `CREATE TABLE table_name (column1 TYPE1, column2 TYPE2, ...)`
- `DROP TABLE table_name`
- `CREATE INDEX index_name ON table_name (column) [USING HASH|BTREE]`
- `DROP INDEX index_name ON table_name`

### Data Manipulation Language (DML)
- `INSERT INTO table_name (col1, col2) VALUES (val1, val2)`
- `SELECT * FROM table_name [WHERE conditions] [ORDER BY column [ASC|DESC]] [LIMIT n]`
- `UPDATE table_name SET col1 = val1 [WHERE conditions]`
- `DELETE FROM table_name [WHERE conditions]`

### Supported Data Types
- `INTEGER`: Integer numbers
- `TEXT`: String values
- More types can be added easily

## Performance

### Index Performance
- Hash indexes: O(1) lookup time
- B-tree indexes: O(log n) lookup and range scan
- Automatic index selection for optimal query performance

### Scalability
- Horizontal scaling through consistent hashing
- Configurable replication for fault tolerance
- Leader-based writes ensure consistency

## Limitations

- **SQL Subset**: No JOINs, subqueries, or complex aggregations (yet)
- **Single-Node Testing**: Full cluster testing requires gRPC implementation
- **In-Memory**: Data stored in memory (with persistence via snapshots)
- **Network**: gRPC communication layer is planned but not yet implemented for multi-node

## Future Enhancements

- Full gRPC-based network communication
- JOIN support in SQL
- Advanced aggregations (GROUP BY, HAVING)
- Secondary indexes
- Query planner and optimizer
- Compression and compaction
- Read replicas
- Async replication mode

## License

MIT

## Contributing

Contributions are welcome! Please submit issues and pull requests.
