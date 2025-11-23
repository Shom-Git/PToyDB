# Libraries and Frameworks Used in DistDB

This document details all the Python libraries and frameworks used in the DistDB distributed database implementation, along with the rationale for each choice.

## Core Dependencies

### 1. **sqlparse** (≥3.4.4)
- **Purpose**: SQL parsing and statement analysis
- **Usage**: Parses SQL queries into tokens for conversion to internal query representation
- **Rationale**: 
  - Well-tested, mature SQL parser
  - Handles complex SQL syntax without needing to write a parser from scratch
  - Extensible for future SQL enhancements
- **Where Used**: `sql_parser.py`

### 2. **grpcio** (≥1.59.0) and **grpcio-tools** (≥1.59.0)
- **Purpose**: RPC framework for distributed communication
- **Usage**: Designed for node-to-node communication in clusters
- **Rationale**:
  - Industry-standard RPC framework
  - High performance with HTTP/2
  - Built-in support for streaming and bidirectional communication
  - Protocol buffers for efficient serialization
- **Where Used**: Planned for `rpc_server.py` and `rpc_client.py` (foundation laid, not yet implemented)

### 3. **protobuf** (≥4.24.0)
- **Purpose**: Data serialization
- **Usage**: Define message formats for RPC communication
- **Rationale**:
  - Compact binary format
  - Language-agnostic schema
  - Faster than JSON for inter-node communication
- **Where Used**: Planned for `proto/database.proto`

### 4. **msgpack** (≥1.0.7)
- **Purpose**: Fast binary serialization
- **Usage**: Serialize data for WAL and internal storage
- **Rationale**:
  - Much faster than JSON or pickle
  - Compact binary format
  - Cross-language compatibility
  - No code execution risks (unlike pickle)
- **Where Used**: `utils.py`, `storage_engine.py`

### 5. **sortedcontainers** (≥2.4.0)
- **Purpose**: Sorted data structures
- **Usage**: B-tree index implementation
- **Rationale**:
  - Pure Python, no C dependencies
  - Highly optimized sorted list, dict, and set
  - O(log n) operations
  - Better performance than bisect module for large datasets
- **Where Used**: `index_manager.py`

## Testing Dependencies

### 6. **pytest** (≥7.4.0)
- **Purpose**: Testing framework
- **Usage**: All unit and integration tests
- **Rationale**:
  - Industry standard for Python testing
  - Rich plugin ecosystem
  - Better assertion introspection than unittest
  - Fixture support for test setup
- **Where Used**: `tests/` directory

### 7. **pytest-asyncio** (≥0.21.0)
- **Purpose**: Async test support
- **Usage**: Testing async operations
- **Rationale**:
  - Enables testing of async functions
  - Future-proofing for async network operations
- **Where Used**: Future async tests

### 8. **pytest-timeout** (≥2.2.0)
- **Purpose**: Test timeout handling
- **Usage**: Prevent hanging tests
- **Rationale**:
  - Critical for distributed system tests
  - Prevents infinite waits in network or consensus tests
  - Configurable timeouts per test
- **Where Used**: All tests (via pytest configuration)

### 9. **pytest-cov** (≥4.1.0)
- **Purpose**: Code coverage measurement
- **Usage**: Measure test coverage
- **Rationale**:
  - Integration with pytest
  - HTML and terminal reports
  - Identifies untested code paths
- **Where Used**: Test suite coverage analysis

## Optional Performance Dependencies

### 10. **uvloop** (≥0.19.0)
- **Purpose**: High-performance event loop
- **Usage**: Drop-in replacement for asyncio event loop
- **Rationale**:
  - 2-4x faster than default asyncio
  - Written in Cython
  - Zero-copy operations where possible
  - Future enhancement for async operations
- **Where Used**: Planned for future async network layer

## Standard Library Dependencies

The implementation also makes extensive use of Python's standard library:

- **threading**: Thread safety and concurrent operations
- **pathlib**: File system path handling
- **json**: Snapshot serialization
- **dataclasses**: Data structure definitions
- **typing**: Type hints for better code quality
- **logging**: Application logging
- **hashlib**: Consistent hashing
- **enum**: Enumerations (Raft states, etc.)
- **collections**: `defaultdict` and other utilities
- **bisect**: Binary search operations
- **re**: Regular expressions for SQL parsing
- **os**: Operating system operations
- **time**: Timestamps and timing
- **random**: Random number generation for Raft
- **tempfile**: Temporary directories in tests
- **shutil**: File operations in tests

## Why These Libraries?

### Design Principles

1. **Minimal Dependencies**: Only include what's necessary
2. **Pure Python First**: Prefer pure Python libraries when performance is acceptable
3. **Battle-Tested**: Use mature, well-maintained libraries
4. **Performance Where It Matters**: Use optimized libraries for hot paths (msgpack, sortedcontainers)
5. **Future-Proof**: Include foundations for planned features (grpc, uvloop)

### Trade-offs

- **No RocksDB/LMDB**: Started with simple file-based persistence for transparency and ease of understanding. Can be added later.
- **No NumPy/Pandas**: Not needed for this use case, would add unnecessary weight
- **gRPC over REST**: More efficient for internal cluster communication
- **msgpack over Protocol Buffers for storage**: Simpler API, good enough for internal storage

## Version Compatibility

All specified versions are minimum versions. The code should work with newer versions, but these have been tested:

- Python 3.8+ required
- All libraries use semantic versioning
- Pin exact versions in production for reproducibility

## Installation

All dependencies can be installed via pip:

```bash
pip install -r requirements.txt
```

For development (all dependencies including optional):

```bash
pip install sqlparse>=3.4.4 grpcio>=1.59.0 grpcio-tools>=1.59.0 \
    protobuf>=4.24.0 msgpack>=1.0.7 sortedcontainers>=2.4.0 \
    pytest>=7.4.0 pytest-asyncio>=0.21.0 pytest-timeout>=2.2.0 \
    pytest-cov>=4.1.0 uvloop>=0.19.0
```

## Future Dependencies

Planned but not yet required:

- **lmdb** or **rocksdb-python**: For production-grade persistence
- **prometheus-client**: For metrics and monitoring
- **rich**: For better CLI output
- **click**: For command-line interface
