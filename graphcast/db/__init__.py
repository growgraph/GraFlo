"""Database connection and management components.

This package provides database connection implementations and management utilities
for different graph databases (ArangoDB, Neo4j). It includes connection interfaces,
query execution, and database operations.

Key Components:
    - Connection: Abstract database connection interface
    - ConnectionManager: Database connection management
    - ArangoDB: ArangoDB-specific implementation
    - Neo4j: Neo4j-specific implementation
    - Query: Query generation and execution utilities

Example:
    >>> from graphcast.db import ConnectionManager
    >>> from graphcast.db.arango import ArangoConnection
    >>> manager = ConnectionManager(
    ...     connection_config={"url": "http://localhost:8529"},
    ...     conn_class=ArangoConnection
    ... )
    >>> with manager as conn:
    ...     conn.init_db(schema)
"""

from .connection import Connection, ConnectionType
from .manager import ConnectionManager

__all__ = [
    "Connection",
    "ConnectionManager",
    "ConnectionType",
]
