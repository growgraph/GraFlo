from suthing import (
    ArangoConnectionConfig,
    ConfigFactory,
    DBConnectionConfig,
    Neo4jConnectionConfig,
    WSGIConfig,
)

from .connection import Connection, ConnectionType
from .manager import ConnectionManager
