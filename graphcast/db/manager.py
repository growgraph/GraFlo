from typing import Optional

from suthing import ConfigFactory, ConnectionKind, ProtoConnectionConfig

from graphcast.db.arango.conn import ArangoConnection
from graphcast.db.neo4j.conn import Neo4jConnection


class ConnectionManager:
    conn_class_mapping = {
        ConnectionKind.ARANGO: ArangoConnection,
        ConnectionKind.NEO4J: Neo4jConnection,
    }

    def __init__(
        self,
        secret_path=None,
        args=None,
        connection_config: Optional[ProtoConnectionConfig] = None,
        **kwargs,
    ):
        self.config: ProtoConnectionConfig = (
            ConfigFactory.create_config(secret_path, args)
            if connection_config is None
            else connection_config
        )
        self.working_db = kwargs.pop("working_db", None)
        self.conn = None

    def __enter__(self):
        cls = self.conn_class_mapping[self.config.connection_type]
        if self.working_db is not None:
            self.config.database = self.working_db
        self.conn = cls(config=self.config)
        return self.conn

    def close(self):
        self.conn.close()
        if self.config.connection_type == ConnectionKind.NEO4J:
            self.conn.conn.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
