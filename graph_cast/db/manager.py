from typing import Optional

from graph_cast.db import ConfigFactory, ConnectionConfigType


class ConnectionManager:
    def __init__(
        self,
        secret_path=None,
        args=None,
        connection_config: Optional[ConnectionConfigType] = None,
    ):
        self.config: ConnectionConfigType = (
            ConfigFactory.create_config(secret_path, args)
            if connection_config is None
            else connection_config
        )
        self.conn = None

    def __enter__(self):
        cls = self.config.connection_class
        self.conn = cls(self.config)
        return self.conn

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()
