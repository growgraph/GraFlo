from typing import Optional
from graph_cast.db import ConfigFactory
from graph_cast.db.abstract_config import ConnectionConfigType
from graph_cast.db.factory import ConnectionFactory


class ConnectionManager:
    def __init__(
        self,
        secret_path=None,
        args=None,
        connection_config: Optional[ConnectionConfigType] = None,
    ):
        if connection_config is None:
            self.config: ConnectionConfigType = ConfigFactory.create_config(
                secret_path, args
            )
        else:
            self.config: ConnectionConfigType = connection_config
        self.conn = None

    def __enter__(self):
        self.conn = ConnectionFactory.create_connection(self.config)
        return self.conn

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()
