from typing import Optional

from graph_cast.db import ConfigFactory, ConnectionConfigType


class ConnectionManager:
    def __init__(
        self,
        secret_path=None,
        args=None,
        connection_config: Optional[ConnectionConfigType] = None,
        **kwargs
    ):
        self.config: ConnectionConfigType = (
            ConfigFactory.create_config(secret_path, args)
            if connection_config is None
            else connection_config
        )
        self.working_db = kwargs.pop("working_db", None)
        self.conn = None

    def __enter__(self):
        cls = self.config.connection_class
        if self.working_db is not None:
            self.config.database = self.working_db
        self.conn = cls(config=self.config)
        return self.conn

    def close(self):
        self.conn.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
