from graph_cast.db import ConfigFactory
from graph_cast.db.factory import ConnectionFactory


class ConnectionManager:
    def __init__(self, secret_path=None, args=None):
        self.config = ConfigFactory.create_config(secret_path, args)
        self.conn = None

    def __enter__(self):
        self.conn = ConnectionFactory.create_connection(self.config)
        return self.conn

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()
