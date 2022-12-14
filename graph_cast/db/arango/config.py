from graph_cast.db.arango.connection import ArangoConnection
from graph_cast.db.connection import ConnectionConfig


class ArangoConnectionConfig(ConnectionConfig):
    connection_class = ArangoConnection

    def __init__(self, **args):
        super().__init__(**args)
        self._init_values(**args)

    def _init_values(self, **config):
        super().__init__(**config)
        self.port = config.get("port", 8529)
        self.hosts = f"{self.protocol}://{self.ip_addr}:{self.port}"
        self.request_timeout = config.pop("request_timeout", 60)
