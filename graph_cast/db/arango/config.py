from graph_cast.db.abstract_config import ConnectionConfig
from graph_cast.db.arango.connection import ArangoConnection


class ArangoConnectionConfig(ConnectionConfig):
    connection_class = ArangoConnection

    def __init__(self, **args):
        super().__init__(**args)
        self._init_values(**args)

    def _init_values(self, **config):
        super()._init_values(**config)
        self.port = config.get("port", 8529)
        self.hosts = f"{self.protocol}://{self.ip_addr}:{self.port}"
