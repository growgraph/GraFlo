from graph_cast.db.abstract_config import ConnectionConfig
from graph_cast.db.neo4j.connection import Neo4jConnection


class Neo4jConnectionConfig(ConnectionConfig):
    connection_class = Neo4jConnection

    def __init__(self, **args):
        super().__init__(**args)
        self._init_values(**args)

    def _init_values(self, **config):
        super().__init__(**config)
        self.port = config.get("port", 7687)
        self.protocol = config.get("protocol", "bolt")
        self.hosts = f"{self.protocol}://{self.ip_addr}:{self.port}"
