import abc
from typing import TypeVar


ConnectionConfigType = TypeVar("ConnectionConfigType", bound="ConnectionConfig")


class ConnectionConfig(abc.ABC):
    connection_class = None

    def __init__(self, **config):
        self.protocol = config.get("protocol", "http")
        self.ip_addr = config.get("ip_addr", None)
        self.cred_name = config.get("cred_name", None)
        self.cred_pass = config.get("cred_pass", None)
        self.database = config.get("database", None)
        self.port = config.get("port", None)
        self.hosts = None


class WSGIConfig(ConnectionConfig):
    connection_class = None

    def __init__(self, **config):
        super(WSGIConfig, self).__init__(**config)
        self.path = config.get("path", "/")
        self.hosts = f"{self.protocol}://{self.ip_addr}:{self.port}{self.path}"
        self.host = config.get("host", None)


