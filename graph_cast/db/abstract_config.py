import abc
from typing import TypeVar


ConnectionConfigType = TypeVar("ConnectionConfigType", bound="ConnectionConfig")


class ConnectionConfig(abc.ABC):
    connection_class = None

    def __init__(self, **args):
        self.protocol = None
        self.ip_addr = None
        self.cred_name = None
        self.cred_pass = None
        self.database = None
        self.hosts = None
        self.port = None

    @abc.abstractmethod
    def _init_values(self, **config):
        self.protocol = config.get("protocol", "http")
        self.ip_addr = config.get("ip_addr")
        self.cred_name = config.get("cred_name")
        self.cred_pass = config.get("cred_pass")
        self.database = config.get("database")
