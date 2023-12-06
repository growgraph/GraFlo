import dataclasses
from enum import Enum, EnumMeta

from dataclass_wizard import JSONWizard


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class ConnectionKind(str, Enum, metaclass=MetaEnum):
    ARANGO = "arango"
    NEO4J = "neo4j"
    WSGI = "wsgi"

    def config_class(self):
        mapping = {
            ConnectionKind.ARANGO: ArangoConnectionConfig,
            ConnectionKind.NEO4J: Neo4jConnectionConfig,
            ConnectionKind.WSGI: WSGIConfig,
        }
        return mapping[self]


@dataclasses.dataclass
class ProtoConnectionConfig(JSONWizard, JSONWizard.Meta):
    ip_addr: str | None = None
    port: str | None = None
    connection_type: ConnectionKind | None = None
    hosts: str | None = None
    protocol: str = "http"
    request_timeout: float = 60
    comment: str | None = None

    def __post_init__(self):
        if self.hosts is None:
            self.hosts = f"{self.protocol}://{self.ip_addr}:{self.port}"
        else:
            self._parse_hosts()

    def _parse_hosts(self):
        h = self.hosts

        h2 = h.split("://")
        self.protocol = h2[0]
        h3 = h2[1].split(":")
        self.ip_addr = h3[0]
        h4 = h3[1].split("/")
        self.port = h4[0]
        return h4


@dataclasses.dataclass(kw_only=True)
class DBConnectionConfig(ProtoConnectionConfig):
    cred_name: str
    cred_pass: str
    database: str | None = None

    def __post_init__(self):
        super().__post_init__()
        self.__identify_conn_type()

    def __identify_conn_type(self):
        """
        relies mostly on ports
        :return:
        """
        if self.connection_type is None:
            if 8500 < int(self.port) < 8600:
                self.connection_type = ConnectionKind.ARANGO
            elif 7400 < int(self.port) < 7700:
                self.connection_type = ConnectionKind.NEO4J


@dataclasses.dataclass
class WSGIConfig(ProtoConnectionConfig):
    path: str = "/"
    paths: dict = dataclasses.field(default_factory=lambda: {})

    def __post_init__(self):
        self.connection_type = ConnectionKind.WSGI
        if self.hosts is None:
            super().__post_init__()
            self.hosts += f"{self.path}"
        else:
            h = self._parse_hosts()
            self.__parse_path(h)

    def __parse_path(self, h):
        self.path = "/" + "/".join(h[1:])


@dataclasses.dataclass
class ArangoConnectionConfig(DBConnectionConfig):
    pass


@dataclasses.dataclass
class Neo4jConnectionConfig(DBConnectionConfig):
    pass
