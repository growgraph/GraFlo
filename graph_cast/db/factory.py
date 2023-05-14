from copy import deepcopy
from enum import Enum, EnumMeta

from suthing import FileHandle

from graph_cast.db.arango.config import ArangoConnectionConfig
from graph_cast.db.connection import ConnectionConfig, WSGIConfig
from graph_cast.db.neo4j.config import Neo4jConnectionConfig


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class ConnectionType(str, Enum, metaclass=MetaEnum):
    ARANGO = "arango"
    NEO4j = "neo4j"
    WSGI = "wsgi"


class ConfigFactory:
    @classmethod
    def create_config(cls, secret_path=None, args=None):
        if secret_path is not None:
            config = FileHandle.load(secret_path)
        elif args is not None and isinstance(args, dict):
            config = deepcopy(args)
        else:
            raise ValueError(
                "At least one of args should be non None : secret_path or args"
            )
        if "db_type" not in config:
            raise TypeError("db type not specified in secret")

        db_type = config["db_type"]
        if db_type not in ConnectionType:
            raise TypeError(
                f"Config db_type not supported: should be {ConnectionType}"
            )

        if db_type == ConnectionType.ARANGO:
            return ArangoConnectionConfig(**config)
        elif db_type == ConnectionType.NEO4j:
            return Neo4jConnectionConfig(**config)
        elif db_type == ConnectionType.WSGI:
            return WSGIConfig(**config)
        else:
            raise NotImplementedError
