import json
import os
from copy import deepcopy
from typing import Type
import yaml

from graph_cast.db.arango.config import ArangoConnectionConfig
from graph_cast.db.neo4j.config import Neo4jConnectionConfig
from graph_cast.db.abstract_config import ConnectionConfig, WSGIConfig


class ConfigFactory:
    _supported_dbs = ("arangos", "neo4js", "wsgi")

    @classmethod
    def create_config(cls, secret_path=None, args=None):
        if secret_path is not None:
            config_type = secret_path.split(".")[-1]
            if config_type not in ["json", "yaml"]:
                raise TypeError(
                    "Config file type not supported: should be json or yaml"
                )
            with open(os.path.expanduser(secret_path), "r") as f:
                if config_type == "json":
                    config = json.load(f)
                else:
                    config = yaml.load(f, Loader=yaml.FullLoader)
        elif args is not None and isinstance(args, dict):
            config = deepcopy(args)
        else:
            raise ValueError(
                "At least one of args should be non None : secret_path or args"
            )
        if "db_type" not in config:
            raise TypeError("db type not specified in secret")

        db_type = config["db_type"]
        if db_type not in cls._supported_dbs:
            raise TypeError(
                f"Config db_type not supported: should be {cls._supported_dbs}"
            )

        if db_type == "arangos":
            return ArangoConnectionConfig(**config)
        elif db_type == "neo4js":
            return Neo4jConnectionConfig(**config)
        elif db_type == "wsgi":
            return WSGIConfig(**config)
        else:
            raise NotImplementedError


class ConnectionFactory:
    @classmethod
    def create_connection(cls, config: Type[ConnectionConfig]):
        return config.connection_class(config)
