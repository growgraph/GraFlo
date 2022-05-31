import json
import os
from copy import deepcopy
from typing import Type
import yaml

from graph_cast.db.arango.config import ArangoConnectionConfig
from graph_cast.db.neo4j.config import Neo4jConnectionConfig
from graph_cast.db.abstract_config import ConnectionConfig


class ConfigFactory:
    _supported_dbs = ("arango", "neo4j")

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

        if db_type == "arango":
            return ArangoConnectionConfig(**config)
        elif db_type == "neo4j":
            return Neo4jConnectionConfig(**config)
        else:
            raise NotImplementedError


class ConnectionFactory:
    @classmethod
    def create_connection(cls, config: Type[ConnectionConfig]):
        return config.connection_class(config)
