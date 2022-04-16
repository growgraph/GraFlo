import json
import yaml
import os
from copy import deepcopy
import logging
from arango import ArangoClient

logger = logging.getLogger(__name__)


class ConnectionConfig:
    _supported_dbs = ("arango", "neo4j")

    def __init__(self, secret_path=None, args=None):
        if secret_path is not None:
            self.config_type = secret_path.split(".")[-1]
            if self.config_type not in ["json", "yaml"]:
                raise TypeError("Config file type not supported: should be json or yaml")
            with open(os.path.expanduser(secret_path), "r") as f:
                if self.config_type == "json":
                    self.config = json.load(f)
                else:
                    self.config = yaml.load(f, Loader=yaml.FullLoader)
        elif args is not None and isinstance(args, dict):
            self.config = deepcopy(args)
        else:
            raise ValueError("At least one of args should be non None : secret_path or args")
        if "db_type" not in self.config:
            raise TypeError("db type not specified in secret")

        self.db_type = self.config["db_type"]

        if self.db_type not in self._supported_dbs:
            raise TypeError(
                f"Config db_type not supported: should be {self._supported_dbs}"
            )
        self._init_values()

    def _init_values(self):
        if self.db_type == "arango":
            self.protocol = self.config.get("protocol", "http")
            self.port = self.config.get("port", 8529)
            self.ip_addr = self.config.get("ip_addr")
            self.cred_name = self.config.get("cred_name")
            self.cred_pass = self.config.get("cred_pass")
            self.database = self.config.get("database")
            self.hosts = f"{self.protocol}://{self.ip_addr}:{self.port}"


class Connection:
    def __init__(self, config: ConnectionConfig):
        self.db_type = config.db_type
        if self.db_type == "arango":
            client = ArangoClient(hosts=config.hosts)

            self.conn = client.db(
                config.database, username=config.cred_name, password=config.cred_pass
            )
        elif config.db_type == "neo4j":
            pass

    def define_collections_and_indices(self, graph_config, vertex_config):
        if self.db_type == "arango":
            self.define_vertex_collections(graph_config, vertex_config.index)
            self.define_edge_collections(graph_config)

    def define_indices(self, graph_config, vertex_config):
        if self.db_type == "arango":
            self.define_vertex_indices(vertex_config)
            self.define_edge_indices(graph_config)

    def define_vertex_collections(self, graph_config, vertex_index):
        if self.db_type == "arango":
            edges = graph_config.all_edges
            for u, v in edges:
                item = graph_config.graph(u, v)
                gname = item["graph_name"]
                logger.info(f'{item["source"]}, {item["target"]}, {gname}')
                if self.conn.has_graph(gname):
                    g = self.conn.graph(gname)
                else:
                    g = self.conn.create_graph(gname)
                # TODO create collections without referencing the graph
                ih = self.create_collection_if_absent(
                    g,
                    item["source"],
                    vertex_index(u),
                )

                ih = self.create_collection_if_absent(
                    g,
                    item["target"],
                    vertex_index(v),
                )

    def define_edge_collections(self, graph_config):
        if self.db_type == "arango":
            edges = graph_config.all_edges
            for u, v in edges:
                item = graph_config.graph(u, v)
                gname = item["graph_name"]
                if self.conn.has_graph(gname):
                    g = self.conn.graph(gname)
                else:
                    g = self.conn.create_graph(gname)
                if not g.has_edge_definition(item["edge_name"]):
                    _ = g.create_edge_definition(
                        edge_collection=item["edge_name"],
                        from_vertex_collections=[item["source"]],
                        to_vertex_collections=[item["target"]],
                    )

    def define_vertex_indices(self, vertex_config):
        for c in vertex_config.collections:
            for index_dict in vertex_config.extra_index_list(c):
                general_collection = self.conn.collection(vertex_config.dbname(c))
                ih = general_collection.add_hash_index(
                    fields=index_dict["fields"], unique=index_dict["unique"]
                )

    def define_edge_indices(self, graph_config):
        for u, v in graph_config.all_edges:
            item = graph_config.graph(u, v)
            if "index" in item:
                for index_dict in item["index"]:
                    general_collection = self.conn.collection(item["edge_name"])
                    ih = general_collection.add_hash_index(
                        fields=index_dict["fields"], unique=index_dict["unique"]
                    )

    def create_collection_if_absent(self, g, vcol, index, unique=True):
        if not self.conn.has_collection(vcol):
            _ = g.create_vertex_collection(vcol)
            general_collection = self.conn.collection(vcol)
            if index is not None and index != ["_key"]:
                ih = general_collection.add_hash_index(fields=index, unique=unique)
                return ih
            else:
                return None

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        if self.db_type == "arango":
            logger.info("collections (non system):")
            logger.info([c for c in self.conn.collections() if c["name"][0] != "_"])

            if delete_all:
                cnames = [c["name"] for c in self.conn.collections() if c["name"][0] != "_"]
                gnames = [g["name"] for g in self.conn.graphs()]

            for cn in cnames:
                if self.conn.has_collection(cn):
                    self.conn.delete_collection(cn)

            logger.info("collections (after delete operation):")
            logger.info([c for c in self.conn.collections() if c["name"][0] != "_"])

            logger.info("graphs:")
            logger.info(self.conn.graphs())

            for gn in gnames:
                if self.conn.has_graph(gn):
                    self.conn.delete_graph(gn)

            logger.info("graphs (after delete operation):")
            logger.info(self.conn.graphs())
        else:
            pass

    def get_collections(self):
        return self.conn.collections()

    def execute(self, query):
        cursor = self.conn.aql.execute(query)
        return cursor

    def close(self):
        # self.conn.close()
        pass


class ConnectionManager:
    def __init__(self, secret_path=None, args=None):
        self.config = ConnectionConfig(secret_path, args)
        self.conn = None

    def __enter__(self):
        self.conn = Connection(self.config)
        return self.conn

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()

