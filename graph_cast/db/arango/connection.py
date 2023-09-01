import logging

from arango import ArangoClient

from graph_cast.architecture.general import Configurator
from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.schema import (
    CollectionIndex,
    IndexType,
    VertexConfig,
)
from graph_cast.db import ConnectionConfigType
from graph_cast.db.connection import Connection

logger = logging.getLogger(__name__)


class ArangoConnection(Connection):
    def __init__(self, config: ConnectionConfigType):
        super().__init__()
        client = ArangoClient(
            hosts=config.hosts, request_timeout=config.request_timeout
        )

        self.conn = client.db(
            config.database,
            username=config.cred_name,
            password=config.cred_pass,
        )

    def create_database(self, name: str):
        if not self.conn.has_database(name):
            self.conn.create_database(name)

    def delete_database(self, name: str):
        if not self.conn.has_database(name):
            self.conn.delete_database(name)

    def init_db(self, conf_obj: Configurator, clean_start):
        if clean_start:
            self.delete_collections([], [], delete_all=True)
            #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
            # elif clean_start == "edges":
            #     delete_collections(sys_db, ecollections, [])
        self.define_collections(conf_obj.graph_config, conf_obj.vertex_config)
        self.define_indices(conf_obj.graph_config, conf_obj.vertex_config)

    def define_collections(self, graph_config, vertex_config: VertexConfig):
        self.define_vertex_collections(graph_config, vertex_config)
        self.define_edge_collections(graph_config)

    def define_indices(self, graph_config, vertex_config: VertexConfig):
        self.define_vertex_indices(vertex_config)
        self.define_edge_indices(graph_config)

    def define_vertex_collections(
        self, graph_config: GraphConfig, vertex_config: VertexConfig
    ):
        disconnected_vertex_collections = set(vertex_config.collections) - set(
            [v for edge in graph_config.all_edges for v in edge]
        )
        es = list(graph_config.all_edge_definitions())
        for item in es:
            u, v = item.source, item.target
            gname = item.graph_name
            logger.info(f"{item.source}, {item.target}, {gname}")
            if self.conn.has_graph(gname):
                g = self.conn.graph(gname)
            else:
                g = self.conn.create_graph(gname)  # type: ignore

            # TODO create collections without referencing the graph
            ih = self.create_collection(
                vertex_config.vertex_dbname(u), vertex_config.index(u), g
            )

            ih = self.create_collection(
                vertex_config.vertex_dbname(v), vertex_config.index(v), g
            )
        for v in disconnected_vertex_collections:
            ih = self.create_collection(
                vertex_config.vertex_dbname(v), vertex_config.index(v), None
            )

    def define_edge_collections(self, graph_config: GraphConfig):
        es = list(graph_config.all_edge_definitions())
        for item in es:
            gname = item.graph_name
            if self.conn.has_graph(gname):
                g = self.conn.graph(gname)
            else:
                g = self.conn.create_graph(gname)  # type: ignore
            if not g.has_edge_definition(item.edge_name):
                _ = g.create_edge_definition(
                    edge_collection=item.edge_name,
                    from_vertex_collections=[item.source_collection],
                    to_vertex_collections=[item.target_collection],
                )

    @staticmethod
    def _add_index(general_collection, index: CollectionIndex):
        data = index.to_dict()
        # in CollectionIndex "name" is used for vertex collection derived index field
        # to let arango name her index, we remove "name"
        data.pop("name")
        if index.type == IndexType.PERSISTENT:
            # temp fix : inconsistentcy in python-arango
            ih = general_collection._add_index(data)
        if index.type == IndexType.HASH:
            ih = general_collection._add_index(data)
        elif index.type == IndexType.SKIPLIST:
            ih = general_collection.add_skiplist_index(
                fields=index.fields, unique=index.unique
            )
        elif index.type == IndexType.FULLTEXT:
            ih = general_collection.add_fulltext_index(
                fields=index.fields, unique=index.unique
            )
        else:
            ih = None
        return ih

    def define_vertex_indices(self, vertex_config):
        for c in vertex_config.collections:
            for index_obj in vertex_config.extra_index_list(c):
                general_collection = self.conn.collection(
                    vertex_config.vertex_dbname(c)
                )
                self._add_index(general_collection, index_obj)

    def define_edge_indices(self, graph_config: GraphConfig):
        for item in graph_config.all_edge_definitions():
            general_collection = self.conn.collection(item.edge_name)
            for index_obj in item.indices:
                self._add_index(general_collection, index_obj)

    def create_collection(
        self, collection_name, index: CollectionIndex | None = None, g=None
    ):
        if not self.conn.has_collection(collection_name):
            if g is not None:
                _ = g.create_vertex_collection(collection_name)
            else:
                self.conn.create_collection(collection_name)
            general_collection = self.conn.collection(collection_name)
            if index is not None and index.fields != ["_key"]:
                ih = self._add_index(general_collection, index)
                return ih
            else:
                return None

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        logger.info("collections (non system):")
        logger.info(
            [c for c in self.conn.collections() if c["name"][0] != "_"]
        )

        if delete_all:
            cnames = [
                c["name"]
                for c in self.conn.collections()
                if c["name"][0] != "_"
            ]
            gnames = [g["name"] for g in self.conn.graphs()]

        for cn in cnames:
            if self.conn.has_collection(cn):
                self.conn.delete_collection(cn)

        logger.info("collections (after delete operation):")
        logger.info(
            [c for c in self.conn.collections() if c["name"][0] != "_"]
        )

        logger.info("graphs:")
        logger.info(self.conn.graphs())

        for gn in gnames:
            if self.conn.has_graph(gn):
                self.conn.delete_graph(gn)

        logger.info("graphs (after delete operation):")
        logger.info(self.conn.graphs())

    def get_collections(self):
        return self.conn.collections()

    def execute(self, query):
        cursor = self.conn.aql.execute(query)
        return cursor

    def close(self):
        # self.conn.close()
        pass
