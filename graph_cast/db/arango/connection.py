import logging

from arango import ArangoClient

from graph_cast.architecture.schema import (
    CollectionIndex,
    GraphConfig,
    VertexConfig,
)
from graph_cast.db import ConnectionConfigType
from graph_cast.db.connection import Connection

logger = logging.getLogger(__name__)


class ArangoConnection(Connection):
    def __init__(self, config: ConnectionConfigType):
        super().__init__()
        client = ArangoClient(hosts=config.hosts)

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
        for u, v in graph_config.all_edges:
            item = graph_config.graph(u, v)
            gname = item.graph_name
            logger.info(f"{item.source}, {item.target}, {gname}")
            if self.conn.has_graph(gname):
                g = self.conn.graph(gname)
            else:
                g = self.conn.create_graph(gname)

            # TODO create collections without referencing the graph
            ih = self.create_collection_if_absent(
                g,
                vertex_config.vertex_dbname(u),
                vertex_config.index(u),
            )

            ih = self.create_collection_if_absent(
                g,
                vertex_config.vertex_dbname(v),
                vertex_config.index(v),
            )
        for v in disconnected_vertex_collections:
            dbc = self.conn.create_collection(vertex_config.vertex_dbname(v))
            ih = dbc.add_hash_index(fields=vertex_config.index(v).fields)

    def define_edge_collections(self, graph_config: GraphConfig):
        edges = graph_config.all_edges
        for u, v in edges:
            item = graph_config.graph(u, v)
            gname = item.graph_name
            if self.conn.has_graph(gname):
                g = self.conn.graph(gname)
            else:
                g = self.conn.create_graph(gname)
            if not g.has_edge_definition(item.edge_name):
                _ = g.create_edge_definition(
                    edge_collection=item.edge_name,
                    from_vertex_collections=[
                        graph_config.graph(u, v).source_collection
                    ],
                    to_vertex_collections=[
                        graph_config.graph(u, v).target_collection
                    ],
                )

    from graph_cast.architecture.schema import CollectionIndex

    @staticmethod
    def _add_index(general_collection, index: CollectionIndex):
        data = index.to_dict()
        if index.type == "persistent":
            data = index.to_dict()
            ih = general_collection._add_index(data)
            # temp fix : inconsistentcy in python-arango
        if index.type == "hash":
            # ih = general_collection.add_hash_index(
            #     **data
            # )
            data = index.to_dict()
            ih = general_collection._add_index(data)
        elif index.type == "skiplist":
            ih = general_collection.add_skiplist_index(
                fields=index.fields, unique=index.unique
            )
        elif index.type == "fulltext":
            ih = general_collection.add_fulltext_index(
                fields=index.fields, unique=index.unique
            )
        else:
            ih = None
        return ih

    def define_vertex_indices(self, vertex_config):
        for c in vertex_config.collections:
            for index in vertex_config.extra_index_list(c):
                general_collection = self.conn.collection(
                    vertex_config.vertex_dbname(c)
                )
                self._add_index(general_collection, index)

    def define_edge_indices(self, graph_config: GraphConfig):
        for u, v in graph_config.all_edges:
            item = graph_config.graph(u, v)
            general_collection = self.conn.collection(item.edge_name)
            for index_dict in item.index:
                self._add_index(general_collection, index_dict)

    def create_collection_if_absent(self, g, vcol, index: CollectionIndex):
        if not self.conn.has_collection(vcol):
            _ = g.create_vertex_collection(vcol)
            general_collection = self.conn.collection(vcol)
            if index is not None and index.fields != ["_key"]:
                ih = general_collection.add_hash_index(
                    fields=index.fields, unique=index.unique
                )
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
