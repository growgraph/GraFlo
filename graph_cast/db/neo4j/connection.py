import logging

from neo4j import GraphDatabase

from graph_cast.architecture import Configurator
from graph_cast.architecture.schema import (
    CollectionIndex,
    IndexType,
    VertexConfig,
)
from graph_cast.db import Connection
from graph_cast.db.onto import Neo4jConnectionConfig

logger = logging.getLogger(__name__)


class Neo4jConnection(Connection):
    def __init__(self, config: Neo4jConnectionConfig):
        super().__init__()
        driver = GraphDatabase.driver(
            uri=config.hosts, auth=(config.cred_name, config.cred_pass)
        )
        self.conn = driver.session()

    def execute(self, query, params=None):
        cursor = self.conn.run(query, params)
        return cursor

    def close(self):
        self.conn.close()

    def create_database(self, name: str):
        """
        supported only in enterprise version

        :param name:
        :return:
        """
        try:
            self.execute(f"CREATE DATABASE {name}")
        except Exception as e:
            logger.error(f"{e}")

    def delete_database(self, name: str):
        """
        supported only in enterprise version
        :param name:
        :return:
        """
        try:
            self.execute(f"DROP DATABASE {name}")
        except Exception as e:
            logger.error(f"{e}")

    def define_vertex_indices(self, vertex_config: VertexConfig):
        for c in vertex_config.collections:
            for index_obj in vertex_config.extra_index_list(c):
                self._add_index(c, index_obj)

    def _add_index(self, vertex_name, index: CollectionIndex):
        for f in index.fields:
            q = f"CREATE INDEX ON :{vertex_name}({f})"
            self.execute(q)
        if len(index.fields) > 1:
            fields_str = ", ".join(index.fields)
            q = f"CREATE INDEX ON :{vertex_name}({fields_str})"
            self.execute(q)

    def define_collections(self, graph_config, vertex_config: VertexConfig):
        pass
        # self.define_vertex_collections(graph_config, vertex_config)
        # self.define_edge_collections(graph_config)

    def define_indices(self, graph_config, vertex_config: VertexConfig):
        self.define_vertex_indices(vertex_config)
        # self.define_edge_indices(graph_config)

    def define_vertex_collections(self, graph_config, vertex_config):
        pass

    def define_edge_collections(self, graph_config):
        pass

    def define_edge_indices(self, graph_config):
        pass

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        # for relation in gnames:
        #     q = f"match (a) -[r] -> () delete r"
        #     self.execute(q)
        for c in cnames:
            q = f"MATCH (a:{c}) DELETE a"
            self.execute(q)

    def init_db(self, conf_obj: Configurator, clean_start):
        self.define_indices(conf_obj.graph_config, conf_obj.vertex_config)
