import json
import logging
from collections import defaultdict

from arango import ArangoClient

from graph_cast.architecture import Configurator
from graph_cast.architecture.graph import GraphConfig
from graph_cast.architecture.schema import (
    SOURCE_AUX,
    TARGET_AUX,
    CollectionIndex,
    Edge,
    IndexType,
    VertexConfig,
)
from graph_cast.db import Connection
from graph_cast.db.onto import ArangoConnectionConfig
from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


class ArangoConnection(Connection):
    def __init__(self, config: ArangoConnectionConfig):
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

    def _add_index(self, general_collection, index: CollectionIndex):
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

    def define_vertex_indices(self, vertex_config: VertexConfig):
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

    def upsert_docs_batch(
        self,
        docs,
        collection_name,
        match_keys,
        update_keys=None,
        filter_uniques=True,
    ):
        """

        :param docs: list of dicts (json-like, ie keys are strings)
        :param collection_name: collection where to upsert
        :param match_keys: keys on which to look for document
        :param update_keys: keys which to update if doc in the collection, if update_keys='doc', update all
        :param filter_uniques:
        :return:
        """

        if isinstance(docs, list):
            if filter_uniques:
                docs = pick_unique_dict(docs)
            docs = json.dumps(docs)
        upsert_clause = ", ".join([f'"{k}": doc.{k}' for k in match_keys])
        upsert_clause = f"{{{upsert_clause}}}"

        if isinstance(update_keys, list):
            update_clause = ", ".join([f'"{k}": doc.{k}' for k in update_keys])
            update_clause = f"{{{update_clause}}}"
        elif update_keys == "doc":
            update_clause = "doc"
        else:
            update_clause = "{}"

        options = "OPTIONS {exclusive: true, ignoreErrors: true}"

        q_update = f"""FOR doc in {docs}
                            UPSERT {upsert_clause}
                            INSERT doc
                            UPDATE {update_clause} 
                                IN {collection_name} {options}"""
        return q_update

    def insert_edges_batch(
        self,
        docs_edges,
        source_class,
        target_class,
        relation_name,
        match_keys_source=("_key",),
        match_keys_target=("_key",),
        filter_uniques=True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option=False,
        head=None,
        **kwargs,
    ):
        f"""
            using ("_key",) for match_keys_source and match_keys_target saves time
                (no need to look it up from field discriminants)

        :param docs_edges: in format  [{{ _source_aux: source_doc, _target_aux: target_doc}}]
        :param source_class,
        :param target_class,
        :param relation_name:
        :param match_keys_source:
        :param match_keys_target:
        :param filter_uniques:
        :param uniq_weight_fields
        :param uniq_weight_collections
        :param upsert_option
        :param head: keep head docs

        :return:
        """

        if isinstance(docs_edges, list):
            if docs_edges:
                logger.debug(f" docs_edges[0] = {docs_edges[0]}")
            if head is not None:
                docs_edges = docs_edges[:head]
            if filter_uniques:
                docs_edges = pick_unique_dict(docs_edges)
            docs_edges_str = json.dumps(docs_edges)
        else:
            return ""

        if match_keys_source[0] == "_key":
            result_from = f'CONCAT("{source_class}/", edge.{SOURCE_AUX}._key)'
            source_filter = ""
        else:
            result_from = "sources[0]._id"
            filter_source = " && ".join(
                [f"v.{k} == edge.{SOURCE_AUX}.{k}" for k in match_keys_source]
            )
            source_filter = (
                f"LET sources = (FOR v IN {source_class} FILTER"
                f" {filter_source} LIMIT 1 RETURN v)"
            )

        if match_keys_target[0] == "_key":
            result_to = f'CONCAT("{target_class}/", edge.{TARGET_AUX}._key)'
            target_filter = ""
        else:
            result_to = "targets[0]._id"
            filter_target = " && ".join(
                [f"v.{k} == edge.{TARGET_AUX}.{k}" for k in match_keys_target]
            )
            target_filter = (
                f"LET targets = (FOR v IN {target_class} FILTER"
                f" {filter_target} LIMIT 1 RETURN v)"
            )

        doc_definition = (
            f"MERGE({{_from : {result_from}, _to : {result_to}}},"
            f" UNSET(edge, '{SOURCE_AUX}', '{TARGET_AUX}'))"
        )

        logger.debug(f" source_filter = {source_filter}")
        logger.debug(f" target_filter = {target_filter}")
        logger.debug(f" doc = {doc_definition}")

        if upsert_option:
            ups_from = result_from if source_filter else "doc._from"
            ups_to = result_to if target_filter else "doc._to"

            weight_fs = []
            weight_fs += (
                uniq_weight_fields if uniq_weight_fields is not None else []
            )
            weight_fs += (
                uniq_weight_collections
                if uniq_weight_collections is not None
                else []
            )
            if weight_fs:
                weights_clause = ", " + ", ".join(
                    [f"'{x}' : edge.{x}" for x in weight_fs]
                )
            else:
                weights_clause = ""

            upsert = (
                f"{{'_from': {ups_from}, '_to': {ups_to}"
                + weights_clause
                + "}"
            )
            logger.debug(f" upsert clause: {upsert}")
            clauses = f"UPSERT {upsert} INSERT doc UPDATE {{}}"
            options = "OPTIONS {exclusive: true}"
        else:
            clauses = "INSERT doc"
            options = "OPTIONS {exclusive: true, ignoreErrors: true}"

        q_update = f"""
            FOR edge in {docs_edges_str} {source_filter} {target_filter}
                LET doc = {doc_definition}
                {clauses}
                in {relation_name} {options}"""
        return q_update

    def insert_return_batch(self, docs, collection_name):
        docs = json.dumps(docs)
        query0 = f"""FOR doc in {docs}
              INSERT doc
              INTO {collection_name}
              LET inserted = NEW
              RETURN {{_key: inserted._key}}
        """
        return query0
