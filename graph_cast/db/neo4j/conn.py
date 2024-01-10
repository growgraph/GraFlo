import logging

from neo4j import GraphDatabase
from suthing import Neo4jConnectionConfig

from graph_cast.architecture.edge import Edge
from graph_cast.architecture.onto import Index
from graph_cast.architecture.schema import Schema
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.db.connection import Connection
from graph_cast.filter.onto import Expression
from graph_cast.onto import AggregationType, DBFlavor

logger = logging.getLogger(__name__)


class Neo4jConnection(Connection):
    flavor = DBFlavor.NEO4J

    def __init__(self, config: Neo4jConnectionConfig):
        super().__init__()
        driver = GraphDatabase.driver(
            uri=config.hosts, auth=(config.cred_name, config.cred_pass)
        )
        self.conn = driver.session()

    def execute(self, query, **kwargs):
        cursor = self.conn.run(query, **kwargs)
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
        for c in vertex_config.vertex_set:
            for index_obj in vertex_config.indexes(c):
                self._add_index(c, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        for edge in edges:
            for index_obj in edge.indexes:
                self._add_index(
                    edge.relation, index_obj, is_vertex_index=False
                )

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        fields_str = ", ".join([f"x.{f}" for f in index.fields])
        fields_str2 = "_".join(index.fields)
        index_name = f"{obj_name}_{fields_str2}"
        if is_vertex_index:
            formula = f"(x:{obj_name})"
        else:
            formula = f"()-[x:{obj_name}]-()"

        q = (
            f"CREATE INDEX {index_name} IF NOT EXISTS FOR {formula} ON"
            f" ({fields_str});"
        )

        self.execute(q)

    def define_collections(self, schema: Schema):
        pass

    def define_vertex_collections(self, schema: Schema):
        pass

    def define_edge_collections(self, edges: list[Edge]):
        pass

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        if cnames:
            for c in cnames:
                q = f"MATCH (n:{c}) DELETE n"
                self.execute(q)
        else:
            q = f"MATCH (n) DELETE n"
            self.execute(q)

    def init_db(self, schema: Schema, clean_start):
        self.define_indexes(schema)

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """
            batch is sent in context
            {batch: [
            {id:"alice@example.com", name:"Alice",age:32},{id:"bob@example.com", name:"Bob", age:42}]}
            UNWIND $batch as row
            MERGE (n:Label {id: row.id})
            (ON CREATE) SET n += row

        :param docs: list of docs
        :param class_name:
        :param match_keys: dict of properties

        :return:
        """

        dry = kwargs.pop("dry", False)

        index_str = ", ".join([f"{k}: row.{k}" for k in match_keys])
        q = f"""
            WITH $batch AS batch 
            UNWIND batch as row 
            MERGE (n:{class_name} {{ {index_str} }}) 
            ON MATCH set n += row 
            ON CREATE set n += row
        """
        if not dry:
            self.execute(q, batch=docs)

    def insert_edges_batch(
        self,
        docs_edges,
        source_class,
        target_class,
        relation_name,
        collection_name=None,
        match_keys_source=("_key",),
        match_keys_target=("_key",),
        filter_uniques=True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option=False,
        head=None,
        **kwargs,
    ):
        dry = kwargs.pop("dry", False)

        source_match_str = [
            f"source.{key} = row['__source'].{key}"
            for key in match_keys_source
        ]
        target_match_str = [
            f"target.{key} = row['__target'].{key}"
            for key in match_keys_target
        ]

        match_clause = "WHERE " + " AND ".join(
            source_match_str + target_match_str
        )

        q = f"""
            WITH $batch AS batch 
            UNWIND batch as row 
            MATCH (source:{source_class}), 
                  (target:{target_class}) {match_clause} 
                        MERGE (source)-[r:{relation_name}]->(target)
        """
        if not dry:
            self.execute(q, batch=docs_edges)

    def insert_return_batch(self, docs, class_name):
        raise NotImplemented()

    def fetch_docs(
        self,
        class_name,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
    ):
        # "MATCH (d:chunks) WHERE d.t > 15 RETURN d { .kind, .t }"

        if filters is not None:
            ff = Expression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='n', kind=DBFlavor.NEO4J)}"
        else:
            filter_clause = ""

        if return_keys is not None:
            keep_clause_ = ", ".join([f".{item}" for item in return_keys])
            keep_clause = f"{{ {keep_clause_} }}"
        else:
            keep_clause = ""

        if limit is not None and isinstance(limit, int):
            limit_clause = f"LIMIT {limit}"
        else:
            limit_clause = ""

        q = (
            f"MATCH (n:{class_name})"
            f"  {filter_clause}"
            f"  RETURN n {keep_clause}"
            f"  {limit_clause}"
        )
        cursor = self.execute(q)
        r = [item["n"] for item in cursor.data()]
        return r

    def fetch_present_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        flatten=False,
        filters: list | dict | None = None,
    ):
        raise NotImplemented

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        raise NotImplemented

    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
        raise NotImplemented
