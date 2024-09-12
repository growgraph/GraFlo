import json
import logging

from arango import ArangoClient
from suthing import ArangoConnectionConfig

from graph_cast.architecture.edge import Edge
from graph_cast.architecture.onto import (
    SOURCE_AUX,
    TARGET_AUX,
    Index,
    IndexType,
)
from graph_cast.architecture.schema import Schema
from graph_cast.architecture.vertex import VertexConfig
from graph_cast.db.arango.query import fetch_fields_query
from graph_cast.db.arango.util import render_filters
from graph_cast.db.connection import Connection
from graph_cast.db.util import get_data_from_cursor
from graph_cast.filter.onto import Clause
from graph_cast.onto import AggregationType, DBFlavor
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

    def execute(self, query, **kwargs):
        cursor = self.conn.aql.execute(query)
        return cursor

    def close(self):
        # self.conn.close()
        pass

    def init_db(self, schema: Schema, clean_start):
        if clean_start:
            self.delete_collections([], [], delete_all=True)
            #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
            # elif clean_start == "edges":
            #     delete_collections(sys_db, ecollections, [])
        self.define_collections(schema)
        self.define_indexes(schema)

    def define_collections(self, schema: Schema):
        self.define_vertex_collections(schema)
        self.define_edge_collections(schema.edge_config.edges)

    def define_vertex_collections(self, schema: Schema):
        vertex_config = schema.vertex_config
        disconnected_vertex_collections = (
            set(vertex_config.vertex_set) - schema.edge_config.vertices
        )
        for item in schema.edge_config.edges:
            u, v = item.source, item.target
            gname = item.graph_name
            logger.info(f"{item.source}, {item.target}, {gname}")
            if self.conn.has_graph(gname):
                g = self.conn.graph(gname)
            else:
                g = self.conn.create_graph(gname)  # type: ignore

            _ = self.create_collection(
                vertex_config.vertex_dbname(u), vertex_config.index(u), g
            )

            _ = self.create_collection(
                vertex_config.vertex_dbname(v), vertex_config.index(v), g
            )
        for v in disconnected_vertex_collections:
            _ = self.create_collection(
                vertex_config.vertex_dbname(v), vertex_config.index(v), None
            )

    def define_edge_collections(self, edges: list[Edge]):
        for item in edges:
            gname = item.graph_name
            if self.conn.has_graph(gname):
                g = self.conn.graph(gname)
            else:
                g = self.conn.create_graph(gname)  # type: ignore
            if not g.has_edge_definition(item.collection_name):
                _ = g.create_edge_definition(
                    edge_collection=item.collection_name,
                    from_vertex_collections=[item.source_collection],
                    to_vertex_collections=[item.target_collection],
                )

    def _add_index(self, general_collection, index: Index):
        data = index.db_form(DBFlavor.ARANGO)
        # in Index "name" is used for vertex collection derived index field
        # to let arango name her index, we remove "name"
        if index.type == IndexType.PERSISTENT:
            # temp fix : inconsistent in python-arango
            ih = general_collection._add_index(data)
        if index.type == IndexType.HASH:
            ih = general_collection._add_index(data)
        elif index.type == IndexType.SKIPLIST:
            ih = general_collection.add_skiplist_index(
                fields=index.fields, unique=index.unique
            )
        elif index.type == IndexType.FULLTEXT:
            ih = general_collection.add_fulltext_index(fields=index.fields)
        else:
            ih = None
        return ih

    def define_vertex_indices(self, vertex_config: VertexConfig):
        for c in vertex_config.vertex_set:
            general_collection = self.conn.collection(vertex_config.vertex_dbname(c))
            ixs = general_collection.indexes()
            field_combinations = [tuple(ix["fields"]) for ix in ixs]
            for index_obj in vertex_config.indexes(c):
                if tuple(index_obj.fields) not in field_combinations:
                    self._add_index(general_collection, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        for edge in edges:
            general_collection = self.conn.collection(edge.collection_name)
            for index_obj in edge.indexes:
                self._add_index(general_collection, index_obj)

    def fetch_indexes(self, db_class_name: str | None = None):
        if db_class_name is None:
            classes = self.conn.collections()
        elif self.conn.has_collection(db_class_name):
            classes = [self.conn.collection(db_class_name)]
        else:
            classes = []

        r = {}
        for cname in classes:
            assert isinstance(cname["name"], str)
            c = self.conn.collection(cname["name"])
            r[cname["name"]] = c.indexes()
        return r

    def create_collection(self, db_class_name, index: None | Index = None, g=None):
        if not self.conn.has_collection(db_class_name):
            if g is not None:
                _ = g.create_vertex_collection(db_class_name)
            else:
                self.conn.create_collection(db_class_name)
            general_collection = self.conn.collection(db_class_name)
            if index is not None and index.fields != ["_key"]:
                ih = self._add_index(general_collection, index)
                return ih
            else:
                return None

    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        logger.info("collections (non system):")
        logger.info([c for c in self.conn.collections() if c["name"][0] != "_"])

        if delete_all:
            cnames = [c["name"] for c in self.conn.collections() if c["name"][0] != "_"]
            gnames = [g["name"] for g in self.conn.graphs()]

        for gn in gnames:
            if self.conn.has_graph(gn):
                self.conn.delete_graph(gn)

        logger.info("graphs (after delete operation):")
        logger.info(self.conn.graphs())

        for cn in cnames:
            if self.conn.has_collection(cn):
                self.conn.delete_collection(cn)

        logger.info("collections (after delete operation):")
        logger.info([c for c in self.conn.collections() if c["name"][0] != "_"])

        logger.info("graphs:")
        logger.info(self.conn.graphs())

    def get_collections(self):
        return self.conn.collections()

    def upsert_docs_batch(
        self,
        docs,
        class_name,
        match_keys: list[str] | None = None,
        **kwargs,
    ):
        """

        :param docs: list of dicts (json-like, ie keys are strings)
        :param class_name: collection where to upsert
        :param match_keys: keys on which to look for document
        :param update_keys: keys which to update if doc in the collection, if update_keys='doc', update all
        :param filter_uniques:
        :return:
        """
        dry = kwargs.pop("dry", False)
        update_keys = kwargs.pop("update_keys", None)
        filter_uniques = kwargs.pop("filter_uniques", True)

        if isinstance(docs, list):
            if filter_uniques:
                docs = pick_unique_dict(docs)
            docs = json.dumps(docs)
        if match_keys is None:
            upsert_clause = ""
            update_clause = ""
        else:
            upsert_clause = ", ".join([f'"{k}": doc.{k}' for k in match_keys])
            upsert_clause = f"UPSERT {{{upsert_clause}}}"

            if isinstance(update_keys, list):
                update_clause = ", ".join([f'"{k}": doc.{k}' for k in update_keys])
                update_clause = f"{{{update_clause}}}"
            elif update_keys == "doc":
                update_clause = "doc"
            else:
                update_clause = "{}"
            update_clause = f"UPDATE {update_clause}"

        options = "OPTIONS {exclusive: true, ignoreErrors: true}"

        q_update = f"""FOR doc in {docs}
                            {upsert_clause}
                            INSERT doc
                            {update_clause} 
                                IN {class_name} {options}"""
        if not dry:
            self.execute(q_update)

    def insert_edges_batch(
        self,
        docs_edges,
        source_class,
        target_class,
        relation_name=None,
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
        """
            using ("_key",) for match_keys_source and match_keys_target saves time
                (no need to look it up from field discriminants)

        :param docs_edges: in format  [{ _source_aux: source_doc, _target_aux: target_doc}]
        :param source_class,
        :param target_class,
        :param relation_name:
        :param collection_name:
        :param match_keys_source:
        :param match_keys_target:
        :param filter_uniques:
        :param uniq_weight_fields
        :param uniq_weight_collections
        :param upsert_option
        :param head: keep head docs

        :return:
        """

        dry = kwargs.pop("dry", False)

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
            if uniq_weight_fields is not None:
                weight_fs += uniq_weight_fields
            if uniq_weight_collections is not None:
                weight_fs += uniq_weight_collections
            if relation_name is not None:
                weight_fs += ["relation"]

            if weight_fs:
                weights_clause = ", " + ", ".join(
                    [f"'{x}' : edge.{x}" for x in weight_fs]
                )
            else:
                weights_clause = ""

            upsert = f"{{'_from': {ups_from}, '_to': {ups_to}" + weights_clause + "}"
            logger.debug(f" upsert clause: {upsert}")
            clauses = f"UPSERT {upsert} INSERT doc UPDATE {{}}"
            options = "OPTIONS {exclusive: true}"
        else:
            if relation_name is None:
                doc_clause = "doc"
            else:
                doc_clause = f"MERGE(doc, {{'relation': '{relation_name}' }})"
            clauses = f"INSERT {doc_clause}"
            options = "OPTIONS {exclusive: true, ignoreErrors: true}"

        q_update = f"""
            FOR edge in {docs_edges_str} {source_filter} {target_filter}
                LET doc = {doc_definition}
                {clauses}
                in {collection_name} {options}"""
        if not dry:
            self.execute(q_update)

    def insert_return_batch(self, docs, class_name):
        docs = json.dumps(docs)
        query0 = f"""FOR doc in {docs}
              INSERT doc
              INTO {class_name}
              LET inserted = NEW
              RETURN {{_key: inserted._key}}
        """
        return query0

    def fetch_present_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        flatten=False,
        filters: None | Clause | list | dict = None,
    ) -> list | dict:
        """
            for each jth doc from `docs` matching to docs in `collection_name` by `match_keys`
                return the list of `return_keys`
        :param batch:
        :param class_name:
        :param match_keys:
        :param keep_keys:
        :param flatten:
        :param filters: return docs from db satisfying condition
        :return:
        """
        q0 = fetch_fields_query(
            collection_name=class_name,
            docs=batch,
            match_keys=match_keys,
            keep_keys=keep_keys,
            filters=filters,
        )
        # {"__i": i, "_group": [doc]}
        cursor = self.execute(q0)

        if flatten:
            rdata = []
            for item in get_data_from_cursor(cursor):
                group = item.pop("_group", [])
                rdata += [sub_item for sub_item in group]
            return rdata
        else:
            rdata_dict = {}
            for item in get_data_from_cursor(cursor):
                __i = item.pop("__i")
                group = item.pop("_group")
                rdata_dict[__i] = group
            return rdata_dict

    def fetch_docs(
        self,
        class_name,
        filters: None | Clause | list | dict = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
    ):
        """

        :param class_name:
        :param filters:
        :param limit:
            {"AND": [["==", "1", "x"], ["==", "2", "y", "% 2"]]}
        :param return_keys:
        :param unset_keys:
        :return:
        """

        filter_clause = render_filters(filters, doc_name="d")

        if return_keys is None:
            if unset_keys is None:
                return_clause = "d"
            else:
                tmp_clause = ", ".join([f'"{item}"' for item in unset_keys])
                return_clause = f"UNSET(d, {tmp_clause})"
        else:
            if unset_keys is None:
                tmp_clause = ", ".join([f'"{item}"' for item in return_keys])
                return_clause = f"KEEP(d, {tmp_clause})"
            else:
                raise ValueError("both return_keys and unset_keys are set")

        if limit is not None and isinstance(limit, int):
            limit_clause = f"LIMIT {limit}"
        else:
            limit_clause = ""

        q = (
            f"FOR d in {class_name}"
            f"  {filter_clause}"
            f"  {limit_clause}"
            f"  RETURN {return_clause}"
        )
        cursor = self.execute(q)
        return get_data_from_cursor(cursor)

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: None | Clause | list | dict = None,
    ):
        """

        :param class_name:
        :param aggregation_function:
        :param discriminant:
        :param aggregated_field:
        :param filters:
        :return:
        """

        filter_clause = render_filters(filters, doc_name="doc")

        if (
            aggregated_field is not None
            and aggregation_function != AggregationType.COUNT
        ):
            group_unit = f"g[*].doc.{aggregated_field}"
        else:
            group_unit = "g"

        if discriminant is not None:
            collect_clause = f"COLLECT value = doc['{discriminant}'] INTO g"
            return_clause = f"""{{ '{discriminant}' : value, '_value' :{aggregation_function}({group_unit})}}"""
        else:
            if (
                aggregated_field is None
                and aggregation_function == AggregationType.COUNT
            ):
                collect_clause = (
                    f"COLLECT AGGREGATE value =  {aggregation_function} (doc)"
                )
            else:
                collect_clause = (
                    "COLLECT AGGREGATE value ="
                    f" {aggregation_function}(doc['{aggregated_field}'])"
                )
            return_clause = """{ '_value' : value }"""

        q = f"""FOR doc IN {class_name} 
                    {filter_clause}
                    {collect_clause}
                    RETURN {return_clause}"""

        cursor = self.execute(q)
        data = get_data_from_cursor(cursor)
        return data

    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: None | Clause | list | dict = None,
    ):
        """
            from `batch` return docs that are not present in `collection` according to `match_keys`
        :param batch:
        :param class_name:
        :param match_keys:
        :param keep_keys:
        :param filters: filter selects documents, so docs with filter applied will be returned
                            and not returned as negative docs
        :return:
        """

        present_docs_keys = self.fetch_present_documents(
            batch=batch,
            class_name=class_name,
            match_keys=match_keys,
            keep_keys=keep_keys,
            flatten=False,
            filters=filters,
        )

        assert isinstance(present_docs_keys, dict)

        # there were multiple docs return for the same pair of filtering condition
        if any([len(v) > 1 for v in present_docs_keys.values()]):
            logger.warning(
                "fetch_present_documents returned multiple docs per filtering"
                " condition"
            )

        absent_indices = sorted(set(range(len(batch))) - set(present_docs_keys.keys()))
        batch_absent = [batch[j] for j in absent_indices]
        return batch_absent

    def update_to_numeric(self, collection_name, field):
        s1 = f"FOR p IN {collection_name} FILTER p.{field} update p with {{"
        s2 = f"{field}: TO_NUMBER(p.{field}) "
        s3 = f"}} in {collection_name}"
        q0 = s1 + s2 + s3
        return q0
