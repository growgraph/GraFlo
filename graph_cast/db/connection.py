import abc
import logging
from typing import TypeVar

from graph_cast.architecture.general import Configurator
from graph_cast.db.arango.util import define_extra_edges, update_to_numeric
from graph_cast.onto import AggregationType

logger = logging.getLogger(__name__)
ConnectionType = TypeVar("ConnectionType", bound="Connection")


class Connection(abc.ABC):
    def __init__(self):
        pass

    @abc.abstractmethod
    def create_database(self, name: str):
        pass

    @abc.abstractmethod
    def delete_database(self, name: str):
        pass

    @abc.abstractmethod
    def execute(self, query, **kwargs):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def define_indices(self, graph_config, vertex_config):
        pass

    @abc.abstractmethod
    def define_collections(self, graph_config, vertex_config):
        pass

    @abc.abstractmethod
    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        pass

    @abc.abstractmethod
    def init_db(self, conf_obj: Configurator, clean_start):
        pass

    @abc.abstractmethod
    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        pass

    @abc.abstractmethod
    def insert_edges_batch(
        self,
        docs_edges,
        source_class,
        target_class,
        relation_name,
        match_keys_source,
        match_keys_target,
        filter_uniques=True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option=False,
        head=None,
        **kwargs,
    ):
        pass

    @abc.abstractmethod
    def insert_return_batch(self, docs, class_name):
        pass

    @abc.abstractmethod
    def fetch_docs(self, class_name, filters, limit, return_keys):
        pass

    @abc.abstractmethod
    def fetch_present_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        flatten=False,
        filters: list | dict | None = None,
    ):
        pass

    @abc.abstractmethod
    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        pass

    @abc.abstractmethod
    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
        pass

    # @abc.abstractmethod
    # def define_vertex_collections(self, graph_config, vertex_config):
    #     pass
    #
    # @abc.abstractmethod
    # def define_edge_collections(self, graph_config):
    #     pass
    #
    # @abc.abstractmethod
    # def define_vertex_indices(self, vertex_config):
    #     pass
    #
    # @abc.abstractmethod
    # def define_edge_indices(self, graph_config):
    #     pass
    #
    # @abc.abstractmethod
    # def create_collection_if_absent(self, g, vcol, index, unique=True):
    #     pass


def concluding_db_transform(db_client: ConnectionType, conf_obj):
    # TODO this should be made part of atomic etl (not applied to the whole db)
    for cname in conf_obj.vertex_config.vertex_set:
        for field in conf_obj.vertex_config.numeric_fields_list(cname):
            query0 = update_to_numeric(
                conf_obj.vertex_config.vertex_dbname(cname), field
            )
            db_client.execute(query0)

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    for ee in conf_obj.graph_config.extra_edges:
        query0 = define_extra_edges(ee)
        db_client.execute(query0)
