import abc
import logging
from typing import TypeVar

from graph_cast.architecture.general import Configurator
from graph_cast.db.arango.util import define_extra_edges, update_to_numeric

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
    def execute(self, query):
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

    # @abc.abstractmethod
    # def get_collections(self):
    #     pass

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


def init_db(db_client: ConnectionType, conf_obj: Configurator, clean_start):
    db_client.init_db(conf_obj, clean_start)


def concluding_db_transform(db_client: ConnectionType, conf_obj):
    # TODO this should be made part of atomic etl (not applied to the whole db)
    for cname in conf_obj.vertex_config.collections:
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
