import abc
import logging
from typing import Type, TypeVar

from graph_cast.architecture.general import Configurator
from graph_cast.db.arango.util import define_extra_edges, update_to_numeric

logger = logging.getLogger(__name__)


ConnectionType = TypeVar("ConnectionType", bound="Connection")
ConnectionConfigType = TypeVar(
    "ConnectionConfigType", bound="ConnectionConfig"
)


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


class ConnectionConfig(abc.ABC):
    connection_class: Type[Connection]

    def __init__(self, **config):
        self.protocol = config.get("protocol", "http")
        self.ip_addr = config.get("ip_addr", None)
        self.cred_name = config.get("cred_name", None)
        self.cred_pass = config.get("cred_pass", None)
        self.database = config.get("database", None)
        self.port = config.get("port", None)
        self.hosts = None
        self.request_timeout = config.get("request_timeout", 60)


class WSGIConfig(ConnectionConfig):
    def __init__(self, **config):
        hosts = config.pop("hosts", None)
        if hosts is None:
            super(WSGIConfig, self).__init__(**config)
            self.path = config.get("path", "/")
            self.paths = config.get("paths", {})
            self.hosts = (
                f"{self.protocol}://{self.ip_addr}:{self.port}{self.path}"
            )
            self.host = config.get("host", None)
        else:
            # validate hosts?
            self.hosts = hosts
            self._parse_hosts()

    def _parse_hosts(self):
        h = self.hosts

        h2 = h.split("://")
        self.protocol = h2[0]
        h3 = h2[1].split(":")
        self.ip_addr = h3[0]
        h4 = h3[1].split("/")
        self.port = h4[0]
        self.path = "/" + "/".join(h4[1:])


def init_db(db_client: ConnectionType, conf_obj: Configurator, clean_start):
    if clean_start:
        db_client.delete_collections([], [], delete_all=True)
        #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
        # elif clean_start == "edges":
        #     delete_collections(sys_db, ecollections, [])
    db_client.define_collections(conf_obj.graph_config, conf_obj.vertex_config)
    db_client.define_indices(conf_obj.graph_config, conf_obj.vertex_config)


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
