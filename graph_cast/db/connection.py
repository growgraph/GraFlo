import logging
from graph_cast.db.abstract_config import ConnectionConfig
from typing import TypeVar
import abc

from graph_cast.db.arango.util import update_to_numeric, define_extra_edges

logger = logging.getLogger(__name__)


ConnectionType = TypeVar("ConnectionType", bound="Connection")


class Connection(abc.ABC):
    def __init__(self, config: ConnectionConfig):
        pass


def init_db(db_client: ConnectionType, conf_obj, clean_start):
    if clean_start:
        db_client.delete_collections([], [], delete_all=True)
        #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
        # elif clean_start == "edges":
        #     delete_collections(sys_db, ecollections, [])
    db_client.define_collections(
        conf_obj.graph_config,
        conf_obj.vertex_config,
    )

    db_client.define_indices(
        conf_obj.graph_config,
        conf_obj.vertex_config,
    )


def concluding_db_transform(db_client: ConnectionType, conf_obj):
    # TODO this should be made part of atomic etl (not applied to the whole db)
    for cname in conf_obj.vertex_config.collections:
        for field in conf_obj.vertex_config.numeric_fields_list(cname):
            query0 = update_to_numeric(conf_obj.vertex_config.dbname(cname), field)
            cursor = db_client.execute(query0)

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    for u, v in conf_obj.graph_config.extra_edges:
        query0 = define_extra_edges(conf_obj.graph(u, v))
        cursor = db_client.execute(query0)
