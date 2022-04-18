from itertools import product
import pandas as pd
import logging
from typing import Union
from graph_cast.input.csv_abs import table_to_vcollections
from graph_cast.util.io import Chunker, ChunkerDataFrame
from graph_cast.db import ConnectionManager, ConnectionConfigType

from graph_cast.db.arango.util import (
    upsert_docs_batch,
    insert_edges_batch,
    insert_return_batch,
)


logger = logging.getLogger(__name__)


def process_table(
    tabular_resource: Union[str, pd.DataFrame],
    batch_size,
    max_lines,
    db_config: ConnectionConfigType,
    conf,
):
    if isinstance(tabular_resource, pd.DataFrame):
        chk = ChunkerDataFrame(tabular_resource, batch_size, max_lines)
    elif isinstance(tabular_resource, str):
        chk = Chunker(tabular_resource, batch_size, max_lines, encoding=conf.encoding)
        conf.set_current_resource_name(tabular_resource)
    else:
        raise TypeError(f"tabular_resource type is not str or pd.DataFrame")
    header = chk.pop_header()
    header_dict = dict(zip(header, range(len(header))))

    logger.debug(f"processing current table resource : {tabular_resource}")

    while not chk.done:
        lines = chk.pop()
        if lines:

            # file to vcols, ecols
            vdocuments, edocuments = table_to_vcollections(
                lines,
                header_dict,
                conf,
            )

            # transform vcols, ecols
            # ingest vcols, ecols

            with ConnectionManager(connection_config=db_config) as db_client:
                for vcol, data in vdocuments.items():
                    # blank nodes: push and get back their keys  {"_key": ...}
                    if vcol in conf.vertex_config.blank_collections:
                        query0 = insert_return_batch(
                            data, conf.vertex_config.dbname(vcol)
                        )
                        cursor = db_client.execute(query0)
                        vdocuments[vcol] = [item for item in cursor]
                    else:
                        query0 = upsert_docs_batch(
                            data,
                            conf.vertex_config.dbname(vcol),
                            conf.vertex_config.index(vcol),
                            "doc",
                            True,
                        )
                        cursor = db_client.execute(query0)

                # update edge data with blank node edges
                for vcol in conf.vertex_config.blank_collections:
                    for vfrom, vto in conf.current_graphs:
                        if vcol == vfrom or vcol == vto:
                            edocuments[(vfrom, vto)].extend(
                                [
                                    {"source": x, "target": y}
                                    for x, y in zip(vdocuments[vfrom], vdocuments[vto])
                                ]
                            )

                for (vfrom, vto), data in edocuments.items():
                    query0 = insert_edges_batch(
                        data,
                        conf.vertex_config.dbname(vfrom),
                        conf.vertex_config.dbname(vto),
                        conf.graph(vfrom, vto)["edge_name"],
                        conf.vertex_config.index(vfrom),
                        conf.vertex_config.index(vto),
                        False,
                    )
                    cursor = db_client.execute(query0)

                # #create edge u -> v from u->w, v->w edges
                # # find edge_cols uw and vw
                # for u, v in conf_obj.graph_config.extra_edges:
                #     query0 = define_extra_edges(conf_obj.graph(u, v))
                #     cursor = db_config.execute(query0)
