import multiprocessing as mp
import queue
from typing import Optional, Union

import pandas as pd

from graph_cast.architecture import ConfiguratorType
from graph_cast.architecture.schema import _source_aux, _target_aux
from graph_cast.db import ConnectionManager
from graph_cast.db.arango.util import (
    insert_edges_batch,
    insert_return_batch,
    upsert_docs_batch,
)
from graph_cast.db.onto import DBConnectionConfig
from graph_cast.input import table_to_collections
from graph_cast.input.table import logger
from graph_cast.input.util import list_to_dict_edges, list_to_dict_vertex
from graph_cast.util.io import AbsChunker, Chunker, ChunkerDataFrame


def process_table(
    tabular_resource: Union[str, pd.DataFrame],
    conf: ConfiguratorType,
    db_config: Optional[DBConnectionConfig] = None,
    batch_size: int = 1000,
    max_lines: int = 10000,
):
    """
        given a table, config that specifies table to graph mapping and db_config, transform table and load it into db
    :param tabular_resource:
    :param conf:
    :param db_config:
    :param batch_size:
    :param max_lines:
    :return:
    """

    logger.info("in process_table")
    logger.info(f"max_lines : {max_lines}")
    logger.info(f"batch_size : {batch_size}")

    if isinstance(tabular_resource, pd.DataFrame):
        chk: AbsChunker = ChunkerDataFrame(
            tabular_resource, batch_size=batch_size, n_lines_max=max_lines
        )
    elif isinstance(tabular_resource, str):
        chk = Chunker(
            tabular_resource,
            batch_size=batch_size,
            n_lines_max=max_lines,
            encoding=conf.encoding,
        )
        conf.set_current_resource_name(tabular_resource)
    else:
        raise TypeError(f"tabular_resource type is not str or pd.DataFrame")
    header = chk.pop_header()
    header_dict = dict(zip(header, range(len(header))))

    logger.info(f"processing current table resource : {tabular_resource}")

    while not chk.done:
        lines = chk.pop()

        if lines:
            # file to vcols, ecols
            docs = table_to_collections(
                lines,
                header_dict,
                conf,
            )

            vdocuments = list_to_dict_vertex(docs)
            edocuments = list_to_dict_edges(docs)

            # transform vcols, ecols
            # ingest vcols, ecols

            with ConnectionManager(connection_config=db_config) as db_client:
                for vcol, data in vdocuments.items():
                    # blank nodes: push and get back their keys  {"_key": ...}
                    if vcol in conf.vertex_config.blank_collections:
                        query0 = insert_return_batch(
                            data, conf.vertex_config.vertex_dbname(vcol)
                        )
                        cursor = db_client.execute(query0)
                        vdocuments[vcol] = [item for item in cursor]
                    else:
                        query0 = upsert_docs_batch(
                            data,
                            conf.vertex_config.vertex_dbname(vcol),
                            conf.vertex_config.index(vcol),
                            "doc",
                            True,
                        )
                        cursor = db_client.execute(query0)

                # update edge misc with blank node edges
                for vcol in conf.vertex_config.blank_collections:
                    for vfrom, vto in conf.current_graphs:
                        if vcol == vfrom or vcol == vto:
                            edocuments[(vfrom, vto)].extend(
                                [
                                    {_source_aux: x, _target_aux: y}
                                    for x, y in zip(
                                        vdocuments[vfrom], vdocuments[vto]
                                    )
                                ]
                            )

                for (vfrom, vto), data in edocuments.items():
                    query0 = insert_edges_batch(
                        data,
                        conf.vertex_config.vertex_dbname(vfrom),
                        conf.vertex_config.vertex_dbname(vto),
                        conf.graph(vfrom, vto).edge_name,
                        conf.vertex_config.index(vfrom).fields,
                        conf.vertex_config.index(vto).fields,
                        False,
                    )
                    cursor = db_client.execute(query0)

                # #create edge u -> v from u->w, v->w edges
                # # find edge_cols uw and vw
                # for u, v in conf_obj.graph_config.extra_edges:
                #     query0 = define_extra_edges(conf_obj.graph(u, v))
                #     cursor = db_config.execute(query0)

            logger.info(f" processed so far: {chk.units_processed} lines")


def process_table_with_queue(tasks: mp.Queue, **kwargs):
    while True:
        try:
            task = tasks.get_nowait()
        except queue.Empty:
            break
        else:
            process_table(tabular_resource=task, **kwargs)
