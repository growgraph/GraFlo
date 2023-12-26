import multiprocessing as mp
import queue
from typing import Optional, Union

import pandas as pd
from suthing import DBConnectionConfig

from graph_cast.architecture import ConfiguratorType
from graph_cast.architecture.onto import SOURCE_AUX, TARGET_AUX
from graph_cast.db import ConnectionManager
from graph_cast.input import table_to_collections
from graph_cast.input.table import logger
from graph_cast.input.util import list_to_dict_edges, list_to_dict_vertex
from graph_cast.util.chunking import AbsChunker, Chunker, ChunkerDataFrame


def process_table(
    tabular_resource: Union[str, pd.DataFrame],
    conf: ConfiguratorType,
    db_config: Optional[DBConnectionConfig] = None,
    batch_size: int = 1000,
    limit: int = 10000,
    dry=False,
):
    """
        given a csv, config that specifies csv to graph mapping and db_config, transform csv and load it into db

    Args:
        tabular_resource:
        conf:
        db_config:
        batch_size:
        limit:
        dry:

    Returns:

    """

    logger.info("in process_table")
    logger.info(f"max_lines : {limit}")
    logger.info(f"batch_size : {batch_size}")

    if isinstance(tabular_resource, pd.DataFrame):
        chk: AbsChunker = ChunkerDataFrame(
            tabular_resource, batch_size=batch_size, n_lines_max=limit
        )
    elif isinstance(tabular_resource, str):
        chk = Chunker(
            tabular_resource,
            batch_size=batch_size,
            limit=limit,
            encoding=conf.encoding,
        )
        # conf.set_current_resource_name(tabular_resource)
    else:
        raise TypeError(f"tabular_resource type is not str or pd.DataFrame")
    header = chk.pop_header()
    header_dict = dict(zip(header, range(len(header))))

    logger.info(f"processing current csv resource : {tabular_resource}")

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

            with ConnectionManager(connection_config=db_config) as db_client:
                for vcol, data in vdocuments.items():
                    # blank nodes: push and get back their keys  {"_key": ...}
                    if vcol in conf.vertex_config.blank_vertices:
                        query0 = db_client.insert_return_batch(
                            data, conf.vertex_config.vertex_dbname(vcol)
                        )
                        cursor = db_client.execute(query0)
                        vdocuments[vcol] = [item for item in cursor]
                    else:
                        db_client.upsert_docs_batch(
                            data,
                            conf.vertex_config.vertex_dbname(vcol),
                            conf.vertex_config.index(vcol),
                            update_keys="doc",
                            filter_uniques=True,
                            dry=dry,
                        )

                # update edge misc with blank node edges
                for vcol in conf.vertex_config.blank_vertices:
                    for vfrom, vto in conf.current_edges:
                        if vcol == vfrom or vcol == vto:
                            edocuments[(vfrom, vto)].extend(
                                [
                                    {SOURCE_AUX: x, TARGET_AUX: y}
                                    for x, y in zip(
                                        vdocuments[vfrom], vdocuments[vto]
                                    )
                                ]
                            )

                for (vfrom, vto), data in edocuments.items():
                    db_client.insert_edges_batch(
                        data,
                        conf.vertex_config.vertex_dbname(vfrom),
                        conf.vertex_config.vertex_dbname(vto),
                        conf.graph(vfrom, vto).relation,
                        conf.vertex_config.index(vfrom).fields,
                        conf.vertex_config.index(vto).fields,
                        False,
                        dry=dry,
                    )

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
