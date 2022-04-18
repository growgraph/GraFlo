import multiprocessing as mp
from collections import defaultdict
from functools import partial
import logging

from graph_cast.architecture import JConfigurator
from graph_cast.db import ConnectionConfigType, ConnectionManager
from graph_cast.db.arango.util import upsert_docs_batch, insert_edges_batch
from graph_cast.util import timer as timer
from graph_cast.util.transform import merge_doc_basis
from graph_cast.input.json import jsonlike_to_collections

logger = logging.getLogger(__name__)


def process_jsonlike(
    json_data,
    conf_obj: JConfigurator,
    db_config: ConnectionConfigType,
    dry=False,
    ncores=1,
):
    """

    :param json_data:
    :param conf_obj:
    :param db_config:
    :param dry:
    :param ncores:
    :return:
    """

    with timer.Timer() as t_parse:

        kwargs = {"config": conf_obj}
        func = partial(jsonlike_to_collections, **kwargs)
        if ncores > 1:
            with mp.Pool(ncores) as p:
                ldicts = p.map(func, json_data)
        else:
            ldicts = list(map(func, json_data))

        super_dict = defaultdict(list)

        for d in ldicts:
            for k, v in d.items():
                super_dict[k].extend(v)

        stats = [(k, len(v) / len(ldicts)) for k, v in super_dict.items()]
        stats = sorted(stats, key=lambda y: y[1])

    logger.info(
        f" converting json to vertices and edges took {t_parse.elapsed:.2f} sec"
    )

    for x in stats[-5:][::-1]:
        logger.info(f" collection {x[0]} has {x[1]} items per record")

    with timer.Timer() as t_ingest:

        kkey_vertex = sorted([k for k in super_dict.keys() if isinstance(k, str)])
        kkey_edge = sorted([k for k in super_dict.keys() if isinstance(k, tuple)])

        cnt = 0
        for k in kkey_vertex:
            v = super_dict[k]
            r = merge_doc_basis(v, conf_obj.vertex_config.index(k))
            cnt += len(r)
            query0 = upsert_docs_batch(
                v,
                conf_obj.vertex_config.dbname(k),
                conf_obj.vertex_config.index(k),
                "doc",
                True,
            )
            if not dry and db_config is not None:
                cursor = db_config.execute(query0)

    # logger.info(f" ingested {cnt} vertices {t_ingest.elapsed:.2f} sec")

    with timer.Timer() as t_ingest_edges:

        cnt = 0

        for uv in kkey_edge:
            vfrom, vto = uv
            if len(super_dict[uv]) == 0:
                logger.error(f" for unknown reason edge batch {vfrom}, {vto} is empty")
                logger.error(
                    f" for unknown reason edge batch "
                    f"size of {vfrom} : {len(super_dict[vfrom])}, {vto} : {len(super_dict[vto])}"
                )
                continue
            cnt += len(super_dict[uv])
            query0 = insert_edges_batch(
                super_dict[uv],
                conf_obj.vertex_config.dbname(vfrom),
                conf_obj.vertex_config.dbname(vto),
                conf_obj.graph(vfrom, vto)["edge_name"],
                conf_obj.vertex_config.index(vfrom),
                conf_obj.vertex_config.index(vto),
                False,
            )

            if not dry and db_config is not None:
                cursor = db_config.execute(query0)

    logger.info(f" ingested {cnt} edges {t_ingest_edges.elapsed:.2f} sec")

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    # for uv, item in graphs.items():
    #     if item["type"] == "indirect":
    #         query0 = define_extra_edges(item)
    #         cursor = sys_db.aql.execute(query0)
