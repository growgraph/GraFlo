import gzip
import json
from collections import defaultdict
from os import listdir
from os.path import isfile, join
from functools import partial
import multiprocessing as mp
import logging
from graph_cast.arango.util import (
    get_arangodb_client,
    delete_collections,
    define_collections,
    upsert_docs_batch,
    insert_edges_batch,
)
from graph_cast.util.tranform import merge_doc_basis
from graph_cast.json.util import parse_edges, process_document_top, parse_config
import graph_cast.util.timer as timer

logger = logging.getLogger(__name__)


def ingest_json_files(
    fpath,
    config,
    protocol="http",
    ip_addr="127.0.0.1",
    port=8529,
    database="_system",
    cred_name="root",
    cred_pass="123",
    keyword="DSSHPSH",
    clean_start="all",
    prefix="toy_",
    dry=False,
):
    sys_db = get_arangodb_client(
        protocol, ip_addr, port, database, cred_name, cred_pass
    )

    vcollections, vmap, graphs, index_fields_dict, eindex = parse_config(
        config=config, prefix=prefix
    )

    edge_des, excl_fields = parse_edges(config["json"], [], defaultdict(list))
    # all_fields_dict = {
    #     k: v["fields"] for k, v in config["vertex_collections"].items()
    # }

    if clean_start == "all":
        delete_collections(sys_db, [], [], delete_all=True)
        #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
        # elif clean_start == "edges":
        #     delete_collections(sys_db, ecollections, [])

        define_collections(sys_db, graphs, vmap, index_fields_dict, eindex)

    files = sorted(
        [f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f]
    )
    logger.info(f" Processing {len(files)} json files : {files}")

    for filename in files:
        with gzip.GzipFile(join(fpath, filename), "rb") as fps:
            with timer.Timer() as t_pro:
                data = json.load(fps)
                ingest_json(data, config, prefix, sys_db, dry)
            logger.info(f" processing {filename} took {t_pro.elapsed:.2f} sec")


def ingest_json(json_data, config, prefix, sys_db=None, dry=False):
    vcollections, vmap, graphs, index_fields_dict, eindex = parse_config(
        config=config, prefix=prefix
    )
    edge_des, excl_fields = parse_edges(config["json"], [], defaultdict(list))

    with timer.Timer() as t_parse:

        kwargs = {
            "config": config["json"],
            "vertex_config": config["vertex_collections"],
            "edge_fields": excl_fields,
            "merge_collections": ["publication"],
        }
        func = partial(process_document_top, **kwargs)
        n_proc = 4
        with mp.Pool(n_proc) as p:
            ldicts = p.map(func, json_data)

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
            r = merge_doc_basis(super_dict[k], index_fields_dict[k])
            cnt += len(r)
            query0 = upsert_docs_batch(v, vmap[k], index_fields_dict[k], "doc", True)
            if not dry and sys_db is not None:
                cursor = sys_db.aql.execute(query0)

    logger.info(f" ingested {cnt} vertices {t_ingest.elapsed:.2f} sec")

    with timer.Timer() as t_ingest_edges:

        cnt = 0

        for uv in kkey_edge:
            u, v = uv
            if len(super_dict[uv]) == 0:
                logger.error(f" for unknown reason edge batch {u}, {v} is empty")
                logger.error(
                    f" for unknown reason edge batch "
                    f"size of {u} : {len(super_dict[u])}, {v} : {len(super_dict[v])}"
                )
                continue
            cnt += len(super_dict[uv])
            query0 = insert_edges_batch(
                super_dict[uv],
                vmap[u],
                vmap[v],
                graphs[uv]["edge_name"],
                index_fields_dict[u],
                index_fields_dict[v],
                False,
            )
            if not dry and sys_db is not None:
                cursor = sys_db.aql.execute(query0)

    logger.info(f" ingested {cnt} edges {t_ingest_edges.elapsed:.2f} sec")

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    # for uv, item in graphs.items():
    #     if item["type"] == "indirect":
    #         query0 = define_extra_edges(item)
    #         cursor = sys_db.aql.execute(query0)
