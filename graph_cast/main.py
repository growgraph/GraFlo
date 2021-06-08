import gzip
import json
import multiprocessing as mp
from collections import defaultdict
from functools import partial
from os import listdir
from os.path import isfile, join
import logging

from graph_cast.input.util import parse_vcollection
from graph_cast.arango.util import (
    delete_collections,
    define_collections,
    upsert_docs_batch,
    insert_edges_batch,
    update_to_numeric,
    define_extra_edges
)

from graph_cast.input.util import define_graphs, update_graph_extra_edges
import graph_cast.input.json as gcij
import graph_cast.input.csv as gcic
from graph_cast.util import timer as timer
from graph_cast.util.transform import merge_doc_basis

logger = logging.getLogger(__name__)


def ingest_json_files(
    fpath,
    config,
    db_client,
    keyword="DSSHPSH",
    clean_start="all",
    dry=False,
):

    vcollections, vmap, graphs, index_fields_dict, extra_index = gcij.parse_config(
        config=config
    )

    if clean_start == "all":
        delete_collections(db_client, [], [], delete_all=True)
        #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
        # elif clean_start == "edges":
        #     delete_collections(sys_db, ecollections, [])

        define_collections(db_client, graphs, vmap, index_fields_dict, extra_index)

    files = sorted(
        [f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f]
    )
    logger.info(f" Processing {len(files)} json files : {files}")

    for filename in files:
        with gzip.GzipFile(join(fpath, filename), "rb") as fps:
            with timer.Timer() as t_pro:
                data = json.load(fps)
                ingest_json(data, config, db_client, dry)
            logger.info(f" processing {filename} took {t_pro.elapsed:.2f} sec")


def ingest_json(json_data, config, sys_db=None, dry=False):
    vcollections, vmap, graphs, index_fields_dict, eindex = gcij.parse_config(
        config=config
    )
    edge_des, excl_fields = gcij.parse_edges(config["json"], [], defaultdict(list))

    with timer.Timer() as t_parse:

        kwargs = {
            "config": config["json"],
            "vertex_config": config["vertex_collections"],
            "edge_fields": excl_fields,
            "merge_collections": ["publication"],
        }
        func = partial(gcij.process_document_top, **kwargs)
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


def ingest_csvs(
    fpath,
    db_client,
    limit_files=None,
    max_lines=None,
    batch_size=50000000,
    clean_start="all",
    config=None,
):
    vmap, index_fields_dict, extra_indices = parse_vcollection(config)

    # vertex_collection_name -> fields_keep
    vcollection_fields_map = {
        k: v["fields"] for k, v in config["vertex_collections"].items()
    }

    # vertex_collection_name -> fields_type
    vcollection_numeric_fields_map = {
        k: v["numeric_fields"]
        for k, v in config["vertex_collections"].items()
        if "numeric_fields" in v
    }

    #############################
    # edge discovery
    field_maps = gcic.parse_input_output_field_map(config["csv"])
    edges, extra_edges = gcic.parse_edges(config)

    graph = define_graphs(edges, vmap)
    graph = update_graph_extra_edges(graph, vmap, extra_edges)

    modes2graphs, modes2collections = gcic.derive_modes2graphs(graph, config["csv"])

    weights_definition = {}
    for item in config["csv"]:
        weights_definition[item["filetype"]] = item["weights"]

    # db operation
    if clean_start == "all":
        delete_collections(db_client, [], [], delete_all=True)
        #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
        # elif clean_start == "edges":
        #     delete_collections(sys_db, ecollections, [])

        define_collections(db_client, graph, vmap, index_fields_dict, extra_indices)

    # file discovery
    files_dict = gcic.discover_files(
        modes2collections.keys(), fpath, limit_files=limit_files
    )
    logger.info(files_dict)

    for mode in modes2collections:
        current_collections = modes2collections[mode]
        current_graphs = modes2graphs[mode]
        with timer.Timer() as t_pro:
            kwargs = {
                "current_collections": current_collections,
                "current_graphs": current_graphs,
                "batch_size": batch_size,
                "max_lines": max_lines,
                "graphs_definition": graph,
                "weights_definition": weights_definition[mode],
                "field_maps": field_maps[mode],
                "index_fields_dict": index_fields_dict,
                "db_client": db_client,
                "vmap": vmap,
                "vcollection_fields_map": vcollection_fields_map
            }

            func = partial(gcic.process_csv, **kwargs)
            n_proc = 1
            if n_proc > 1:
                with mp.Pool(n_proc) as p:
                    ldicts = p.map(func, files_dict[mode])
            else:
                for f in files_dict[mode]:
                    func(f)
        logger.info(f"{mode} took {t_pro.elapsed:.1f} sec")
    for cname, fields in vcollection_numeric_fields_map.items():
        for field in fields:
            query0 = update_to_numeric(vmap[cname], field)
            cursor = db_client.aql.execute(query0)

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    for gname, item in graph.items():
        if item["type"] == "indirect":
            query0 = define_extra_edges(item)
            cursor = db_client.aql.execute(query0)
