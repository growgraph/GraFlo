import gzip
import json
import multiprocessing as mp
from collections import defaultdict
from functools import partial
from os import listdir
from os.path import isfile, join
import logging

from graph_cast.db.arango.util import (
    upsert_docs_batch,
    insert_edges_batch,
    update_to_numeric,
    define_extra_edges,
)

import graph_cast.input.json as gcij
import graph_cast.input.table
from graph_cast.util import timer as timer
from graph_cast.util.transform import merge_doc_basis
from graph_cast.architecture import TConfigurator, JConfigurator
from graph_cast.db import ConnectionManager, ConnectionConfigType, ConnectionType


logger = logging.getLogger(__name__)


def ingest_json_files(
    fpath, config, db_client, keyword="DSSHPSH", clean_start="all", dry=False, ncores=1
):
    conf_obj = JConfigurator(config)

    if clean_start == "all":
        db_client.delete_collections([], [], delete_all=True)

        db_client.define_collections(
            conf_obj.graph_config,
            conf_obj.vertex_config,
        )

        db_client.define_indices(
            conf_obj.graph_config,
            conf_obj.vertex_config,
        )

    files = sorted(
        [f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f]
    )

    logger.info(f" Processing {len(files)} json files : {files}")

    for filename in files:
        with gzip.GzipFile(join(fpath, filename), "rb") as fps:
            with timer.Timer() as t_pro:
                data = json.load(fps)
                ingest_json(data, conf_obj, db_client, dry, ncores)
            logger.info(f" processing {filename} took {t_pro.elapsed:.2f} sec")


def ingest_json(json_data, conf_obj: JConfigurator, sys_db=None, dry=False, ncores=1):

    with timer.Timer() as t_parse:

        kwargs = {
            "config": conf_obj,
            "merge_collections": ["publication"],
        }
        func = partial(gcij.jsondoc_to_vertices_edges, **kwargs)
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
            if not dry and sys_db is not None:
                cursor = sys_db.execute(query0)

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

            if not dry and sys_db is not None:
                cursor = sys_db.execute(query0)

    logger.info(f" ingested {cnt} edges {t_ingest_edges.elapsed:.2f} sec")

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    # for uv, item in graphs.items():
    #     if item["type"] == "indirect":
    #         query0 = define_extra_edges(item)
    #         cursor = sys_db.aql.execute(query0)


def ingest_csvs(
    fpath,
    conn_config: ConnectionConfigType,
    limit_files=None,
    max_lines=None,
    batch_size=5000000,
    clean_start=False,
    config=None,
):

    conf_obj = TConfigurator(config)
    with ConnectionManager(connection_config=conn_config) as db_client:
        init_db(db_client, conf_obj, clean_start)

    # file discovery
    conf_obj.discover_files(fpath, limit_files=limit_files)

    logger.info(conf_obj.mode2files)

    for mode in conf_obj.modes2collections:
        conf_obj.set_mode(mode)
        kwargs = {
            "batch_size": batch_size,
            "max_lines": max_lines,
            "conf": conf_obj,
            "db_config": conn_config,
        }

        with timer.Timer() as klepsidra:
            func = partial(graph_cast.input.table.process_table, **kwargs)
            n_proc = 1
            if n_proc > 1:
                with mp.Pool(n_proc) as p:
                    p.map(func, conf_obj.mode2files[mode])
            else:
                for f in conf_obj.mode2files[mode]:
                    func(f)
        logger.info(f"{mode} took {klepsidra.elapsed:.1f} sec")


def etl_over_files(
    fpath,
    db_config,
    limit_files=None,
    max_lines=None,
    batch_size=5000000,
    clean_start=False,
    config=None,
):

    pass
    # init db: collections, indexes

    # identify files

    # loop over files
    # file to vcols, ecols
    # transform vcols, ecols
    # ingest vcols, ecols

    # extra definitions - should be part atomic


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


def concluding_db_transform(conf_obj, db_config):
    # TODO this should be made part of atomic etl (not applied to the whole db)
    for cname in conf_obj.vertex_config.collections:
        for field in conf_obj.vertex_config.numeric_fields_list(cname):
            query0 = update_to_numeric(conf_obj.vertex_config.dbname(cname), field)
            cursor = db_config.execute(query0)

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    for u, v in conf_obj.graph_config.extra_edges:
        query0 = define_extra_edges(conf_obj.graph(u, v))
        cursor = db_config.execute(query0)
