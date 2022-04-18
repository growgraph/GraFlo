import gzip
import json
import multiprocessing as mp
from functools import partial
from os import listdir
from os.path import isfile, join
import logging
from typing import Optional

import graph_cast.input.json
import graph_cast.input.table_flow
import graph_cast.input.table
from graph_cast.db.connection import init_db
from graph_cast.input.json_flow import process_jsonlike
from graph_cast.util import timer as timer
from graph_cast.architecture import TConfigurator, JConfigurator
from graph_cast.db import ConnectionManager, ConnectionConfigType
from graph_cast.architecture import ConfiguratorType

logger = logging.getLogger(__name__)


# def etl_over_files(
#     fpath,
#     db_config,
#     limit_files=None,
#     max_lines=None,
#     batch_size=5000000,
#     clean_start=False,
#     config=None,
# ):
#
#     init db: collections, indexes
#
#     identify files
#
#     loop over files
#     file to vcols, ecols
#     transform vcols, ecols
#     ingest vcols, ecols
#
#     extra definitions


def ingest_json_files(
    fpath,
    config,
    conn_conf: ConnectionConfigType,
    keyword: Optional[str] = None,
    clean_start="all",
    dry=False,
    ncores=1,
):
    conf_obj = JConfigurator(config)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        init_db(db_client, conf_obj, clean_start)

    # file discovery <- move this foo to JConfigurator
    files = sorted([f for f in listdir(fpath) if isfile(join(fpath, f)) if "json" in f])
    if keyword is not None:
        files = [f for f in files if keyword in f]

    logger.info(f" Processing {len(files)} json files : {files}")

    for filename in files:
        with gzip.GzipFile(join(fpath, filename), "rb") as fps:
            with timer.Timer() as t_pro:
                data = json.load(fps)
                process_jsonlike(data, conf_obj, conn_conf, ncores=ncores, dry=dry)
            logger.info(f" processing {filename} took {t_pro.elapsed:.2f} sec")


def ingest_csvs(
    fpath,
    config,
    conn_config: ConnectionConfigType,
    limit_files=None,
    max_lines=None,
    batch_size=5000000,
    clean_start=False,
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
            func = partial(graph_cast.input.table_flow.process_table, **kwargs)
            n_proc = 1
            if n_proc > 1:
                with mp.Pool(n_proc) as p:
                    p.map(func, conf_obj.mode2files[mode])
            else:
                for f in conf_obj.mode2files[mode]:
                    func(f)
        logger.info(f"{mode} took {klepsidra.elapsed:.1f} sec")
