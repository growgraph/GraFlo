import gzip
import json
import logging
import multiprocessing as mp
from functools import partial
from os import listdir
from os.path import isfile, join
from typing import Optional

import graph_cast.input.json
import graph_cast.input.table
import graph_cast.input.table_flow
from graph_cast.architecture import JConfigurator, TConfigurator
from graph_cast.db import ConnectionConfigType, ConnectionManager
from graph_cast.db.connection import init_db
from graph_cast.input.json_flow import process_jsonlike
from graph_cast.util import timer as timer

logger = logging.getLogger(__name__)


def ingest_json_files(
    fpath,
    config,
    conn_conf: ConnectionConfigType,
    keyword: Optional[str] = None,
    clean_start="all",
    dry=False,
    ncores=1,
    **kwargs,
):
    conf_obj = JConfigurator(config)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        init_db(db_client, conf_obj, clean_start)

    # file discovery <- move this foo to JConfigurator
    files = sorted(
        [f for f in listdir(fpath) if isfile(join(fpath, f)) if "json" in f]
    )
    if keyword is not None:
        files = [f for f in files if keyword in f]

    logger.info(f" Processing {len(files)} json files : {files}")

    def openfile(filename):
        if filename.endswith(".gz"):
            return gzip.open(filename, "rb")
        else:
            return open(filename, "r")

    for filename in files:
        with openfile(join(fpath, filename)) as fps:
            with timer.Timer() as t_pro:
                data = json.load(fps)
                process_jsonlike(
                    data, conf_obj, conn_conf, ncores=ncores, dry=dry, **kwargs
                )
            logger.info(f" processing {filename} took {t_pro.elapsed:.2f} sec")


def ingest_csvs(
    fpath,
    config,
    conn_config: ConnectionConfigType,
    limit_files=None,
    max_lines=None,
    batch_size=5000000,
    clean_start=False,
    n_thread=1,
):
    """

    :param fpath:
    :param config:
    :param conn_config:
    :param limit_files:
    :param max_lines:
    :param batch_size:
    :param clean_start:
    :param n_thread: if there are multiple files per mode, they will be processed using n_core threads
    :return:

    """

    logger.info("in ingest_csvs")
    logger.info(f"limit_files : {limit_files}")
    logger.info(f"max_lines : {max_lines}")
    logger.info(f"batch_size : {batch_size}")
    logger.info(f"clean_start : {clean_start}")

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
            if n_thread > 1:
                func = partial(
                    graph_cast.input.table_flow.process_table_with_queue,
                    **kwargs,
                )
                assert (
                    mp.get_start_method() == "fork"
                ), "Requires 'forking' operating system"
                processes = []
                tasks: mp.Queue = mp.Queue()
                for item in conf_obj.mode2files[mode]:
                    tasks.put(item)
                for w in range(n_thread):
                    p = mp.Process(target=func, args=(tasks,), kwargs=kwargs)
                    processes.append(p)
                    p.start()
                for p in processes:
                    p.join()
            else:
                for batch in conf_obj.mode2files[mode]:
                    graph_cast.input.table_flow.process_table(batch, **kwargs)
        logger.info(f"{mode} took {klepsidra.elapsed:.1f} sec")
