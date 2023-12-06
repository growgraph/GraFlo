import gzip
import json
import logging
import multiprocessing as mp
from functools import partial
from os import listdir
from os.path import isfile, join
from typing import Optional

from suthing import DBConnectionConfig

import graph_cast.input.json
import graph_cast.input.table
import graph_cast.input.table_flow
from graph_cast.architecture import JConfigurator, TConfigurator
from graph_cast.db import ConnectionManager
from graph_cast.input.json_flow import process_jsonlike
from graph_cast.onto import InputType
from graph_cast.util import timer as timer

logger = logging.getLogger(__name__)


def ingest_files(
    fpath,
    schema,
    conn_conf: DBConnectionConfig,
    input_type: InputType,
    **kwargs,
):
    if input_type == InputType.TABLE:
        ingest_tables(
            fpath=fpath, config=schema, conn_conf=conn_conf, **kwargs
        )
    elif input_type == InputType.JSON:
        ingest_json_files(
            fpath=fpath, config=schema, conn_conf=conn_conf, **kwargs
        )

    logger.warning(f"Unknown InputType provided: {InputType}")


def ingest_json_files(
    fpath,
    config,
    conn_conf: DBConnectionConfig,
    keyword: Optional[str] = None,
    clean_start="all",
    dry=False,
    n_threads=1,
    **kwargs,
):
    conf_obj = JConfigurator(config)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(conf_obj, clean_start)

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
                    data,
                    conf_obj,
                    conn_conf,
                    ncores=n_threads,
                    dry=dry,
                    **kwargs,
                )
            logger.info(f" processing {filename} took {t_pro.elapsed:.2f} sec")


def ingest_tables(
    fpath,
    config,
    conn_conf: DBConnectionConfig,
    limit_files=None,
    max_lines=None,
    batch_size=5000000,
    clean_start=False,
    n_threads=1,
    **kwargs,
):
    """

    :param fpath:
    :param config:
    :param conn_conf:
    :param limit_files:
    :param max_lines:
    :param batch_size: in bytes
    :param clean_start:
    :param n_threads: if there are multiple files per mode, they will be processed using n_threads threads
    :return:

    """

    logger.info("in ingest_tables")
    logger.info(f"limit_files : {limit_files}")
    logger.info(f"max_lines : {max_lines}")
    logger.info(f"batch_size : {batch_size}")
    logger.info(f"clean_start : {clean_start}")

    conf_obj = TConfigurator(config)

    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.init_db(conf_obj, clean_start)

    # file discovery
    conf_obj.discover_files(fpath, limit_files=limit_files)

    logger.info(conf_obj.mode2files)

    for mode in conf_obj.tables:
        conf_obj.set_mode(mode)
        kwargs = {
            "batch_size": batch_size,
            "max_lines": max_lines,
            "conf": conf_obj,
            "db_config": conn_conf,
        }

        with timer.Timer() as klepsidra:
            if n_threads > 1:
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
                for w in range(n_threads):
                    p = mp.Process(target=func, args=(tasks,), kwargs=kwargs)
                    processes.append(p)
                    p.start()
                for p in processes:
                    p.join()
            else:
                for batch in conf_obj.mode2files[mode]:
                    graph_cast.input.table_flow.process_table(batch, **kwargs)
        logger.info(f"{mode} took {klepsidra.elapsed:.1f} sec")
