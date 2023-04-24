import argparse
import logging
from os.path import expanduser

import yaml

from graph_cast.db import ConfigFactory
from graph_cast.main import ingest_json_files
from graph_cast.util import ResourceHandler

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--datapath",
        default=expanduser("../misc/wos"),
        help="path to misc files",
    )

    # parser.add_argument(
    #     "-i",
    #     "--id-addr",
    #     default="127.0.0.1",
    #     type=str,
    #     help="port for arangodb connection",
    # )
    #
    # parser.add_argument(
    #     "--protocol", default="http", type=str, help="protocol for arangodb connection"
    # )
    #
    # parser.add_argument(
    #     "-p", "--port", default=8529, type=int, help="port for arangodb connection"
    # )
    #
    # parser.add_argument(
    #     "-l", "--login-name", default="root", help="login name for arangodb connection"
    # )
    #
    # parser.add_argument(
    #     "-w",
    #     "--login-password",
    #     default="123",
    #     help="login password for arangodb connection",
    # )
    #
    # parser.add_argument("--db", default="_system", help="db for arangodb connection")

    parser.add_argument(
        "-f",
        "--limit-files",
        default=None,
        type=int,
        nargs="?",
        help="max files per type to use for ingestion",
    )

    parser.add_argument(
        "-b",
        "--batch-size",
        default=5000,
        type=int,
        help="number of docs in the batch pushed to db",
    )

    parser.add_argument(
        "--keyword", default="DSSHPSH", help="prefix for files to be processed"
    )

    parser.add_argument(
        "--prefix", default="wos", help="prefix for collection names"
    )

    parser.add_argument(
        "--clean-start",
        type=str,
        default="all",
        help='"all" to wipe all the collections, "edges" to wipe only edges',
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="../../conf/wos.yaml",
        help="",
    )

    parser.add_argument(
        "--db-config-path",
        type=str,
        help="",
    )

    args = parser.parse_args()

    limit_files_ = args.limit_files
    batch_size = args.batch_size
    clean_start = args.clean_start

    with open(args.config_path, "r") as f:
        config_ = yaml.load(f, Loader=yaml.FullLoader)

    logger.info(f"limit_files: {limit_files_}")
    logger.info(f"clean start: {clean_start}")

    logging.basicConfig(filename="ingest_json.log", level=logging.INFO)

    schema_config = ResourceHandler.load(fpath=args.config_path)
    conn_conf = ConfigFactory.create_config(
        args=ResourceHandler.load(fpath=args.db_config_path)
    )

    ingest_json_files(
        expanduser(args.datapath),
        config=schema_config,
        conn_conf=conn_conf,
        keyword=args.keyword,
        clean_start=clean_start,
    )
