import argparse
import logging
from graph_cast.util import ResourceHandler
from graph_cast.main import ingest_csvs
from graph_cast.db import ConfigFactory

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--path", type=str, help="path to csv datafiles")

    parser.add_argument(
        "-f",
        "--limit-files",
        default=None,
        type=int,
        nargs="?",
        help="max files per type to use for ingestion",
    )

    parser.add_argument(
        "-m",
        "--max-lines",
        default=None,
        type=int,
        nargs="?",
        help="max lines per file to use for ingestion",
    )

    parser.add_argument(
        "-b",
        "--batch-size",
        default=500000,
        type=int,
        help="number of symbols read from (archived) file for a single batch",
    )

    parser.add_argument(
        "--clean-start", action="store_true", help="wipe all the collections"
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="../conf/wos.yaml",
        help="",
    )

    parser.add_argument(
        "--db-config-path",
        type=str,
        help="",
    )

    args = parser.parse_args()

    name = args.config_path.split("/")[-1]
    name = name.split(".")[0]

    logging.basicConfig(
        filename=f"ingest_csv_{name}.log",
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
        filemode="w",
    )

    schema_config = ResourceHandler.load(fpath=args.config_path)
    conn_conf = ConfigFactory.create_config(
        args=ResourceHandler.load(fpath=args.db_config_path)
    )

    ingest_csvs(
        args.path,
        schema_config,
        conn_conf,
        batch_size=args.batch_size,
        limit_files=args.limit_files,
        max_lines=args.max_lines,
        clean_start=args.clean_start,
    )
