import argparse
import yaml
import logging
from graph_cast.db import ConfigFactory
from graph_cast.main import ingest_csvs

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--path", type=str, help="path to csv datafiles")

    parser.add_argument(
        "-i",
        "--id-addr",
        default="127.0.0.1",
        type=str,
        help="port for arangodb connection",
    )

    parser.add_argument(
        "--protocol", default="http", type=str, help="protocol for arangodb connection"
    )

    parser.add_argument(
        "-p", "--port", default=8529, type=int, help="port for arangodb connection"
    )

    parser.add_argument(
        "-l", "--cred-name", default="root", help="login name for arangodb connection"
    )

    parser.add_argument(
        "-w",
        "--cred-pass",
        default="123",
        help="login password for arangodb connection",
    )

    parser.add_argument(
        "--db",
        default="wos",
        help="db for arangodb connection",
    )

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

    args = parser.parse_args()

    logging.basicConfig(
        filename=f"ingest_csv_{args.config_path.split('/')[-1]}.log",
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
        filemode="w",
    )

    with open(args.config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    conn_conf = ConfigFactory.create_config(args=config)

    ingest_csvs(
        args.path,
        config,
        conn_conf,
        limit_files=args.limit_files,
        max_lines=args.max_lines,
        clean_start=args.clean_start,
    )
