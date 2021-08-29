import argparse
import yaml
from os.path import expanduser

from graph_cast.main import ingest_json_files
from graph_cast.arango.util import get_arangodb_client
import logging

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--datapath", default=expanduser("../data/wos"), help="path to data files"
    )

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
        "-l", "--login-name", default="root", help="login name for arangodb connection"
    )

    parser.add_argument(
        "-w",
        "--login-password",
        default="123",
        help="login password for arangodb connection",
    )

    parser.add_argument("--db", default="_system", help="db for arangodb connection")

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

    parser.add_argument("--prefix", default="wos", help="prefix for collection names")

    parser.add_argument(
        "--clean-start",
        type=str,
        default="all",
        help='"all" to wipe all the collections, "edges" to wipe only edges',
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="../../conf/wos_json.yaml",
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

    db_client = get_arangodb_client(
        protocol=args.protocol,
        ip_addr=args.id_addr,
        port=args.port,
        database=args.db,
        cred_name=args.login_name,
        cred_pass=args.login_password,
    )

    ingest_json_files(
        expanduser(args.datapath),
        config=config_,
        db_client=db_client,
        keyword=args.keyword,
        clean_start=clean_start,
    )
