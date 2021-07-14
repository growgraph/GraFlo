from graph_cast.arango.util import get_arangodb_client
import logging
import argparse
import sys
import gzip
import json
import pathlib
from os.path import join, expanduser


logger = logging.getLogger(__name__)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

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

    parser.add_argument("--db", default="ibes", help="db for arangodb connection")

    cfolder = pathlib.Path(__file__).parent.resolve()

    parser.add_argument(
        "-q", default=join(cfolder, "./test_query.aql"), help="path to query"
    )
    parser.add_argument(
        "-o", default=join(cfolder, "./test_query.log.json.gz"), help="path to query"
    )

    args = parser.parse_args()

    sys_db = get_arangodb_client(
        args.protocol,
        args.id_addr,
        args.port,
        args.db,
        args.login_name,
        args.login_password,
    )

    with open(args.q) as f:
        q = f.read()

    cursor = sys_db.aql.execute(q)
    chunk = list(cursor.batch())
    with gzip.open(
        expanduser(args.o),
        "wt",
        encoding="ascii",
    ) as fp:
        json.dump(chunk, fp, indent=4)
