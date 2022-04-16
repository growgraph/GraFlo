from graph_cast.db.arango import get_arangodb_client, insert_return_batch
import logging
import argparse
import sys

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

    parser.add_argument("--db", default="test", help="db for arangodb connection")

    args = parser.parse_args()

    db_client = get_arangodb_client(
        args.protocol,
        args.id_addr,
        args.port,
        args.db,
        args.login_name,
        args.login_password,
    )

    cnames = [c["name"] for c in db_client.collections() if c["name"][0] != "_"]
    for c in cnames:
        logger.info(c)

    docs = [{"value": i} for i in range(5)]
    query0 = insert_return_batch(docs, "test")

    cursor = db_client.aql.execute(query0)
    for item in cursor:
        logger.info(item)
