import argparse
from os.path import dirname, realpath, join
import pandas as pd
import yaml
import logging
from graph_cast.db.arango import get_arangodb_client, define_collections_and_indices
from graph_cast.input.table_flow import process_table
from graph_cast.architecture.table import TConfigurator


logger = logging.getLogger(__name__)

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#
#     cpath = dirname(realpath(__file__))
#
#     logging.basicConfig(
#         filename="ingest_csv.log",
#         format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
#         datefmt="%Y-%m-%d %H:%M:%S",
#         level=logging.INFO,
#     )
#
#     parser.add_argument("--path", type=str, help="path to csv datafiles")
#
#     parser.add_argument(
#         "-i",
#         "--id-addr",
#         default="127.0.0.1",
#         type=str,
#         help="port for arangodb connection",
#     )
#
#     parser.add_argument(
#         "--protocol", default="http", type=str, help="protocol for arangodb connection"
#     )
#
#     parser.add_argument(
#         "-p", "--port", default=8529, type=int, help="port for arangodb connection"
#     )
#
#     parser.add_argument(
#         "-l", "--cred-name", default="root", help="login name for arangodb connection"
#     )
#
#     parser.add_argument(
#         "-w",
#         "--cred-pass",
#         default="123",
#         help="login password for arangodb connection",
#     )
#
#     parser.add_argument(
#         "--db",
#         default="ibes_test",
#         help="db for arangodb connection",
#     )
#
#     parser.add_argument(
#         "-f",
#         "--limit-files",
#         default=None,
#         type=int,
#         nargs="?",
#         help="max files per type to use for ingestion",
#     )
#
#     parser.add_argument(
#         "-m",
#         "--max-lines",
#         default=None,
#         type=int,
#         nargs="?",
#         help="max lines per file to use for ingestion",
#     )
#
#     parser.add_argument(
#         "-b",
#         "--batch-size",
#         default=500000,
#         type=int,
#         help="number of symbols read from (archived) file for a single batch",
#     )
#
#     parser.add_argument(
#         "--clean-start",
#         type=str,
#         default="all",
#         help='"all" to wipe all the collections, "edges" to wipe only edges',
#     )
#
#     parser.add_argument(
#         "--config-path",
#         type=str,
#         default=join(cpath, "../../conf/ibes.yaml"),
#         help="",
#     )
#
#     args = parser.parse_args()
#
#     with open(args.config_path, "r") as f:
#         config = yaml.load(f, Loader=yaml.FullLoader)
#
#     db_client = get_arangodb_client(
#         args.protocol, args.id_addr, args.port, args.db, args.cred_name, args.cred_pass
#     )
#
#     conf_obj = TConfigurator(config)
#
#     define_collections_and_indices(
#         db_client,
#         conf_obj.graph_config,
#         conf_obj.vertex_config,
#     )
#
#     mode = "ibes"
#
#     tabular_resource = pd.read_csv(join(cpath, "../data/all/ibes.csv.gz"))
#     tabular_resource = tabular_resource.fillna("")
#     conf_obj.set_mode(mode)
#     process_table(tabular_resource, 10, 10000, db_client, conf_obj)
