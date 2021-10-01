# import yaml
# from graph_cast.input.json import parse_edges, get_json_data, foo_parallel
# from collections import defaultdict
# from os.path import expanduser
# from timeit import default_timer
#
# sources = [
#     expanduser(
#         "~/data/wos/experiment/tmp/1980/WR_1980_20190212023637_DSSHPSH_0001#good#0.json.gz"
#     ),
#     expanduser("~/data/wos/experiment/tmp/1985/dump_xml_0#good#0.json.gz"),
#     expanduser(
#         "~/data/wos/experiment/tmp/2010/WR_2010_20190215011716_DSSHPSH_0001#good#0.json.gz"
#     ),
# ]
#
#
# config_path = "../../conf/wos_json.yaml"
#
# with open(config_path, "r") as f:
#     config = yaml.load(f, Loader=yaml.FullLoader)
# index_fields_dict = {k: v["index"] for k, v in config["vertex_collections"].items()}
#
# all_fields_dict = {k: v["fields"] for k, v in config["vertex_collections"].items()}
#
# edge_des, excl_fields = parse_edges(config["json"], [], defaultdict(list))
#
#
# # parallelize
# kwargs = {
#     "config": config["json"],
#     "vertex_config": config["vertex_collections"],
#     "edge_fields": excl_fields,
#     "merge_collections": ["publication"],
# }
#
# for source in sources:
#     print(source)
#     data = get_json_data(source)
#     print(len(data))
#     begin = default_timer()
#     foo_parallel(data, kwargs, 1000)
#     end = default_timer()
#     print(f"{end - begin:.3g} sec")


import unittest
from os.path import join, dirname, realpath
import yaml
import logging
from pprint import pprint
from graph_cast.arango.util import get_arangodb_client
from graph_cast.main import ingest_json_files

logger = logging.getLogger(__name__)


class TestIngestJSON(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    # set_reference = True
    set_reference = False

    id_addr = "127.0.0.1"
    protocol = "http"
    port = 8529
    cred_name = "root"
    cred_pass = "123"

    modes = ["wos"]

    def _atomic(self, mode):
        prefix = f"{mode}_json"
        db = f"{prefix}_test"

        path = join(self.cpath, f"../data/{prefix}")

        config_path = join(self.cpath, f"../../conf/{prefix}.yaml")
        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        if not self.set_reference:
            ref_path = join(self.cpath, f"./ref/{prefix}_sizes.yaml")
            with open(ref_path, "r") as f:
                ref_sizes = yaml.load(f, Loader=yaml.FullLoader)

        db_client = get_arangodb_client(
            self.protocol,
            self.id_addr,
            self.port,
            db,
            self.cred_name,
            self.cred_pass,
        )

        ingest_json_files(
            path,
            db_client=db_client,
            keyword="wos",
            clean_start=True,
            config=config,
        )

        cols = db_client.collections()
        test_sizes = []
        for c in cols:
            if not c["system"]:
                cursor = db_client.aql.execute(f"return LENGTH({c['name']})")
                size = next(cursor)
                test_sizes += [(c["name"], size)]
        test_sizes = sorted(test_sizes, key=lambda x: x[0])
        if self.set_reference:
            ref_path = join(self.cpath, f"./ref/{prefix}_sizes.yaml")
            with open(ref_path, "w") as file:
                yaml.dump(test_sizes, file)
        else:
            for (k, v), (q, w) in zip(test_sizes, ref_sizes):
                pprint(f"{k} {v} {w}")
            self.assertTrue(test_sizes == ref_sizes)

    def test_modes(self):
        for mode in self.modes:
            self._atomic(mode)


if __name__ == "__main__":
    unittest.main()

