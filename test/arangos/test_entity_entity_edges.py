import argparse
import logging
import sys
import unittest
from os.path import dirname, realpath
from pprint import pprint

from graph_cast.architecture import JConfigurator
from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.input import jsondoc_to_collections
from graph_cast.main import ingest_json_files
from graph_cast.util import ResourceHandler, equals

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
)


class TestIngestJSON(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    db_args = {
        "protocol": "http",
        "ip_addr": "127.0.0.1",
        "port": 8529,
        "cred_name": "test",
        "cred_pass": "123",
        "database": "testdb",
        "db_type": "arango",
        "request_timeout": 120,
    }

    modes = [
        "kg_v3b",
    ]

    def __init__(self, reset):
        super().__init__()
        self.reset = reset

    def _atomic(self, mode):
        jsonlike = ResourceHandler.load(
            f"test.data.json.{mode}", f"{mode}.json.gz"
        )
        config = ResourceHandler.load(f"conf.json", f"{mode}.yaml")
        conf_obj = JConfigurator(config)

        defdict = jsondoc_to_collections(jsonlike[0], conf_obj)

        pprint(defdict)
        mumu = defdict[("mention", "mention")]
        mu = defdict["mention"]

        set([e["_role"] for e in mumu])

        [e["_anchor"] if "_anchor" in e else "None" for e in mu]

        # find entity relations

        # path = join(self.cpath, f"../misc/json/{mode}")
        # config = ResourceHandler.load(f"conf.json", f"{mode}.yaml")
        #
        # db_args = dict(self.db_args)
        # db_args["database"] = "testdb"
        # conn_conf = ConfigFactory.create_config(args=db_args)
        # ingest_json_files(
        #     path, config, conn_conf=conn_conf, ncores=1, upsert_option=False
        # )

    def runTest(self):
        for mode in self.modes:
            self._atomic(mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reset", action="store_true", help="reset test results"
    )
    args = parser.parse_args()
    suite = unittest.TestSuite()
    suite.addTest(TestIngestJSON(args.reset))
    unittest.TextTestRunner(verbosity=2).run(suite)
