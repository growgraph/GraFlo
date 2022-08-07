import argparse
import logging
import unittest
from os.path import dirname, join, realpath

import pandas as pd
import yaml

from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.main import ingest_json_files
from graph_cast.util import ResourceHandler, equals

logger = logging.getLogger(__name__)


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
    }

    modes = ["wos", "freshcaller"]

    def __init__(self, reset):
        super().__init__()
        self.reset = reset

    def _atomic(self, mode):

        path = join(self.cpath, f"../data/json/{mode}")
        config = ResourceHandler.load(f"conf.json", f"{mode}.yaml")

        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)

        ingest_json_files(path, config, conn_conf=conn_conf, ncores=1)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            cols = db_client.get_collections()
            vc = {}
            for c in cols:
                if not c["system"]:
                    cursor = db_client.execute(f"return LENGTH({c['name']})")
                    size = next(cursor)
                    vc[c["name"]] = size

        if not self.reset:
            ref_vc = ResourceHandler.load(
                f"test.ref.json", f"{mode}_sizes.yaml"
            )
            flag = equals(vc, ref_vc)
            if not flag:
                print(vc)
                print(ref_vc)
            self.assertTrue(flag)

        else:
            ResourceHandler.dump(
                vc, join(self.cpath, f"../ref/json/{mode}_sizes.yaml")
            )

    def test_modes(self):
        for mode in self.modes:
            self._atomic(mode)

    # def test_one_json(self):
    #     import gzip
    #     import json
    #     from graph_cast.architecture.json import JConfigurator
    #     from graph_cast.input.json_flow import process_jsonlike
    #
    #     fpath = join(self.cpath, f"../data/wos_unit.json.gz")
    #
    #     config_path = join(self.cpath, f"../../conf/wos_json.yaml")
    #     with open(config_path, "r") as f:
    #         config = yaml.load(f, Loader=yaml.FullLoader)
    #     with gzip.GzipFile(fpath, "rb") as fps:
    #         data = json.load(fps)
    #
    #     json_data = data
    #
    #     db = f"json_test"
    #
    #     conf_obj = JConfigurator(config)
    #
    #     r = process_jsonlike(json_data, conf_obj, db_config=None, dry=True)

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
