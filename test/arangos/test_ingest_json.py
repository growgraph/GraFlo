import argparse
import logging
import sys
import unittest
from os.path import dirname, join, realpath

from graph_cast.db import ConfigFactory, ConnectionManager
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
    }

    modes = ["wos", "freshcaller", "kg_v2"]

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

    def _verify(self, mode):
        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)
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

    def test_weights_ind_db(self):
        self._atomic("kg_v1")

        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            cols = db_client.get_collections()
            ecols = [c for c in cols if "edges" in c["name"]]
            for c in ecols:
                cursor = db_client.execute(
                    f"FOR x in {c['name']} limit 1 return x.publication"
                )
                doc = next(cursor)
                self.assertEqual(doc, {"arxiv": "current.123"})

    def runTest(self):
        self.test_weights_ind_db()
        for mode in self.modes:
            self._atomic(mode)
            self._verify(mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reset", action="store_true", help="reset test results"
    )
    args = parser.parse_args()
    suite = unittest.TestSuite()
    suite.addTest(TestIngestJSON(args.reset))
    unittest.TextTestRunner(verbosity=2).run(suite)
