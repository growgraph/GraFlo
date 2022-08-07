import argparse
import logging
import unittest
from os.path import dirname, join, realpath
from pprint import pprint

from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.main import ingest_csvs
from graph_cast.util import ResourceHandler, equals

logger = logging.getLogger(__name__)


class TestIngestCSV(unittest.TestCase):
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

    modes = [
        "ibes",
        "wos",
        "ticker",
    ]

    def __init__(self, reset):
        super().__init__()
        self.reset = reset

    def _atomic(self, mode):
        f"{mode}_test"

        path = join(self.cpath, f"../data/csv/{mode}")
        schema_config = ResourceHandler.load(f"conf.table", f"{mode}.yaml")

        db_args = dict(self.db_args)
        db_args["database"] = "testdb"
        conn_conf = ConfigFactory.create_config(args=db_args)

        ingest_csvs(
            path,
            schema_config,
            conn_conf,
            limit_files=None,
            clean_start=True,
        )

        with ConnectionManager(connection_config=conn_conf) as db_client:
            cols = db_client.get_collections()
            vc = {}
            for c in cols:
                if not c["system"]:
                    cursor = db_client.execute(f"return LENGTH({c['name']})")
                    size = next(cursor)
                    vc[c["name"]] = size

            db_client.delete_collections([], [], delete_all=True)

        if not self.reset:
            ref_vc = ResourceHandler.load(
                f"test.ref.csv", f"{mode}_sizes.yaml"
            )
            flag = equals(vc, ref_vc)
            if not flag:
                pprint(f"ref keys: {sorted(ref_vc.keys())}")
                pprint(f"cur keys: {sorted(vc.keys())}")
                for k in vc.keys():
                    pprint(f"ref: {ref_vc[k]}, current: {vc[k]}")
            self.assertTrue(flag)

        else:
            ResourceHandler.dump(
                vc, join(self.cpath, f"../ref/csv/{mode}_sizes.yaml")
            )

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
    suite.addTest(TestIngestCSV(args.reset))
    unittest.TextTestRunner(verbosity=2).run(suite)
