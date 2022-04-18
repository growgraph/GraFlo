import unittest
from os.path import join, dirname, realpath
import logging
from graph_cast.main import ingest_csvs
from graph_cast.util import ResourceHandler, equals
from graph_cast.architecture import TConfigurator
from graph_cast.db import ConnectionManager, ConfigFactory
import argparse

logger = logging.getLogger(__name__)


class TestIngestCSV(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    db_args = {
        "protocol": "http",
        "ip_addr": "127.0.0.1",
        "port": 8529,
        "cred_name": "root",
        "cred_pass": "123",
        "database": "root",
        "db_type": "arango",
    }

    modes = [
        "ibes",
        "wos",
        "ticker"
    ]

    def __init__(self, reset):
        super().__init__()
        self.reset = reset

    def _atomic(self, mode):
        db = f"{mode}_test"

        path = join(self.cpath, f"../data/{mode}")
        config = ResourceHandler.load(f"conf", f"{mode}.yaml")
        # conf_obj = TConfigurator(config)

        db_args = dict(self.db_args)
        db_args["database"] = db
        conn_conf = ConfigFactory.create_config(args=db_args)

        # loop over files
        ingest_csvs(
            path,
            conn_conf,
            limit_files=None,
            config=config,
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
        if not self.reset:
            ref_vc = ResourceHandler.load(f"test.ref", f"{mode}_sizes_ingest_csv.yaml")
            flag = equals(vc, ref_vc)
            if not flag:
                print(vc)
                print(ref_vc)
            self.assertTrue(flag)

        else:
            ResourceHandler.dump(
                vc, join(self.cpath, f"../ref/{mode}_sizes_ingest_csv.yaml")
            )

    def runTest(self):
        for mode in self.modes:
            self._atomic(mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="reset test results")
    args = parser.parse_args()
    suite = unittest.TestSuite()
    suite.addTest(TestIngestCSV(args.reset))
    unittest.TextTestRunner(verbosity=2).run(suite)
