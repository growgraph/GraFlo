import unittest
from os.path import join, dirname, realpath
import yaml
import logging
from pprint import pprint
from graph_cast.arango.util import get_arangodb_client
from graph_cast.main import ingest_csvs

logger = logging.getLogger(__name__)


class TestIngestCSV(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    # set_reference = True
    set_reference = False

    id_addr = "127.0.0.1"
    protocol = "http"
    port = 8529
    cred_name = "root"
    cred_pass = "123"

    modes = ["ibes", "wos"]

    def _atomic(self, mode):

        db = f"{mode}_test"

        path = join(self.cpath, f"../data/{mode}")

        config_path = join(self.cpath, f"../../conf/{mode}.yaml")
        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        if not self.set_reference:
            ref_path = join(self.cpath, f"./ref/{mode}_sizes.yaml")
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

        ingest_csvs(
            path,
            db_client,
            limit_files=None,
            max_lines=None,
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
            ref_path = join(self.cpath, f"./ref/{mode}_sizes.yaml")
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
