import unittest
from os.path import join, dirname, realpath
import yaml
import logging
from pprint import pprint
import pandas as pd
from graph_cast.arango.util import get_arangodb_client
from graph_cast.main import ingest_json_files

logging.basicConfig(
    filename="test_ingest_json.log",
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    filemode="w",
)


class TestIngestJSON(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    set_reference = False
    # set_reference = True

    id_addr = "127.0.0.1"
    protocol = "http"
    port = 8529
    cred_name = "root"
    cred_pass = "123"

    modes = ["wos"]

    def _atomic(self, mode):
        prefix = f"{mode}_json"
        db = f"{mode}_test"

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
            path, db_client=db_client, keyword="wos", config=config, ncores=1
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
            t = pd.DataFrame(test_sizes).set_index(0).rename(columns={1: "test"}).sort_index()
            r = pd.DataFrame(ref_sizes).set_index(0).rename(columns={1: "ref"}).sort_index()
            cmp = pd.concat([r, t], axis=1).sort_index()
            outstanding = cmp[cmp["test"] != cmp["ref"]]
            pprint(outstanding)
            pprint((cmp["test"] == cmp["ref"]).all())
            self.assertTrue((cmp["test"] == cmp["ref"]).all())


    def test_modes(self):
        for mode in self.modes:
            self._atomic(mode)

    def test_one_json(self):
        import gzip
        import json
        from graph_cast.architecture.json import JConfigurator
        from graph_cast.main import ingest_json
        fpath = join(self.cpath, f"../data/wos_unit.json.gz")

        config_path = join(self.cpath, f"../../conf/wos_json.yaml")
        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        with gzip.GzipFile(fpath, "rb") as fps:
            data = json.load(fps)

        json_data = data

        db = f"json_test"

        db_client = get_arangodb_client(
            self.protocol,
            self.id_addr,
            self.port,
            db,
            self.cred_name,
            self.cred_pass,
        )

        conf_obj = JConfigurator(config)

        r = ingest_json(json_data, conf_obj, sys_db=None, dry=True)


if __name__ == "__main__":
    unittest.main()
