import unittest
from os.path import join, dirname, realpath
import pathlib
import yaml
import logging
from graph_cast.arango.util import get_arangodb_client
from graph_cast.main import ingest_csvs

logger = logging.getLogger(__name__)


class TestIngestCSV(unittest.TestCase):
    cpath = dirname(realpath(__file__))

    set_reference = False

    id_addr = "127.0.0.1"
    protocol = "http"
    port = 8529
    db = "ibes_test"
    cred_name = "root"
    cred_pass = "123"
    path = join(cpath, "../data/ibes")

    config_path = join(cpath, "../../conf/ibes.yaml")
    with open(config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    ref_path = join(cpath, "./ref/ibes_sizes.yaml")
    with open(ref_path, "r") as f:
        ref_sizes = yaml.load(f, Loader=yaml.FullLoader)

    def test_ibes_ingest(self):
        db_client = get_arangodb_client(
            self.protocol,
            self.id_addr,
            self.port,
            self.db,
            self.cred_name,
            self.cred_pass,
        )

        ingest_csvs(
            self.path,
            db_client,
            limit_files=None,
            max_lines=None,
            config=self.config,
        )
        # rq = list_collections(db_client)
        cols = db_client.collections()
        ref_sizes = []
        for c in cols:
            if not c["system"]:
                cursor = db_client.aql.execute(f"return LENGTH({c['name']})")
                size = next(cursor)
                ref_sizes += [(c["name"], size)]
        ref_sizes = sorted(ref_sizes, key=lambda x: x[0])
        if self.set_reference:
            ref_path = join(self.cpath, "./ref/ibes_sizes.yaml")
            with open(ref_path, "w") as file:
                yaml.dump(ref_sizes, file)

        self.assertTrue(ref_sizes == self.ref_sizes)
